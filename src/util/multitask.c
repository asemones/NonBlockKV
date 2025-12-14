#include "multitask.h"
#include <stdbool.h>
#include "io.h"
#include <stdio.h>
#define SHARED_S_SIZE 512
#define MAX_RUNTIMES_DEFAULT 512
/*our user space task scheduler*/
void aco_execute(void);
__thread task_q __ready_q;
// Using the cascade_runtime_t definition from the header file
task_t* create_task(aco_t* main, void * allocator, int id, enum task_type type, int stack_s, void* stack, aco_cofuncp_t func, void* args);
void allocate_new_tasks(cascade_runtime_t * runtime) {
    while(!sync_task_q_is_full(runtime->scheduler.pool)){ // Use sync version
        task_t* new_task = create_task(runtime->main_co, runtime->scheduler.a, 0, POOL_WAIT, 64, runtime->scheduler.shared, aco_execute, NULL);
        new_task->scheduler = runtime;
        sync_task_q_enqueue(runtime->scheduler.pool, new_task); // Use sync version
    }
}
void std_io_callback(void * arg, enum post_io_behavior b){
    //printf("[SCHED] IO Callback Entered for task %p\n", arg);
    task_t * task = arg;
    if (b == ASYNC_AWAIT){
        if (task->real_task == NULL){
            fprintf(stdout, "found it\n");
        }
        task->stat = RUNNING;
        sync_task_q_enqueue(task->scheduler->scheduler.active, task); 
    }
    else if (b == FORK_JOIN){
        task->leaf_counter --;
        if (task->stat == LEAF_WAIT && task->leaf_counter == 0) {/* if it was dropped to preform a leaf wait */ 
            task->stat = RUNNING;
            sync_task_q_enqueue(task->scheduler->scheduler.active,task);
        }
    }
}
task_t* create_task(aco_t* main, void * allocator, int id, enum task_type type, int stack_s, void* stack, aco_cofuncp_t func, void* args) {
    task_t * task;
    if (allocator != NULL){
        task = arena_alloc(allocator, sizeof(*task));
    }
    else{
        task  = malloc(sizeof(*task));
    }
    if (!task) {
        return NULL;
    }

    task->type = type;
    task->stat = NOT_START;
    task->real_task = NULL;
    
    // Now we can pass the heap-allocated task pointer directly as the argument
    if (stack != NULL) {
        task->thread = aco_create(main, stack, stack_s, func, task);
    } else {
        task->thread = aco_create(main, aco_share_stack_new(stack_s), stack_s, func, task);
    }
    
    return task;
}

void aco_execute(void) {
    task_t* task = (task_t*)aco_get_arg(); // Use aco_get_arg() to get the passed task pointer
    
    task->stat = NOT_START;
    aco_yield();
    
    while(1) {
        while(!task->real_task) {
            // printf("[SCHED] Task %p yielding in aco_execute (waiting for real_task)\n", task); // Potentially too verbose
            aco_yield();
        }
        
        task->stat = RUNNING;
        //printf("[SCHED] Task %p executing real_task %p\n", task, task->real_task); // Log before real_task execution
        if (aco_likely(task->type != SUBTASK && task->type != EXTERNAL_RPC) && task->type != SUBTASK_INTERN){
            task->ret.ret = task->real_task(task->arg);
        }
        else if (task->ret.parent_ptr){
            future_t ret =  task->real_task(task->arg);
            *task->ret.parent_ptr = ret;
        }
        else{
             task->real_task(task->arg);
        }
        if (task->stat == IO_WAIT){
            fprintf(stdout, "found it2\n");
        }
        //printf("[SCHED] Task %p finished real_task %p\n", task, task->real_task); // Log after real_task execution
        task->real_task = NULL;
        task->stat = DONE;
        if (task->virtual){
            return_arena(task->virtual, task->scheduler->scheduler.scratch_pad_pool);
        }
        task->virtual =NULL;
        //printf("[SCHED] Task %p set to DONE\n", task); // Log setting DONE status
    }
}
task_t * add_task(uint64_t id, task_func func, enum task_type type, void * arg ,cascade_runtime_t * scheduler, task_t * parent) {
    task_t* task;
    
    // Atomically load ongoing_tasks for the check. Acquire ensures visibility of prior updates.
    if (sync_task_q_is_empty(scheduler->scheduler.pool) || atomic_load_explicit(&scheduler->scheduler.ongoing_tasks, memory_order_acquire) >= scheduler->scheduler.max_ongoing_tasks) {
       return NULL;
    }
    
    sync_task_q_dequeue(scheduler->scheduler.pool, &task);
    task->real_task = func;
    task->type = type;
    task->arg = arg;
    task->id = id;
    task->parent = parent;
    task->running_subtacks = 0;
    task->running_rpcs =0;
    task->virtual = NULL;
    sync_task_q_enqueue(scheduler->scheduler.active, task);
    // Atomically increment ongoing_tasks. Relaxed is likely sufficient as it's just a counter.
    atomic_fetch_add_explicit(&scheduler->scheduler.ongoing_tasks, 1, memory_order_relaxed);
    return task;
}
task_t * add_rpc(uint64_t id, task_func func,void * arg ,cascade_runtime_t * runtime, uint64_t p_id, task_t * parent) {
    task_t* task;
    db_schedule* scheduler = &runtime->scheduler;
    if (sync_task_q_is_empty(scheduler->pool)) {
        return NULL;
    }
    
    sync_task_q_dequeue(scheduler->pool, &task);
    task->real_task = func;
    task->type = RPC;
    task->arg = arg;
    task->id = id;
    task->parent = parent;
    task->p_id = p_id;
    task->running_subtacks = 0;
    task->running_rpcs =0;
    task->virtual = NULL;
    sync_task_q_enqueue(scheduler->active, task); 
    // Atomically increment ongoing_tasks. Relaxed is likely sufficient.
    atomic_fetch_add_explicit(&scheduler->ongoing_tasks, 1, memory_order_relaxed);
    return task;
}
void cascade_submit_external(cascade_runtime_t * rt, future_t * mem_loc, _Atomic uint64_t * wait_counter, task_func function, void * args){ // Update signature

    task_t * new_task = NULL;
    enum task_type type = (function == NULL) ? TERMINATE_WAIT : SUBTASK;
    while (new_task == NULL){
        new_task = add_task( 0,function, type, args, rt, NULL);
    }
    new_task->ret.parent_ptr = mem_loc;
    new_task->wait_counter = wait_counter;
    if (wait_counter){
        atomic_fetch_add_explicit(new_task->wait_counter, 1, memory_order_relaxed);
    }
    return;
}
future_t cascade_sub_intern_wait(task_func func, void* arg) {
    task_t * current_task =  aco_get_arg();
    cascade_runtime_t * rt = current_task->scheduler;

    task_t * new_task = NULL;

    while (new_task == NULL) {
        new_task = add_task(0, func, MISC_COMPUTE, arg, rt, current_task);
        if (new_task == NULL) {
            current_task->stat = BLOCKED_YIELD;
            aco_yield();
            current_task->stat = RUNNING;
        }
    }
    await_regular;
    return new_task->ret.ret;
}
void intern_grp_nowait(task_func func, void* arg, future_t * addr) {
    task_t * current_task =  aco_get_arg();
    cascade_runtime_t * rt = current_task->scheduler;

    task_t * new_task = NULL;
    
    while (new_task == NULL) {
        new_task = add_task(0, func, SUBTASK, arg, rt, current_task);
        if (new_task == NULL) {
            current_task->stat = BLOCKED_YIELD;
            aco_yield();
            current_task->stat = RUNNING;
        }
        else{
            new_task->ret.parent_ptr = addr;
        }
    }
}
void cascade_intern_group_wait(task_func  * funcs, future_t * futures, int num_reg, void ** args){
    task_t * task = aco_get_arg();
    for (int  i  = 0; i < num_reg ; i++){
       intern_grp_nowait(funcs[i], args[i], &futures[i]);
    }
    task->running_subtacks += num_reg;
    await_regular;
}
void intern_wait_for_x(uint32_t num){

    task_t * t= aco_get_arg(); 
    int targ_num=  t->running_subtacks + -num;
    if (targ_num < 0 ) targ_num = t->running_subtacks;

    while (targ_num >= t->running_subtacks){
        aco_yield();
    }
}
void cascade_sub_intern_nowait(task_func func, void* arg,  future_t * addr) {
    task_t * current_task =  aco_get_arg();
    cascade_runtime_t * rt = current_task->scheduler;

    task_t * new_task = NULL;
    
    while (new_task == NULL) {
        new_task = add_task(0, func, SUBTASK_INTERN, arg, rt, current_task);
        if (new_task == NULL) {
            current_task->stat = BLOCKED_YIELD;
            aco_yield();
            current_task->stat = RUNNING;
        }
        else{
            new_task->ret.parent_ptr = addr;
        }
        current_task->running_subtacks ++;
    }
    return;
}
uint64_t cascade_rpc_wait(cascade_runtime_t * targ, task_func func, void * arg){
    uint64_t id;
    {
        task_t * ts = aco_get_arg();
        msg_q * outbox = ts->scheduler->scheduler.links[targ->cpu].outbox;
        rpc_table_t * tbl = ts->scheduler->scheduler.table;
        id = rpc_start(tbl);
        if (msg_q_is_empty(outbox)){
            bitmap_set(ts->scheduler->scheduler.links[targ->cpu].queues_with_items, ts->scheduler->cpu);
        }
        msg_q_enqueue(outbox, (msg_t){
        .payload.value = arg,
        .on_recieve = func,
        .sender = ts,
        .recive = NULL,
        .size = id,
    });
    }
    await_rpc;
    return id;
}
uint64_t cascade_rpc_nowait(cascade_runtime_t * targ, task_func func, void * arg){
    uint64_t id;
    {
        task_t * ts = aco_get_arg();
        msg_q * outbox = ts->scheduler->scheduler.links[targ->cpu].outbox;
        rpc_table_t * tbl = ts->scheduler->scheduler.table;
        id = rpc_start(tbl);
        if (msg_q_is_empty(outbox)){
            bitmap_set(ts->scheduler->scheduler.links[targ->cpu].queues_with_items, ts->scheduler->cpu);
        }
        msg_q_enqueue(outbox, (msg_t){
        .payload.value = arg,
        .on_recieve = func,
        .sender = ts,
        .recive = NULL,
        .size = id,
    });
    }
    return id;
}
void poll_rpc(uint64_t *rpc_ids, int num) {
    rpc_table_t* tbl;
    {
          task_t *task = (task_t*) aco_get_arg();
          cascade_runtime_t * scheduler = task->scheduler;
          tbl = scheduler->scheduler.table;
    }
    uint32_t completed_count = 0;
    while (1) {
        for (uint32_t i = 0; i < num; i++) {
            if (rpc_is_complete(tbl, rpc_ids[i])) {
                completed_count++;
            }
        }
        if (completed_count >= num) {
            return;
        }
        {
            task_t *task = aco_get_arg();
            task->stat = RPC_YIELD;
        }
        aco_yield();
    }
}
void poll_rpc_external(cascade_runtime_t * frame, _Atomic uint64_t * wait_counter) { // Update signature
   // Atomically load the value in each loop iteration.
   // Acquire semantics ensure that prior writes (decrements) by other threads become visible.
   while (atomic_load_explicit(wait_counter, memory_order_acquire) > 0) {
        sched_yield();
   }
   // Optional: Log exit if needed for debugging
   // fprintf(stderr, "[%p] Exited poll_rpc_external. Final counter = %lu\n", (void*)wait_counter, atomic_load_explicit(wait_counter, memory_order_relaxed));
}
void task_cpy(task_t * dest, task_t * src){
    memcpy(dest, src, sizeof(sizeof(*dest)));
}
void process_msg(cascade_runtime_t * rt, msg_t msg){
    db_schedule * sch = &rt->scheduler;
    switch (msg.type){
        case ADD_TASK: {
            task_t * sub = add_rpc(msg.size, msg.on_recieve, msg.payload.value, rt, msg.size, msg.sender);
            if (sub == NULL) {
                msg_t fail_msg;
                fail_msg.type = ADD_TASK_FAILED;
                fail_msg.size = msg.size;
                fail_msg.sender = NULL;
                fail_msg.recive = msg.sender;
                fail_msg.reciver_shard_id = msg.sender_shard_id;
                fail_msg.payload.return_code = CASCADE_ERROR_TASK_ADD_FAILED;

                msg_q * outbox = sch->links[fail_msg.reciver_shard_id].outbox;
                 if (msg_q_is_empty(outbox)) {
                    bitmap_set(sch->links[fail_msg.reciver_shard_id].queues_with_items, sch->id);
                 }
                msg_q_enqueue(outbox, fail_msg);
            }
            break;
        }
        case ADD_TASK_FAILED: {
            future_t failure_reason = msg.payload;
            rpc_fail(sch->table, msg.size, failure_reason);
            task_t * original_task = msg.recive;
            if (original_task != NULL) {
                 original_task->running_rpcs--;
                 if (original_task->running_rpcs <= 0 && original_task->stat == RPC_YIELD) {
                    original_task->stat = RUNNING;
                    sync_task_q_enqueue(sch->active, original_task);
                 }
            }
            break;
        }
        case FUNCTION_RETURN_VAL: {
            task_t * callee = msg.recive;
            if (callee != NULL) {
                callee->running_rpcs --;
                 if (callee->running_rpcs <= 0 && callee->stat == RPC_YIELD){
                    callee->stat = RUNNING;
                    sync_task_q_enqueue(sch->active, callee);
                 }
            }
            rpc_complete(sch->table, msg.size, msg.payload);
            break;
        }
        case FUNCTION_RETURN_VAL_NOWAIT: {
             task_t * callee = msg.recive;
             if (callee != NULL) {
                callee->running_rpcs --;
             }
            rpc_complete(sch->table, msg.size, msg.payload);
            break;
        }
    }
}
void handle_parent(task_t * task, db_schedule * sch){
     if (!task) return;

    task->running_subtacks--;
    if (task->running_subtacks <=0){
        task->stat = RUNNING;
        sync_task_q_enqueue(sch->active, task);
    }    
}

void check_return_val(rpc_table_t * tbl, task_t * task, db_schedule * scheduler){
    switch (task->type){
        case RPC: {
            msg_t msg;
            msg.payload = task->ret.ret;
            msg.size = task->p_id;
            msg.recive = task->parent;
            msg.reciver_shard_id = rpc_get_shard_id(task->p_id);
            msg.type = FUNCTION_RETURN_VAL;
            if (msg_q_is_empty(scheduler->links[msg.reciver_shard_id].outbox)) {
                bitmap_set(scheduler->links[msg.reciver_shard_id].queues_with_items, scheduler->id);
            }
            msg_q_enqueue(scheduler->links[msg.reciver_shard_id].outbox, msg);
            break;
        }
        case RPC_NO_WAIT: {
            msg_t msg;
            msg.payload = task->ret.ret;
            msg.size = task->p_id;
            msg.recive = task->parent;
            msg.reciver_shard_id = rpc_get_shard_id(task->p_id);
            msg.type = FUNCTION_RETURN_VAL_NOWAIT;
            if (msg_q_is_empty(scheduler->links[msg.reciver_shard_id].outbox)) {
                bitmap_set(scheduler->links[msg.reciver_shard_id].queues_with_items, scheduler->id);
            }
            msg_q_enqueue(scheduler->links[msg.reciver_shard_id].outbox, msg);
            break;
        }
        case NO_RETURN:
            task->type = POOL_WAIT;
            if (task->parent){
                task->parent->running_rpcs --;
                if (task->parent->running_rpcs <=0){
                    task->parent->stat = RUNNING;
                    sync_task_q_enqueue(scheduler->active, task->parent);
                }   
            }   
        case SUBTASK:
            task->type = POOL_WAIT;
            // Atomically decrement the counter. Release semantics ensure that prior writes
            // within this task become visible to threads doing an acquire-load on the counter.
            if (!task->wait_counter ) return;
            atomic_fetch_sub_explicit(task->wait_counter, 1, memory_order_release);  
            break;
        case SUBTASK_INTERN:
            task->type = POOL_WAIT;
            handle_parent(task->parent, scheduler);  
            break;
        default:
            task->type = POOL_WAIT;
            handle_parent(task->parent, scheduler);   
    }
}
uint16_t process_a_queue(cascade_runtime_t *rt, uint32_t targ_id, uint32_t batch_size){
    uint16_t processed;
    msg_t msg;
    bitmap * shard_map = &rt->scheduler.my_active_msgs;
    msg_q * inbox = rt->scheduler.links[targ_id].inbox;
    for (processed =0; processed < batch_size; processed++){
        if (!msg_q_dequeue(inbox, &msg)){
            bitmap_clear(shard_map, targ_id);
            return processed;
        }
        process_msg(rt, msg);
    }
    if (msg_q_is_empty(inbox)){
        bitmap_clear(shard_map, targ_id);
    }
    return batch_size;
}
uint32_t check_queues(cascade_runtime_t *rt, uint32_t batch_size, uint32_t max_to_process){
    uint64_t msgs[MAX_RUNTIMES_DEFAULT];
    uint32_t num = bitmap_get_set_bits_512(&rt->scheduler.my_active_msgs, msgs, MAX_RUNTIMES_DEFAULT);
    if (num == 0) return 0; 
    uint32_t msgs_processed = 0;
    for (uint32_t i = 0; i < num && msgs_processed < max_to_process; i++) {
        uint32_t targ_id = (uint32_t)msgs[i];
        msgs_processed += process_a_queue(rt, targ_id, batch_size);
    }
    return msgs_processed;
}
void freeze_scheduler(db_schedule* scheduler, int finish_tasks, struct io_uring  *ring);
void on_tw_advance(task_t * groggy_task, sync_task_q * q){
    groggy_task->stat = RUNNING;
    sync_task_q_enqueue(q, groggy_task);
}
/*under dev*/
static inline void scheduler_epoch(db_schedule * scheduler,  struct io_uring* ring){
    const int epoch_size = 100;
    task_t * tasks [epoch_size];
    task_t * put_in_active[epoch_size];
    task_t * put_in_pool[epoch_size];
    int actual = sync_task_q_dequeue_bulk(scheduler->active,tasks, epoch_size);
    for (int i = 0; i < actual; i++){
        aco_resume(tasks[i]->thread);
    }

}
static inline void scheduler_logic(db_schedule * scheduler,  struct io_uring* ring){
        task_t* task;
        
        /*dont aquire a lock for no real reason*/
        tw_advance(scheduler->sleep, get_ns(), on_tw_advance, scheduler->active);
        if (sync_task_q_is_empty(scheduler->active)) { // Use sync version;
            if (has_been_us(scheduler->complete_timer, 100, &scheduler->complete_timer)){
                 try_submit_interface(ring);
                 process_completions(ring, std_io_callback);
                 perform_tuning(&man->tuner);
            }
            //check_queues(rt, 4, 40);
            return;
        }
        sync_task_q_dequeue(scheduler->active, &task);
        if (aco_unlikely(task->type == TERMINATE_NO_WAIT)){
            pthread_exit(NULL);
        }
        else if(aco_unlikely(task->type == TERMINATE_WAIT)){
            freeze_scheduler(scheduler, 1, ring);
            if (task->wait_counter){
                atomic_fetch_sub(task->wait_counter, 1);
            }
            pthread_exit(NULL);
            return;
        }
       
        else if (aco_unlikely(task->stat == RPC_YIELD)){ 
            return;
        }
        else if (aco_likely(task->real_task && task->stat != IO_WAIT)) {
            aco_resume(task->thread);
        }
        if (task->stat == IO_WAIT || task->stat == NAP_TIME || task->stat ==LOCK_YIELD)  {
            return;
        }
        if (task->stat == DONE) {
            check_return_val(scheduler->table, task, scheduler);
            sync_task_q_enqueue(scheduler->pool, task); 
            atomic_fetch_sub_explicit(&scheduler->ongoing_tasks, 1, memory_order_relaxed);
        } 
        else {
            sync_task_q_enqueue(scheduler->active, task); 
        }
        if (has_been_us(scheduler->complete_timer, 100, &scheduler->complete_timer)){
            perform_tuning(&man->tuner);
            process_completions(ring, std_io_callback);
        }
        try_submit_interface(ring);
        //check_queues(rt, 4, 40);
}
void cascade_schedule(cascade_runtime_t* rt, struct io_uring* ring) {
    db_schedule * scheduler = &rt->scheduler;
    
    while(1) {
        scheduler_logic(scheduler, ring);
    }
}
void freeze_scheduler(db_schedule* scheduler, int finish_tasks, struct io_uring  *ring){
    const int TERM_TASK = 1;
    // Atomically load ongoing_tasks for the loop condition. Acquire ensures visibility.
    while(atomic_load_explicit(&scheduler->ongoing_tasks, memory_order_acquire) > TERM_TASK){
        scheduler_logic(scheduler, ring);
    }
    return;
}
void init_scheduler(db_schedule* scheduler, aco_t* main_co, int pool_size, int stack_size, void * io_manager) {
    const int arena_size = pool_size * 5 * sizeof(task_t);
    scheduler->active = sync_task_q_create(pool_size); 
    scheduler->pool = sync_task_q_create(pool_size);   
    scheduler->shared = aco_share_stack_new(stack_size);
    scheduler->a = malloc_arena(arena_size);
    const int scratch_pad_num = pool_size / 100;
    const int scratch_pads_start_size = 1024 * 32;
    scheduler->scratch_pad_pool = create_pool(scratch_pad_num);
    for (int i = 0; i < scratch_pad_num; i++){
        insert_struct(scheduler->scratch_pad_pool, malloc_arena(scratch_pads_start_size));
    }
    // Atomically store the initial value. Relaxed is fine before the scheduler is shared.
    atomic_store_explicit(&scheduler->ongoing_tasks, 0, memory_order_relaxed);
    scheduler->table = calloc(sizeof(rpc_table_t),1 );
    rpc_table_init(scheduler->table,  scheduler->id, pool_size);
    scheduler->sleep = malloc(sizeof(timer_wheel_t));
    calibrate_tsc();
    tw_init(scheduler->sleep);
    scheduler->complete_timer = 0;
}
io_config create_io_config (){
    io_config config;
    config.big_buf_s = 1024 * 64;
    config.max_concurrent_io = 5000;
    config.huge_buf_s = 1024 * 1024;
    config.num_big_buf = 16;
    config.num_huge_buf = 4;
    config.small_buff_s = 4096;
    config.coroutine_stack_size = 256;
    config.max_tasks = 50000;
    config.buffer_pool = FIXED;
    config.bp_memsize = 0;
    memset(&config.config, 0, sizeof(size_tier_config));
    return config;
}
typedef struct runtime_spawn_args{
    task_func  seed;
    void * arg;
    io_config config;
    cascade_runtime_t * runtime;
    pthread_cond_t * sig;
    bool done;
} runtime_spawn_args;
void spawn_runtime_internal(void * arg){
      aco_thread_init(NULL); 
      runtime_spawn_args * args = arg;
      const double rpc_overhead = 1.2;
      io_config config=  args->config;
      man = malloc(sizeof(*man));
      io_prep_in(man, config, std_io_callback);
      /*sick of refactoring, so heres a lazy fix since this code should only be ran cpu cores * x */
      aco_t * main = aco_create(NULL, NULL, 0, NULL, NULL);
      args->runtime->main_co = main;
      args->runtime->scheduler.main_co = main;
      init_scheduler(&args->runtime->scheduler,main, config.max_tasks * rpc_overhead, config.coroutine_stack_size * config.max_tasks * 1.5, man);
      args->runtime->scheduler.max_ongoing_tasks = config.max_tasks;
      allocate_new_tasks(args->runtime);
      args->runtime->scheduler.my_active_msgs = bitmap_create(MAX_RUNTIMES_DEFAULT);
      if (args->seed){
        add_task(0, args->seed, MISC_COMPUTE, args->arg, args->runtime, NULL);
      }
      args->done = true;
      pthread_cond_signal(args->sig);
      cascade_schedule(args->runtime, &man->ring);
      
}
cascade_runtime_t * cascade_spawn_runtime_config(task_func  seed, void * arg, io_config config){
    cascade_runtime_t * runtime = calloc(sizeof(*runtime),1);
    runtime_spawn_args * args = calloc(sizeof(*args), 1);
    args->seed = seed;
    args->config = config;
    args->arg = arg;
    args->runtime =runtime;
    args->done =  false;
    pthread_cond_t var;
    pthread_mutex_t mutx;
    pthread_mutex_init(&mutx, NULL);
    pthread_cond_init(&var, NULL);
    args->sig = &var;
    pthread_create(&runtime->thread, NULL, (void *(*)(void *))spawn_runtime_internal, args);
    pthread_mutex_lock(&mutx);
    while(!args->done){
        pthread_cond_wait(&var,&mutx);
    }
    pthread_mutex_unlock(&mutx);
    pthread_mutex_destroy(&mutx);
    pthread_cond_destroy(&var);
    //free(args);
    return runtime;
}
cascade_runtime_t * cascade_spawn_runtime_self(task_func  seed, void * arg, io_config config){
    cascade_runtime_t * runtime = calloc(sizeof(*runtime),1);
    runtime_spawn_args * args = calloc(sizeof(*args), 1);
    args->seed = seed;
    args->config = config;
    args->arg = arg;
    args->runtime =runtime;
    spawn_runtime_internal(arg);
    return runtime;
}
cascade_runtime_t * cascade_spawn_runtime_default(task_func  seed, void * arg){
    return cascade_spawn_runtime_config(seed, arg, create_io_config());
}
void free_db_sch(db_schedule * sch){
    task_t * task;
    while (!sync_task_q_is_empty(sch->active)) {
        sync_task_q_dequeue(sch->active, &task); 
        aco_destroy(task->thread);
    }
    
    while (!sync_task_q_is_empty(sch->pool)) { 
        sync_task_q_dequeue(sch->pool, &task); 
        aco_destroy(task->thread);
    }
    sync_task_q_destroy(sch->pool);
    sync_task_q_destroy(sch->active);
    free_arena(sch->a);
    free_pool(sch->scratch_pad_pool, &free_arena);
    aco_destroy(sch->main_co);
    aco_share_stack_destroy(sch->shared);
    for (int i  = 0; i < sch->linked; i++){
        if (i == sch->id) continue;
        msg_q_destroy(sch->links[i].outbox);
    }
}

void end_runtime(cascade_runtime_t * rt){
    _Atomic uint64_t wait =0;
    cascade_submit_external(rt, NULL, &wait, NULL, NULL);
    poll_rpc_external(rt, &wait);
    db_schedule * sch = &rt->scheduler;
    free_db_sch(sch);
    free(rt);
    return;
}
void link_two_runtimes(cascade_runtime_t * one , cascade_runtime_t * two){
    /*consider using allocators from runtime one and runtime two*/
    msg_q * one_writes_to_me = msg_q_create(MAX_RUNTIMES_DEFAULT);
    msg_q * two_writes_to_me = msg_q_create(MAX_RUNTIMES_DEFAULT);
    int one_id = one->cpu;
    int two_id = two->cpu;
    one->scheduler.links[two_id].outbox = one_writes_to_me;
    one->scheduler.links[two_id].inbox  = two_writes_to_me;
    one->scheduler.links[two_id].queues_with_items = &two->scheduler.my_active_msgs;

    two->scheduler.links[one_id].outbox = two_writes_to_me;
    two->scheduler.links[one_id].inbox = one_writes_to_me;
    two->scheduler.links[one_id].queues_with_items = &one->scheduler.my_active_msgs;
    one->scheduler.linked ++;
    two->scheduler.linked ++;

}
void add_link_to_framework(cascade_framework_t * framework, cascade_runtime_t * new_runtime){
    new_runtime->cpu = framework->num_shards;
    new_runtime->scheduler.id = new_runtime->cpu;
    new_runtime->scheduler.table->shard_id =  new_runtime->cpu;
    ///pin cpu here?
    for (int i = 0; i < framework->num_shards; i++) {
        cascade_runtime_t *existing = framework->runtimes[i];
        link_two_runtimes(existing, new_runtime);
    }
    framework->runtimes[framework->num_shards] = new_runtime;
    framework->num_shards ++;
}
cascade_framework_t * create_framework(){
    cascade_framework_t * framework = calloc(sizeof(*framework), 1);
    return framework;
}
uint64_t find_node_hash(cascade_framework_t * frame, const char * req_key, int size,  int me){
    uint64_t hash = fnv1a_64(req_key, size) % frame->num_shards;
    if (me!= hash || frame->num_shards <= 1 ){
        return hash;
    }
    return (hash + 1) % frame->num_shards;
}
uint64_t find_node_hash_internal(cascade_framework_t * frame, const char * req_key, int size){
    task_t * task = aco_get_arg();
    int me = task->scheduler->cpu;
    uint64_t hash = fnv1a_64(req_key, size) % frame->num_shards;
    if (me!= hash|| frame->num_shards <= 1 ){
        return hash;
    }
    return (hash + 1) % frame->num_shards;
}
void * pad_allocate(uint64_t size){
    task_t * task = aco_get_arg();
    if (task->virtual){
        return arena_alloc_expand(task->virtual, size, task->scheduler->scheduler.scratch_pad_pool);
    }
    task->virtual = request_struct(task->scheduler->scheduler.scratch_pad_pool);
    if (!task->virtual){
        task->virtual = malloc_arena(size * 4);
    }
    return arena_alloc_expand(task->virtual, size,task->scheduler->scheduler.scratch_pad_pool);
}
future_t get_return_val(uint64_t id){
    future_t fut;
    task_t * t = aco_get_arg();
    rpc_get_result(t->scheduler->scheduler.table, id, &fut);
    return fut;
}
future_t get_return_val_rt(cascade_runtime_t * rt, uint64_t id){
    future_t fut;
    rpc_get_result(rt->scheduler.table, id, &fut);
    return fut;
}
static inline void sleep_logic(uint64_t microseconds){
    {
          task_t * task = aco_get_arg();
          task->stat = NAP_TIME;
          sleep_entry_t entry;
          entry.wake_me = microseconds;
          entry.napper = task;
          tw_add(task->scheduler->scheduler.sleep, microseconds * 1000, task);
    }
    aco_yield();

}
void cascade_sleep(uint64_t seconds){
    sleep_logic(seconds * ONE_MILLION);
}
void cascade_msleep(uint64_t ms){
    sleep_logic(ms * 1000);
}
void cascade_usleep(uint64_t useconds){
    sleep_logic(useconds);
}
