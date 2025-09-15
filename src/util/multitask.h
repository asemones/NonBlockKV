#ifndef MULTITASK_H
#define MULTITASK_H

#include <stdbool.h>
#include "../ds/list.h"
#include "aco.h"
#include "../ds/circq.h"
#include "../ds/buffer_pool_stratgies.h"
#include <pthread.h>
#include <liburing.h>
#include <stdint.h>
#include <stdatomic.h> // Include for atomics
#include "../ds/arena.h"
#include "io_types.h" // Use extracted types
#include "../ds/sync_queue.h"
#include "../ds/rpc_table.h"
#include "../ds/bitmap.h"
#include "../ds/sleepwheel.h"
#include "maths.h"
#define MAX_RUNTIMES_DEFAULT 512
#define MAX_SUBTASKS 128
#define SEED_ID 0
#define yield_compute \
    task_t * task = aco_get_arg();\
    task->stat = COMPUTE_YIELD;\
    aco_yield();
#define await_rpc\
    task_t * t = aco_get_arg();\
    t->stat = RPC_YIELD;\
    t->running_rpcs ++;\
    aco_yield();
#define await_regular\
    task_t * t = aco_get_arg();\
    t->stat = RPC_YIELD;\
    t->running_subtacks ++;\
    aco_yield();
#define cascade_return_int(val)\
    future_t fut;\
    fut.return_code = val;\
    return fut
#define cascade_return_ptr(val)\
    future_t fut;\
    fut.value = val;\
    return fut
#define cascade_return_none()\
    future_t f;\
    f.return_code = -1;\
    task_t * task = aco_get_arg();\
    task->type = NO_RETURN;\
    return f
#define init_leaf_ctx()\
    task_t * _t  = aco_get_arg();\
    _t->leaf_counter = 0;\

#define await_leafs()\
    task_t * _t  = aco_get_arg();\
    _t->stat=  LEAF_WAIT;\
    aco_yield();\

#define leaf_call(call)\
  do {\
    task_t *_t = aco_get_arg();\
    _t->leaf_counter++;\
    call;\
  } while (0)

typedef struct leaf_task{
    uint64_t * wait_counter;

} leaf_task;
#define await_local_id(table, id)                        \
    do {                                                     \
        while (!rpc_is_complete(table, id)) {     \
                                                             \
            aco_yield();                                     \
        }                                                    \
    } while(0)
// Standard error code for future_t.return_code when task add fails
#define CASCADE_ERROR_TASK_ADD_FAILED -1

enum task_status {
    NOT_START,/*task is in pool and has not begun execution*/
    RUNNING, /*standard running*/
    IO_WAIT, /*task is waiting for an IO request and should not be polled*/
    COMPUTE_YIELD, /*task yielded to reduce starvation, safe to re pool in a round robin system*/
    RPC_YIELD, /*task is waiting for an RPC result*/
    BLOCKED_YIELD,
    LEAF_WAIT,
    LOCK_YIELD,
    NAP_TIME, /*task is blocked waiting for a resource (e.g., task pool slot)*/
    DONE /*finished, recycle task*/
};
enum message_type{
    ADD_TASK,
    ADD_TASK_FAILED, // Indicates the target shard could not allocate the requested task
    FUNCTION_RETURN_VAL,
    FUNCTION_RETURN_VAL_NOWAIT
};
enum task_type {
    MISC_COMPUTE,
    SUBTASK,
    WAL_WRITE,
    DB_READ,
    DB_WRITE,
    COMPACT_MERGE,
    POOL_WAIT,
    TERMINATE_WAIT,
    TERMINATE_NO_WAIT,
    RPC,
    RPC_NO_WAIT,
    EXTERNAL_RPC,
    NO_RETURN,
    SUBTASK_INTERN
};

typedef struct task task_t;
typedef void (* func)(void * arg);

DEFINE_SYNC_CIRCULAR_QUEUE(task_t *, sync_task_q); 
DEFINE_CIRCULAR_QUEUE(task_t *, task_q);
typedef future_t (*task_func)(void * arg);
typedef struct msg_t {
    enum message_type type;
    uint16_t sender_shard_id;
    uint16_t reciver_shard_id;
    task_func on_recieve;
    future_t payload;
    uint64_t size;
    task_t *sender;
    task_t *recive;
} msg_t;
DEFINE_SYNC_CIRCULAR_QUEUE(future_t, ret_q);
DEFINE_SYNC_CIRCULAR_QUEUE(msg_t , msg_q);
typedef struct shard_link_t{
   bitmap * queues_with_items;
   msg_q * inbox;
   msg_q * outbox;
}shard_link_t;
typedef struct db_schedule {
    sync_task_q * active; 
    sync_task_q * pool;  
    _Atomic int ongoing_tasks;
    int max_ongoing_tasks; // Max tasks allowed before rejecting cross-shard requests
    rpc_table_t * table;
    uint16_t linked;
    bitmap  my_active_msgs;
    shard_link_t links[MAX_RUNTIMES_DEFAULT];
    aco_share_stack_t * shared;
    struct_pool * scratch_pad_pool;
    arena * a;
    uint32_t id;
    aco_t* main_co;
    uint64_t complete_timer;
    timer_wheel_t * sleep;

} db_schedule;
typedef struct cascade_runtime_t{
    pthread_t thread;
    aco_t* main_co;
    int cpu; /*id*/
    db_schedule scheduler;
}cascade_runtime_t;
/*why do we have p_id and parent? because parent is a memory address that should not be accessed
accross shards and especially accross frameworks. it could potentially be a read from a different numa node*/
typedef union future_t_ptr{
    future_t ret;
    future_t * parent_ptr;
} future_t_ptr;
typedef struct task {
    uint8_t stat;           
    uint8_t type;
    uint16_t running_rpcs; 
    uint16_t running_subtacks;           
    aco_t *thread;         
    task_func real_task; 
    cascade_runtime_t *scheduler;         
    union{
        uint64_t id;
        _Atomic uint64_t * wait_counter; // Change to atomic pointer
    };
    future_t_ptr ret;
    uint64_t p_id;     
    task_t *parent;
    union{
        void *arg;
        uint64_t leaf_counter;
    };
    arena * virtual;
} task_t;
typedef struct partition_t{
    char * min;
    char * max;
}partition_t;
typedef struct cascade_framework_t{
    cascade_runtime_t *runtimes[MAX_RUNTIMES_DEFAULT];
    int num_shards;
    partition_t * ranges;
} cascade_framework_t;
// Using the cascade_runtime_t definition from above
/* Define circular queue after the struct task is fully defined */
 /*why do we do this? libaco aco requires heap allocated args sadly*/

struct io_uring;
task_t* create_task(aco_t* main, void * allocator, int id, enum task_type type, int stack_s, void* stack, aco_cofuncp_t func, void* args);
task_t * add_task(uint64_t id, task_func func, enum task_type type, void * arg ,cascade_runtime_t* scheduler, task_t * parent);
void init_scheduler(db_schedule *scheduler, aco_t *main_co, int pool_size,int stack_size, void * io_manager);
void cascade_schedule(cascade_runtime_t* rt, struct io_uring* ring);
cascade_runtime_t * cascade_spawn_runtime_config(task_func  seed, void * arg, io_config config);
cascade_runtime_t * cascade_spawn_runtime_default(task_func  seed, void * arg);
/*add a function call as a user thread not apart of a cascade rt*/
void cascade_sub_intern_nowait(task_func func, void* arg,  future_t * addr);
/*add a function call as the same thread apart of the same cacscade, with a wait*/
future_t cascade_sub_intern_wait(task_func func, void* arg);
/*add a series as the same msg_q * inbox[MAX_RUNTIMES_DEFAULT];
    msg_q * outbox [MAX_RUNTIMES_DEFAULT];thread apart of the same cacscade, without a wait */
void cascade_submit_external(cascade_runtime_t * rt, future_t * mem_loc, _Atomic uint64_t * wait_counter, task_func function, void * args); // Update signature
void poll_rpc(uint64_t *rpc_ids, int num);
void poll_rpc_external(cascade_runtime_t * frame, _Atomic uint64_t * wait_counter); // Update signature
cascade_framework_t * create_framework();
void add_link_to_framework(cascade_framework_t * framework, cascade_runtime_t * new_runtime);
uint64_t find_node_hash(cascade_framework_t * frame, const char * req_key, int size,  int me);
uint64_t find_node_hash_internal(cascade_framework_t * frame, const char * req_key, int size);
uint64_t cascade_rpc_wait(cascade_runtime_t * targ, task_func func, void * arg);
uint64_t cascade_rpc_nowait(cascade_runtime_t * targ, task_func func, void * arg);
void end_runtime(cascade_runtime_t * rt);
void * pad_allocate(uint64_t size);
future_t get_return_val(uint64_t id);
void intern_wait_for_x(uint32_t num);
void cascade_sleep(uint64_t seconds);
void cascade_msleep(uint64_t ms);
void cascade_usleep(uint64_t useconds);
void asyc_io_callback(void * arg);
void std_io_callback(void * arg, enum post_io_behavior b);
extern __thread task_q __ready_q;
static inline aco_t *  get_next_co(){
    task_t * t;
    if (!task_q_dequeue(&__ready_q, &t)){
        return NULL;
    }
    return t->thread;
}
static inline void cascade_yield(){
    aco_t * next = get_next_co();
    if (next == NULL){
        aco_yield();
        return;
    }
    else{
        aco_yield_to(next);
    }
}
#endif 