#define _GNU_SOURCE // Ensure this is defined early
#include <fcntl.h>  // Added for AT_FDCWD, O_DIRECT
#include <time.h>   // For clock_gettime, CLOCK_MONOTONIC
#include "io.h"
#include "multitask.h" // Include multitask for task_t and IO_WAIT
#define DEPTH 64
#define ALLIGNMENT 4 * 1024
struct db_FILE;
/*REMEBR: need to allign*/
/*design info:
This is a generalized io_uring interface for disk and eventually network. O_Direct for io, 
ZERO memcpy, dma, registered buffers. Kernel polling in an event loop with callbacks for submitted tasks. priority based scheduling
. extenability for io statistics and rate limiting. single point of IO for majority of IO, excluding metadata reads at the 
start-no need to overcomplicate. 
*/
typedef void (*aio_callback)(void * arg, enum post_io_behavior b);
__thread struct io_manager * man;
int setup_io_uring(struct io_uring *ring, int queue_depth) {
    int ret = io_uring_queue_init(queue_depth, ring, 0);
    if (ret < 0) {
        fprintf(stderr, "io_uring_queue_init failed: %s\n", strerror(-ret));
        return -1;
    }
    return 0;
}
void clear_tuner(io_batch_tuner * tuner){
    tuner->full_flush_runs = 0;
    tuner->non_full_runs = 0;
    tuner->last_recorded_ns = get_ns();
    tuner->batch_timer_len_ns = 1 * ONE_MILLION;
    tuner->full_ratio = .5;
    tuner->io_req_in_slice = 0;
}
int init_struct_pool_kernel(struct_pool * pool, arena * a,int bufsize, int bufnum){
      for (int i = 0; i< bufnum; i++){
        byte_buffer  * buffer = calloc(sizeof(byte_buffer), 1);

        buffer->max_bytes = bufsize;
        buffer->curr_bytes = 0;
        buffer->buffy = arena_alloc(a, bufsize);
        insert_struct(pool, buffer);
    }
    return 0;
}
int add_fsync_request(struct io_uring *ring, struct db_FILE * file, int seq) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "Could not get SQE for fsync request\n");
        return -1;
    }
    if (man->pending_sqe_count == 0) {
        clock_gettime(CLOCK_MONOTONIC, &man->first_sqe_timestamp);
    }
    man->pending_sqe_count++;
    if (seq) {
        sqe->flags |= IOSQE_IO_LINK;
    }
    io_uring_prep_fsync(sqe, file->desc.fd, IORING_FSYNC_DATASYNC);
    io_uring_sqe_set_data(sqe, file);
    
    return 0;
}

int register_logical_strat(struct io_manager *manage,const int max_single_buffer, int curr){
    struct_pool * pools [2];
    pools[0] = manage->sst_table_buffers;
    pools[1] = manage->mem_table_buffers;
    int curr_mem = 0;
    for (int i = 0; i < sizeof(pools)/ sizeof(pools[0]); i++){
        for (int j = 0; j < pools[i]->capacity; j++){
            byte_buffer * buf = pools[i]->all_ptrs[j];
            buf->id = curr_mem / max_single_buffer + curr;
            curr_mem += buf->max_bytes;
        }
    }
    return 0;
}
int register_logical_segments(struct io_manager *manage, const int max_single_buffer) {
    struct_pool * pools [3];
    pools[0] = manage->four_kb;
    pools[1] = manage->sst_table_buffers;
    pools[2] = manage->mem_table_buffers;
    int curr_mem = 0;
    for (int i = 0; i < sizeof(pools)/ sizeof(pools[0]); i++){
        for (int j = 0; j < pools[i]->capacity; j++){
            byte_buffer * buf = pools[i]->all_ptrs[j];
            buf->id = curr_mem / max_single_buffer;
            curr_mem += buf->max_bytes;
        }
    }
    return ceil_int_div(curr_mem, max_single_buffer);
}
void add_db_io(uint32_t bufs_to_add){
    for (int i = 0 ; i < bufs_to_add  ; i++){
        struct db_FILE * re = calloc(sizeof(*re), 1);
        re->behavior = ASYNC_AWAIT;
        insert_struct(man->io_requests, re);
    }
}
void init_io_manager_not_fixed(struct io_manager * manage, int num_sst_tble, int num_memtable, int sst_tbl_s, int mem_tbl_s){

    manage->mem_table_buffers = create_pool(num_memtable);
    manage->sst_table_buffers = create_pool(num_sst_tble);
    const int max_single_buffer = 1024 * 1024* 1024;
    size_t total_size = (num_memtable * mem_tbl_s) + (num_sst_tble * sst_tbl_s);
    
    const int physical_buffers = ceil_int_div(total_size, max_single_buffer);
    manage->num_io_vecs = physical_buffers ;
    manage->iovecs = calloc(physical_buffers, sizeof(struct iovec));
    manage->a.capacity = 0;
    manage->a.data = aligned_alloc(ALLIGNMENT, total_size);
    if (!manage->a.data) {
        perror("Failed to allocate aligned memory");
        return;
    }
    manage->a.size = total_size;
    int max_to_alloc = min(max_single_buffer, total_size);
    int chunked = 0;
    for (int i = 0 ; i < physical_buffers; i++){
        int real_size = min(total_size - chunked, max_to_alloc);
        chunked += real_size;
        manage->iovecs[i].iov_base= arena_alloc(&manage->a, real_size);
        manage->iovecs[i].iov_len = real_size;
    }
    manage->a.capacity = 0;
    manage->max_buffer_size = max_single_buffer;
    int res = setup_io_uring(&manage->ring, DEPTH);
    if (res < 0) {
        perror("Failed to setup io_uring");
        return;
    }
    int logical_buff = num_memtable + num_sst_tble;
    
    manage->total_buffers = logical_buff;
    if (num_sst_tble > 0){
        init_struct_pool_kernel(manage->sst_table_buffers, &manage->a, sst_tbl_s, num_sst_tble);
    }
    if (num_memtable > 0){
        init_struct_pool_kernel(manage->mem_table_buffers, &manage->a, mem_tbl_s, num_memtable);
    }
    manage->sst_tbl_s = sst_tbl_s;
    manage->m_tbl_s = mem_tbl_s;

    manage->four_kb = NULL;

    manage->pending_sqe_count = 0;
    manage->pool.trad.four_kb = manage->four_kb;
    manage->pool.strat = FIXED;

    clear_tuner(&manage->tuner);
}
void init_io_manager(struct io_manager * manage, int num_4kb, int num_sst_tble, int num_memtable, int sst_tbl_s, int mem_tbl_s){
    manage->four_kb = create_pool(num_4kb);
    manage->mem_table_buffers = create_pool(num_memtable);
    manage->sst_table_buffers = create_pool(num_sst_tble);
    const int max_single_buffer = 1024 * 1024* 1024;
    size_t total_size = (num_4kb * 4096) + (num_memtable * mem_tbl_s) + (num_sst_tble * sst_tbl_s);
    
    const int physical_buffers = ceil_int_div(total_size, max_single_buffer);

    manage->iovecs = calloc(physical_buffers, sizeof(struct iovec));

    manage->a.capacity = 0;
    manage->a.data = aligned_alloc(ALLIGNMENT, total_size);
    if (!manage->a.data) {
        perror("Failed to allocate aligned memory");
        return;
    }
    manage->a.size = total_size;
    int max_to_alloc = min(max_single_buffer, total_size);
    int chunked = 0;
    manage->num_io_vecs = physical_buffers ;
    for (int i = 0 ; i < physical_buffers; i++){
        int real_size = min(total_size - chunked, max_to_alloc);
        chunked += real_size;
        manage->iovecs[i].iov_base= arena_alloc(&manage->a, real_size);
        manage->iovecs[i].iov_len = real_size;
    }
    manage->a.capacity = 0;
    manage->max_buffer_size = max_single_buffer;
    int res = setup_io_uring(&manage->ring, DEPTH);
    if (res < 0) {
        perror("Failed to setup io_uring");
        return;
    }
    res = io_uring_register_buffers(&manage->ring, manage->iovecs, physical_buffers);

    if (res <  0){
        perror("Failed to allocate segements");
    }
    int logical_buff = num_4kb + num_memtable + num_sst_tble;
    
    manage->total_buffers = logical_buff;

    if (num_4kb > 0){
        init_struct_pool_kernel(manage->four_kb, &manage->a, 4 * 1024, num_4kb);
    }
    if (num_sst_tble > 0){
        init_struct_pool_kernel(manage->sst_table_buffers, &manage->a, sst_tbl_s, num_sst_tble);
    }
    if (num_memtable > 0){
        init_struct_pool_kernel(manage->mem_table_buffers, &manage->a, mem_tbl_s, num_memtable);
    }
    register_logical_segments(manage, max_single_buffer);
    manage->sst_tbl_s = sst_tbl_s;
    manage->m_tbl_s = mem_tbl_s;
    
    const int max_concurrent_ops = 0;
    manage->io_requests = create_pool(max_concurrent_ops);
    for (int i = 0 ; i < manage->total_buffers / 4 ; i++){
        struct db_FILE * re = calloc(sizeof(*re), 1);
        re->behavior= ASYNC_AWAIT;
        insert_struct(manage->io_requests, re);
    }
    manage->pending_sqe_count = 0;
    manage->pool.trad.four_kb = manage->four_kb;
    manage->pool.strat = FIXED;

    clear_tuner(&manage->tuner);
}
int allign_to_page_size(int curr_size) {
    if (curr_size % 4096 == 0) {
        return curr_size;
    }
    int to_add = 4096 - (curr_size % 4096);
    return curr_size + to_add;
}
// Modified function to add read/write requests using the segmented approach
int add_read_write_requests(struct io_uring *ring, struct io_manager *manage, struct db_FILE *requests, int seq) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "Could not get SQE for request\n");
        return -1;
    }
    if (man->pending_sqe_count == 0) {
        clock_gettime(CLOCK_MONOTONIC, &man->first_sqe_timestamp);
    }
    man->pending_sqe_count++;

    if (seq) {
        sqe->flags |= IOSQE_IO_LINK;
    }
    requests->len = allign_to_page_size(requests->len);
    if (manage->pool.strat == VM_MAPPED && requests->len < manage->sst_tbl_s){
        if (requests->op == READ) {
            io_uring_prep_read(sqe, requests->desc.fd, requests->buf->buffy, 
                requests->len, requests->offset);
        }
        else if (requests->op == WRITE){
            io_uring_prep_write(sqe, requests->desc.fd, requests->buf->buffy, 
                requests->len, requests->offset);
        }
    }
    else if (requests->op == UNFIXED_READ){
         io_uring_prep_read(sqe, requests->desc.fd, requests->buf->buffy, 
                requests->len, requests->offset);
    }
    else if (requests->op == UNFIXED_WRITE){
          io_uring_prep_write(sqe, requests->desc.fd, requests->buf->buffy, 
                requests->len, requests->offset);
    }
    else{
        if (requests->op == READ) {
            io_uring_prep_read_fixed(sqe, requests->desc.fd, requests->buf->buffy, 
                requests->len, requests->offset, requests->buf->id);
        } 
        else if (requests->op == WRITE) {
            io_uring_prep_write_fixed(sqe, requests->desc.fd, requests->buf->buffy, 
                                   requests->len, requests->offset, requests->buf->id);
        }
    }
  
    io_uring_sqe_set_data(sqe, requests);
    if (!io_uring_sq_space_left(ring)){
        int res = io_uring_submit(ring);
        if (res < 0) perror("submit fail");
        else man->pending_sqe_count = 0;

    }
    return 0;
}

static inline bool try_submit(struct io_uring *ring, io_batch_tuner *t, uint64_t pending){   
    uint64_t ns = get_ns();
    uint64_t elapsed = ns - t->last_recorded_ns;           
    if (elapsed < t->batch_timer_len_ns){
        return false;
    }
    if (pending < DEPTH){
        t->non_full_runs++;
    }
    else{
        t->full_flush_runs++;
    }
    int ret = io_uring_submit(ring);
    if (ret >= 0) {
        man->pending_sqe_count = 0;
        t->last_recorded_ns = get_ns();                       
    }
    return ret >= 0;
}
bool try_submit_interface(struct io_uring *ring){
    io_batch_tuner * tuner = &man->tuner;
    uint64_t pending = man->pending_sqe_count;
    if (try_submit(ring, tuner, pending)){
        man->pending_sqe_count = 0;
        return true;
    }
    return false;
}
void reset_tuner(io_batch_tuner * tuner){
    tuner->full_flush_runs = 0;
    tuner->non_full_runs = 0;
    tuner->io_req_in_slice = 0;
    tuner->last_recorded_ns = get_ns();
}
void perform_tuning(io_batch_tuner *t){
    const double step = 0.10;      
    const double target_full = 0.50;         
    const double hysteresis= 0.05;
    const double min_ns = 10e3;         
    const double max_ns = 250e6;         

    uint64_t total = t->full_flush_runs + t->non_full_runs;
    if (!total)
        return;

    double full_ratio = (double)t->full_flush_runs / (double)total;

    double elapsed_ms = t->batch_timer_len_ns / 1e6;                            

    double io_per_ms = t->io_req_in_slice / elapsed_ms;

    if (io_per_ms < 1.0) {
        t->batch_timer_len_ns = (uint64_t)min_ns;
    }
    else if (full_ratio > target_full + hysteresis) {
        double next = t->batch_timer_len_ns * (1.0 + step);
        if (next > max_ns) next = max_ns;
        t->batch_timer_len_ns = (uint64_t)next;
    } 
    else if (full_ratio < target_full - hysteresis) {
        double next = t->batch_timer_len_ns * (1.0 - step);
        if (next < min_ns) next = min_ns;
        t->batch_timer_len_ns = (uint64_t)next;
    }
    //fprintf(stdout, "tuned ratio of %f with submits per ms of %f and new timeout %f next\n", full_ratio, io_per_ms, t->batch_timer_len_ns);
    reset_tuner(t);
}

int process_completions(struct io_uring *ring, void(*func) (void *, enum post_io_behavior)) {
    if (!ring) return 0;

    struct io_uring_cqe *cqes[DEPTH * 2]; 
    int count = io_uring_peek_batch_cqe(ring, cqes, DEPTH * 2);
    if (count <= 0) return 0;

    for (int i = 0; i < count; i++) {
        struct db_FILE *io = io_uring_cqe_get_data(cqes[i]);
        if (!io) continue;

        io->response_code = cqes[i]->res;
        if (cqes[i]->res < 0) {
            perror(strerror(cqes[i]->res));
        }
       
        func(io->callback_arg, io->behavior);
        
    }

    io_uring_cq_advance(ring, count);  // advance by actual batch count
    return count;
}
void cleanup_io_uring(struct io_uring *ring) {
    io_uring_queue_exit(ring);
}
int add_open_close_requests(struct io_uring *ring, struct db_FILE * requests, int seq) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "Could not get SQE for request\n" );
        return -1;
    }
    if (man->pending_sqe_count == 0) {
        clock_gettime(CLOCK_MONOTONIC, &man->first_sqe_timestamp);
    }
    man->pending_sqe_count++;
    if (seq){
        sqe->flags|= IOSQE_IO_LINK;
    }
    if (requests->op == OPEN){
        io_uring_prep_openat(sqe, AT_FDCWD, requests->desc.fn,requests->flags, requests->perms);
    }
    else if (requests->op == CLOSE){
        io_uring_prep_close(sqe,requests->desc.fd);
    }
    io_uring_sqe_set_data(sqe, requests);
    if (!io_uring_sq_space_left(ring)){
        int res = io_uring_submit(ring);
        if (res < 0) perror("submit fail");
        else man->pending_sqe_count = 0;

    }
    return 0;
}
byte_buffer * fixed_strat(struct io_manager * m, int size){
    if (size <= 4 * 1024){
        return request_struct(m->four_kb);
    }
    if (size <= m->sst_tbl_s){
        return request_struct(m->sst_table_buffers);
    }
    if (size <= m->m_tbl_s){
        return request_struct(m->mem_table_buffers);
    }
    return NULL;
}
byte_buffer * select_buffer(int size){
    struct io_manager * m  = man;
    if (m->pool.strat == FIXED){
        return fixed_strat(m, size);
    }
    if (size <= m->sst_tbl_s){
        return request_struct(m->sst_table_buffers);
    }
    if (size <= m->m_tbl_s){
        return request_struct(m->mem_table_buffers);
    }
    byte_buffer * buffer = get_buffer(&m->pool, size);
    if (buffer == NULL) return NULL;
    if (m->pool.strat == PINNED_BUDDY){
        /*pointer arth to determine the buffer id by finding the difference in addr\
        must cast because i guess we cannot preform pointer arth on a void * and a char * ????*/
        uintptr_t diff_for_id = (uintptr_t)m->pool.pinned.pinned - (uintptr_t)buffer->buffy;

        buffer->id = ceil_int_div(diff_for_id, m->max_buffer_size);
    }
    return buffer;
}
void fixed_buffer_return(byte_buffer * b){
    struct io_manager * m  = man;
    int size= b->max_bytes;
    if (size <= 4096){
        return_struct(m->four_kb, b, &reset_buffer);
    }
    if (size <= m->sst_tbl_s){
        return_struct(m->sst_table_buffers, b, &reset_buffer);
        return;
    }   
    if (size <= m->m_tbl_s){
       return_struct(m->mem_table_buffers, b, &reset_buffer);
       return;
    }
}
void return_buffer(byte_buffer * buff){
    struct io_manager * m  = man;
    if (m->pool.strat == FIXED){
        fixed_buffer_return(buff);
    }
    return_buffer_strat(&m->pool, buff);

}
int chain_open_op_close(struct io_uring *ring, struct io_manager * m, struct db_FILE * req){
    int r_w_flag = 0;
    if (req->op == READ) {
        r_w_flag = O_RDONLY | O_DIRECT;
    }
    else {
        r_w_flag = O_WRONLY | O_CREAT | O_APPEND | O_DIRECT;
    }
    mode_t perms  = S_IRUSR  | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

    int res;
    res = open(req->desc.fn, r_w_flag, perms);
    if (res < 0) return -1;
    req->desc.fd =res;
    if (io_uring_sq_space_left(ring) == 0){
        int submit_res = io_uring_submit(ring);
        if (submit_res >= 0) man->pending_sqe_count = 0;
    }
    res = add_read_write_requests(ring,m, req, 0);
    if(res < 0 ) return -1;
    if (io_uring_sq_space_left(ring) == 0){
        int submit_res = io_uring_submit(ring);
        if (submit_res >= 0) man->pending_sqe_count = 0;
    }
    if (req->op== WRITE){
        res = add_fsync_request(ring, req, 1);
    }
    if(res < 0 ) return -1;
    if (io_uring_sq_space_left(ring) == 0){
        int submit_res = io_uring_submit(ring);
        if (submit_res >= 0) man->pending_sqe_count = 0;
    }
    req->op = CLOSE;
    res = add_open_close_requests(ring,req ,0);
    if (res < 0) return -1;
    return 0;
}
void rw_generic(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager){
    req->len = len;
    req->offset = offset;
    manager->tuner.io_req_in_slice ++;
    add_read_write_requests(&manager->ring,manager, req, 0);
}
int do_write(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager){
    rw_generic(req,  offset, len, manager);
    task_t * task_r = aco_get_arg();
    if (task_r) {
        task_r->stat = IO_WAIT;
    }
    aco_yield();
    return req->response_code;
}
void do_async_rw(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager){
    rw_generic(req,  offset, len, manager);
}
int do_read(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager){   
    rw_generic(req,  offset, len, manager);
    task_t * task_r = aco_get_arg();
    if (task_r) {
        task_r->stat = IO_WAIT;
    }
    aco_yield();
    return req->response_code;
}
void do_oc_generic(const char * fn, struct db_FILE * req, struct io_manager * manager){
    add_open_close_requests(&manager->ring,req, 0);
    manager->tuner.io_req_in_slice ++;
    task_t * task_o = aco_get_arg();
    if (task_o) {
        task_o->stat = IO_WAIT;
    }
}
int do_open(const char * fn, struct db_FILE * req, struct io_manager * manager){
    
    req->op = OPEN;
    req->desc.fn = (char*)fn;
    add_open_close_requests(&manager->ring,req, 0);
    manager->tuner.io_req_in_slice ++;
    task_t * task_o = aco_get_arg();
    if (task_o) {
        task_o->stat = IO_WAIT;
    }
    aco_yield();
    return req->response_code;
}
int do_close(int fd, struct db_FILE * req, struct io_manager * manager){
    req->op = CLOSE;
    add_open_close_requests(&manager->ring,req, 0);
    manager->tuner.io_req_in_slice ++;
    task_t * task_c = aco_get_arg();
    if (task_c) {
        task_c->stat = IO_WAIT;
    }
    aco_yield();
    return req->response_code;
}
int do_fsync(struct db_FILE * file, struct io_manager * manager){
    add_fsync_request(&manager->ring,file, 0);
    manager->tuner.io_req_in_slice ++;
    task_t * task_f = aco_get_arg();
    if (task_f) {
        task_f->stat = IO_WAIT;
    }
    aco_yield();
    return 0;
}
struct db_FILE * dbio_open(const char * file_name, char  mode){
    struct db_FILE * req = request_struct(man->io_requests);
    if (req == NULL) return NULL;
    
    req->callback_arg = aco_get_arg(); /*task*/
    if (mode == 'r'){
        req->flags = DEFAULT_READ_FLAGS;
    }
    else{
        req->flags = DEFAULT_WRT_FLAGS;
    }
    req->perms = DEFAULT_PERMS;
    req->desc.fd = do_open(file_name, req,man);
    if (req->desc.fd < 0){
        return_struct(man->io_requests, req, NULL);
        req = NULL;
    }
    return req;
}
void init_db_FILE_ctx(const int max_concurrent_ops, db_FILE * dbs){
    for (int i = 0; i < max_concurrent_ops; i++){
        insert_struct(man->io_requests, &dbs[i]);
    }
}
void fix_on_strat(struct io_manager * m){
    buffer_pool pool = m->pool;
    if (pool.strat == VM_MAPPED){
        return;
    }
    const int max_single_buffer = 1024 * 1024* 1024;
    if (pool.strat == PINNED_BUDDY){
        const int total_size =  pool.pinned.alloc.base - pool.pinned.alloc.arena_end;
        uint16_t num_buffers=  ceil_int_div(total_size,max_single_buffer);
        struct iovec * final = calloc(sizeof(struct iovec), num_buffers + m->num_io_vecs);
        uint64_t  curr = 0;
        for (int i = 0 ; i < num_buffers; i++){
            int real_size = min(total_size - curr, max_single_buffer);
            m->iovecs[i].iov_base= &pool.pinned.pinned[curr];
            curr += real_size;
            m->iovecs[i].iov_len = real_size;
        }
        register_logical_strat(m, max_single_buffer, num_buffers);
        /*copy old copy, lazy and bad*/
        memcpy(&final[num_buffers], m->iovecs, sizeof(struct iovec) * m->num_io_vecs);
        free(m->iovecs);
        m->iovecs = final;
        int32_t res = io_uring_register_buffers(&m->ring, m->iovecs, num_buffers + m->num_io_vecs);
        assert(res > 0);
        
    }
}
void io_prep_in(struct io_manager * io_manager, io_config config, aio_callback std_func){
    if (config.buffer_pool == FIXED){
        init_io_manager(io_manager,  config.max_concurrent_io,  config.big_buf_s, config.huge_buf_s, config.num_huge_buf, config.num_big_buf);
    }
    else{
        /*need dumb conifg*/
        io_manager->pool = make_b_p(config.buffer_pool, config.bp_memsize, config.config);
        init_io_manager_not_fixed(io_manager, config.num_big_buf,  config.num_huge_buf, config.big_buf_s, config.huge_buf_s);
    }
    io_manager->io_requests = create_pool(config.max_concurrent_io);
    struct db_FILE * io= malloc(sizeofdb_FILE () * config.max_concurrent_io);
    for (int i = 0; i < config.max_concurrent_io; i++){
        io[i].buf = NULL;
        io[i].behavior = ASYNC_AWAIT;
        insert_struct(io_manager->io_requests, &io[i]);
    }

}
int do_rename(struct io_manager * io_manager, const char * fn, const char * new,  struct db_FILE * req ){
    struct io_uring_sqe *sqe = io_uring_get_sqe(&io_manager->ring);
    req->callback_arg = aco_get_arg();
    io_uring_prep_renameat(sqe, AT_FDCWD, fn, AT_FDCWD, new, 0);
    req->op = RENAME;
    io_uring_sqe_set_data(sqe, req);
     if (man->pending_sqe_count == 0) {
        clock_gettime(CLOCK_MONOTONIC, &man->first_sqe_timestamp);
    }
    man->pending_sqe_count++;
    aco_yield();
    return req->response_code;

}