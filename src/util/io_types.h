#ifndef IO_TYPES_H
#define IO_TYPES_H

#include <stddef.h>
#include <sys/types.h>
#include <stdint.h>
#include <float.h>
#include <time.h> // Added for timespec
#include <liburing.h>
#include <sys/uio.h>

struct arena;
struct byte_buffer;
struct struct_pool;

#include "../ds/arena.h"
#include "../ds/byte_buffer.h"
#include "../ds/structure_pool.h"
#include "../ds/buffer_pool_stratgies.h"

enum operation{
    READ,
    UNFIXED_READ,
    UNFIXED_WRITE,
    WRITE,
    OPEN,
    CLOSE,
    RENAME

};
enum post_io_behavior{
    ASYNC_AWAIT,
    FORK_JOIN
};

typedef void (*aio_callback)(void * arg, enum post_io_behavior b);

typedef struct io_batch_tuner{
    uint64_t full_flush_runs;
    uint64_t non_full_runs;
    double batch_timer_len_ns;
    double full_ratio;
    uint64_t last_recorded_ns;
    uint64_t io_req_in_slice;
} io_batch_tuner;
typedef struct io_config{
    int max_concurrent_io;
    int big_buf_s;
    int huge_buf_s;
    int num_big_buf;
    int num_huge_buf;
    int small_buff_s;
    int coroutine_stack_size;
    int max_tasks;
    int buffer_pool;
    int bp_memsize;
    size_tier_config config;
}io_config;
struct io_manager{
    int m_tbl_s;
    int sst_tbl_s;
    int max_buffer_size;
    buffer_pool pool;
    struct_pool * sst_table_buffers;
    struct_pool * mem_table_buffers;
    struct_pool * four_kb;
    struct_pool * io_requests;
    struct io_uring ring;
    arena a;
    int num_in_queue;
    int total_buffers;
    int num_segments;
    struct iovec *iovecs;
    int num_io_vecs;
    int pending_sqe_count;      
    struct timespec first_sqe_timestamp; 
    io_batch_tuner tuner;
};

// Declaration for the thread-local io_manager pointer defined in io.c
extern __thread struct io_manager * man;


union descriptor{
    uint64_t fd;
    char * fn;
};

typedef struct db_FILE {
    union descriptor desc;
    off_t offset;
    size_t len;
    enum post_io_behavior behavior;
    void *callback_arg;
    byte_buffer* buf;
    int priority;
    int response_code;
    int flags;
    enum operation op;
    mode_t perms;
} db_FILE;
#endif 