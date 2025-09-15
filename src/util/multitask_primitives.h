#include "multitask.h"
#include <stdatomic.h>
#ifndef MULTASK_PRIMITIVES
#define MULTASK_PRIMITIVES
typedef struct cascade_mutex_t {
    task_t *owner;
    bool locked;
    sync_task_q *wake_queue;
} cascade_mutex_t;
typedef struct cascade_rw_lock_t {
    sync_task_q *reader_wake_queue;
    sync_task_q *writer_wake_queue;
    atomic_int_least64_t active_readers;
    atomic_bool writer_active;
} cascade_rw_lock_t;

typedef struct cascade_cond_var_t{
    sync_task_q *wake_queue;
} cascade_cond_var_t;
void cascade_cond_var_init(cascade_cond_var_t * var);
void cascade_cond_var_destroy(cascade_cond_var_t * var);
void cascade_cond_var_broadcast(cascade_cond_var_t * var);
void cascade_cond_var_wait(cascade_cond_var_t * var);
void cascade_cond_var_signal(cascade_cond_var_t * var);
void cascade_mutex_init(cascade_mutex_t *m);
void cascade_mutex_lock(cascade_mutex_t *m);
void cascade_mutex_unlock(cascade_mutex_t *m);
void cascade_mutex_destroy(cascade_mutex_t *m);

void cascade_rw_lock_init(cascade_rw_lock_t *rw);
void cascade_rw_rd_lock(cascade_rw_lock_t *rw);
void cascade_rw_w_lock(cascade_rw_lock_t  *rw);
void cascade_rw_lock_destroy(cascade_rw_lock_t *rw);
void cascade_rw_unlock(cascade_rw_lock_t * rw);
#endif