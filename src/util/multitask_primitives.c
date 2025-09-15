#include "multitask_primitives.h"
#include "multitask.h"
#include <stdbool.h>
#include <stddef.h> 
#include <assert.h>

void cascade_mutex_init(cascade_mutex_t *m) {
    if (!m) return;
    m->owner = NULL;
    m->locked = false;
    const int cap = 500;
    m->wake_queue = sync_task_q_create(cap);
   
    assert(m->wake_queue != NULL);
}

void wake_task(task_t * task){
    sync_task_q * targ = task->scheduler->scheduler.active;
    task->stat = RUNNING;
    sync_task_q_enqueue(targ, task);
}   
void cascade_mutex_lock(cascade_mutex_t *m) {
    if (!m) return;
    task_t *current_task = aco_get_arg();
    assert(current_task != NULL); 

 
    if (m->locked && m->owner == current_task) {
        return;
    }

    while (m->locked) {
        sync_task_q_enqueue(m->wake_queue, current_task);
        aco_yield();
    }
    m->locked = true;
    m->owner = current_task;
}
bool cascade_mutex_checklock(cascade_mutex_t * m){
    return (m->locked);
}
void cascade_mutex_unlock(cascade_mutex_t *m) {
    if (!m) return;
    task_t *current_task = aco_get_arg();
    assert(current_task != NULL);

    assert(m->locked && m->owner == current_task);
    if (!sync_task_q_is_empty(m->wake_queue)) {
        task_t *next_task = NULL; 
      
        if (sync_task_q_dequeue(m->wake_queue, &next_task)) {
            assert(next_task != NULL); 
            m->owner = next_task;
            
            wake_task(next_task);
        }
    }
    else {
        m->locked = false;
        m->owner = NULL;
    }
}


void cascade_mutex_destroy(cascade_mutex_t *m) {
    if (!m) return;
   
    assert(!m->locked);
    assert(sync_task_q_is_empty(m->wake_queue));

    sync_task_q_destroy(m->wake_queue); 
    m->wake_queue = NULL;
    m->owner = NULL;
}

void cascade_rw_lock_init(cascade_rw_lock_t *rw) {
    if (!rw) return;
    const int cap = 250;
    rw->reader_wake_queue = sync_task_q_create(cap); 
    rw->writer_wake_queue = sync_task_q_create(cap);
    atomic_init(&rw->active_readers, 0);
    atomic_init(&rw->writer_active, false);
    assert(rw->reader_wake_queue != NULL);
    assert(rw->writer_wake_queue != NULL);
}

void cascade_rw_lock_destroy(cascade_rw_lock_t *rw) {
    if (!rw) return;
    assert(atomic_load(&rw->active_readers) == 0);
    assert(atomic_load(&rw->writer_active) == false);
    assert(sync_task_q_is_empty(rw->reader_wake_queue)); 
    assert(sync_task_q_is_empty(rw->writer_wake_queue)); 
    sync_task_q_destroy(rw->reader_wake_queue); 
    sync_task_q_destroy(rw->writer_wake_queue);
    rw->reader_wake_queue = NULL;
    rw->writer_wake_queue = NULL;
}

void cascade_rw_rd_lock(cascade_rw_lock_t *rw) {
    if (!rw) return;
    task_t *current_task = aco_get_arg();
    assert(current_task != NULL);
    while (true) {
        if (atomic_load_explicit(&rw->writer_active, memory_order_acquire) || !sync_task_q_is_empty(rw->writer_wake_queue)){
            sync_task_q_enqueue(rw->reader_wake_queue, current_task); 
            aco_yield();
        } else {
            atomic_fetch_add_explicit(&rw->active_readers, 1, memory_order_release);
            break;
        }
    }
}
void cascade_rw_w_lock(cascade_rw_lock_t *rw) {
    if (!rw) return;
    task_t *current_task = aco_get_arg();
    assert(current_task != NULL);
    while (true) {
        if (atomic_load_explicit(&rw->active_readers, memory_order_acquire) > 0 || atomic_load_explicit(&rw->writer_active, memory_order_acquire)){
            sync_task_q_enqueue(rw->writer_wake_queue, current_task); 
            current_task->stat = LOCK_YIELD;
            aco_yield();
        } 
        else {
            bool expected = false;
            if (atomic_compare_exchange_strong_explicit(&rw->writer_active, &expected, true, memory_order_acq_rel, memory_order_acquire)){
                break;
            }
        }
    }
}
bool rw_w_lock_writer(cascade_rw_lock_t *rw){
    return rw->writer_active;
}


void cascade_rw_unlock(cascade_rw_lock_t *rw) {
    if (!rw) return;
    bool was_writer = atomic_load_explicit(&rw->writer_active, memory_order_acquire);
    if (was_writer) {
        atomic_store_explicit(&rw->writer_active, false, memory_order_release);
    }
    if (rw->writer_wake_queue->size > 0){

    }
    else {
        if (atomic_fetch_sub_explicit(&rw->active_readers, 1, memory_order_release) != 1) {
            return;
        }
    }
    if (!sync_task_q_is_empty(rw->writer_wake_queue)) {
        task_t *next_task = NULL; 
        if (sync_task_q_dequeue(rw->writer_wake_queue, &next_task) && next_task) {
            wake_task(next_task);
        }
    }
    else {
        while (!sync_task_q_is_empty(rw->reader_wake_queue)) {
            task_t *next_task = NULL; 
          
            if (sync_task_q_dequeue(rw->reader_wake_queue, &next_task) && next_task) {
                wake_task(next_task);
            }
            else {
                break; 
            }
        }
    }
}
void cascade_cond_var_init(cascade_cond_var_t * var){
    const int cap = 2500;
    var->wake_queue = sync_task_q_create(cap);
}
void cascade_cond_var_destroy(cascade_cond_var_t * var){
    sync_task_q_destroy(var->wake_queue);
}
void cascade_cond_var_broadcast(cascade_cond_var_t * var){
    while(!sync_task_q_is_empty(var->wake_queue)){
        task_t * task;
        sync_task_q_dequeue(var->wake_queue, &task);
        task->stat = RUNNING;
        sync_task_q_enqueue(task->scheduler->scheduler.active, task);
    }
}
void cascade_cond_var_signal(cascade_cond_var_t * var){
    task_t * task;
    if (!sync_task_q_dequeue(var->wake_queue, &task)) return;
    task->stat = RUNNING;
    sync_task_q_enqueue(task->scheduler->scheduler.active, task);
}
void cascade_cond_var_wait(cascade_cond_var_t * var){
    sync_task_q_enqueue(var->wake_queue, aco_get_arg());
}