#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h> 
#include <stdio.h>
#ifndef STRUCTURE_POOL_H
#define STRUCTURE_POOL_H



/**
 * @brief Pool of reusable structures implemented as a simple queue.
 * @struct struct_pool
 * @param queue_buffer Circular buffer holding pointers to *available* structs.
 * @param all_ptrs Array holding *all* struct pointers ever added (for freeing).
 * @param capacity Maximum number of structs the pool can manage.
 * @param size Current number of *available* structs in the queue.
 * @param head Queue head index (for dequeue).
 * @param tail Queue tail index (for enqueue).
 * @param total_managed Count of structs currently managed (index for all_ptrs).
 * @param write_lock Mutex for thread-safe operations.
 */
typedef struct struct_pool {
    size_t capacity;       
    size_t size;            
    size_t head;            
    size_t tail;           
    size_t total_managed;  
    void** queue_buffer;   
    void** all_ptrs; 
} struct_pool;

/**
 * @brief Creates a new structure pool.
 * @param capacity Maximum capacity of the pool.
 * @return Pointer to the newly created structure pool, or NULL on failure.
 */
struct_pool * create_pool(size_t capacity);

/**
 * @brief Inserts a structure into the pool's management and makes it available.
 *        Does nothing if the pool is at full capacity (`total_managed == capacity`).
 * @param pool Pointer to the structure pool.
 * @param data Pointer to the structure to insert.
 */
void insert_struct(struct_pool * pool, void * data);

/**
 * @brief Requests a structure from the pool (dequeues an available pointer).
 * @param pool Pointer to the structure pool.
 * @return Pointer to an available structure from the pool, or NULL if the pool is empty.
 */
static inline void * request_struct(struct_pool *  restrict pool) {
    if (__glibc_unlikely(pool->size == 0)) {    
        return NULL;
    }
    void* ptr = pool->queue_buffer[pool->head];
    pool->head++;
    if (pool->head == pool->capacity){
        pool->head = 0;
    }
    pool->size--;
    return ptr;
}

static inline int return_struct(struct_pool * restrict pool, void *  restrict struct_ptr, void reset_func(void*)) {
    //if (!pool || !struct_ptr) return;

    if (__glibc_unlikely(pool->size >= pool->capacity)) {
        return 0;
    } 
    if (reset_func != NULL) {
        reset_func(struct_ptr);
    }
    pool->queue_buffer[pool->tail] = struct_ptr;
    /*changing this from a % DOUBLES preformance*/
    pool->tail = (pool->tail + 1);
    if (pool->tail == pool->capacity){
        pool->tail = 0;
    }
    pool->size++;
    return 1; //why the fuck does this return 1


}
/**
 * @brief Frees the structure pool and all managed structures.
 * @param pool Pointer to the structure pool to free.
 * @param free_func Function to free each individual structure managed by the pool. Can be NULL if structures don't need freeing or are freed elsewhere.
 * @return Always returns NULL.
 */
struct_pool * free_pool(struct_pool * pool, void free_func(void *));

#endif // STRUCTURE_POOL_H