#include "structure_pool.h"


static void cleanup_create_failure(struct_pool * pool) {
    if (!pool) return;
    if (pool->queue_buffer) free(pool->queue_buffer);
    if (pool->all_ptrs) free(pool->all_ptrs);
    free(pool);
}

struct_pool * create_pool(size_t capacity) {
 
    struct_pool * pool = (struct_pool*)malloc(sizeof(struct_pool));
    if (!pool) {
        perror("Failed to allocate struct_pool");
        return NULL;
    }

    pool->queue_buffer = NULL;
    pool->all_ptrs = NULL;
    pool->capacity = capacity;
    pool->size = 0;
    pool->head = 0;
    pool->tail = 0;
    pool->total_managed = 0;

    pool->queue_buffer = malloc(sizeof(void*) * capacity);
    if (!pool->queue_buffer) {
        perror("Failed to allocate queue_buffer");
        cleanup_create_failure(pool);
        return NULL;
    }

    pool->all_ptrs = malloc(sizeof(void*) * capacity);
    if (!pool->all_ptrs) {
        perror("Failed to allocate all_ptrs");
        cleanup_create_failure(pool);
        return NULL;
    }
    return pool;
}

void insert_struct(struct_pool * pool, void * data) {
    if (!pool || !data) return; 
    if (pool->total_managed >= pool->capacity) {
        return;
    }
    if (pool->size >= pool->capacity) {
         return;
    }
 
    pool->all_ptrs[pool->total_managed++] = data;

 
    pool->queue_buffer[pool->tail] = data;
    pool->tail = (pool->tail + 1) % pool->capacity;
    pool->size++;

}
struct_pool * free_pool(struct_pool * pool, void free_func(void*)) {
    if (!pool) return NULL;
    if (free_func != NULL) {
        for (size_t i = 0; i < pool->total_managed; i++) {
             if (pool->all_ptrs[i] != NULL) { 
                 free_func(pool->all_ptrs[i]);
             }
        }
    }

 
    if (pool->queue_buffer) free(pool->queue_buffer);
    if (pool->all_ptrs) free(pool->all_ptrs);

  
    

    free(pool);

    return NULL;
}

