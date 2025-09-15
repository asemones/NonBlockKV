#include <stdlib.h>
#pragma once
#include <stdlib.h>
#include <string.h>
#include "structure_pool.h"
#include "../util/maths.h"

/**
 * @brief Memory arena for efficient memory allocation
 * @struct arena
 * @param size Current size of the arena
 * @param capacity Maximum capacity of the arena
 * @param data Pointer to the allocated memory
 * @param next Pointer to the next memory value
 */
typedef struct arena {
    size_t size;
    size_t capacity;
    void *data;
    struct arena * next;
    struct arena * cursor;
    bool free;
}
arena;
/**
 * @brief Creates a new memory arena using malloc
 * @param size Initial capacity of the arena
 * @return Pointer to the newly created arena
 */
arena *malloc_arena(size_t size);

/**
 * @brief Creates a new memory arena using calloc (zeroed memory)
 * @param size Initial capacity of the arena
 * @return Pointer to the newly created arena
 */
arena *calloc_arena(size_t size);

/**
 * @brief Resets an arena to its initial state without freeing memory
 * @param va Pointer to the arena to reset
 */
void reset_arena(void *va);

/**
 * @brief Allocates memory from an arena
 * @param a Pointer to the arena
 * @param size Size of memory to allocate
 * @return Pointer to the allocated memory
 */
void *arena_alloc(arena *a, size_t size);

/**
 * @brief Frees an arena and all its memory
 * @param a Pointer to the arena to free
 */
void free_arena(void *a);
void return_arena(arena * a, struct_pool * pool);
void *arena_alloc_expand(arena *a, size_t size, struct_pool * src);
void reset_expanded_arena(arena * a);