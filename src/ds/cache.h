#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include "byte_buffer.h"
#include "arena.h"
#include "hashtbl.h"
#include "structure_pool.h"
#include "associative_array.h"
#include "../db/backend/indexer.h"
#include "../db/backend/key-value.h"
#include "structure_pool.h"
#include "../db/backend/compression.h"
#include "../util/io.h"
#include <immintrin.h>
#pragma once

/**
 * @brief Cache entry structure representing a cached page
 * @struct cache_entry
 * @param buf Pointer to the byte buffer containing the cached data
 * @param ar Pointer to the key-value array for the cached data
 * @param ref_count Reference counter for this cache entry
 */
typedef struct cache_entry {
    byte_buffer *buf;
    uint16_t *ar;
    uint16_t arr_len;
    int ref_count;
    struct_pool * loc;
    int idx;
} cache_entry;
/**
 * @brief Cache structure implementing a clock replacement policy
 * @struct cache
 * @param capacity Total capacity of the cache in bytes
 * @param page_size Size of each cache page
 * @param filled_pages Number of pages currently in the cache
 * @param max_pages Maximum number of pages the cache can hold
 * @param map Dictionary mapping keys to cache entries
 * @param mem Arena for memory allocation
 * @param c_lock Mutex for thread-safe cache operations
 * @param frames Array of cache entry pointers
 * @param ref_bits Reference bits for clock algorithm
 * @param clock_hand Current position of the clock hand
 * @param compression_buffers Pool of buffers for compression operations
 */
typedef struct cache {
    size_t capacity;
    size_t page_size;
    size_t filled_pages;
    size_t max_pages;
    arena  *mem;
    pthread_mutex_t c_lock;
    cache_entry *frames;
    uint8_t *ref_bits;
    size_t  clock_hand;
    struct_pool * compression_buffers;
} cache;

/**
 * @brief Creates a new cache with the specified capacity and page size
 * @param capacity Total capacity of the cache in bytes
 * @param page_size Size of each cache page
 * @return Initialized cache structure
 */
cache create_cache(size_t capacity, size_t page_size);
/**
 * @brief Retrieves or loads a cache entry for a block index. Does not retreive any other logical pages within a physical block
 * @param c Pointer to the cache
 * @param index Pointer to the block index
 * @param file_name Name of the file containing the block
 * @param sst Pointer to the SST file info containing compression info
 * @return Pointer to the cache entry
 */
cache_entry retrieve_entry_no_prefetch(cache *c, block_index *index, const char *file_name, sst_f_inf *sst);

/**
 * @brief Pins a page in the cache to prevent eviction
 * @param c Pointer to the cache entry to pin
 */
void pin_page(cache_entry *c);

/**
 * @brief Unpins a page in the cache, allowing it to be evicted
 * @param c Pointer to the cache entry to unpin
 */
void unpin_page(cache_entry *c);

/**
 * @brief Frees a cache entry and its resources
 * @param entry Pointer to the cache entry to free
 */
void free_entry(void *entry);

/**
 * @brief Frees the cache and all its resources
 * @param c Pointer to the cache to free
 */
void free_cache(cache *c);
