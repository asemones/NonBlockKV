#include "cache.h"
#ifndef CACHE_SHRD_MNGER_C
#define CACHE_SHRD_MNGER_C

/**
 * @brief Structure representing a shard controller for managing multiple caches.
 * @struct shard_controller
 * @param caches Array of cache instances.
 * @param num_caches Number of caches in the array.
 */
typedef struct shard_controller{
    cache * caches;
    int num_caches;
}shard_controller;

/**
 * @brief Creates a new shard controller.
 * @param num_caches Number of caches to manage.
 * @param shrd_cap Capacity of each shard.
 * @param pg_size Page size for each cache.
 * @return The newly created shard controller.
 */
shard_controller create_shard_controller(int num_caches, int shrd_cap,  int pg_size);

/**
 * @brief Retrieves a cache entry from the sharded cache.
 * @param controller The shard controller.
 * @param ind Block index of the entry to retrieve.
 * @param f_n File name associated with the entry.
 * @param sst Pointer to the SST file info containing compression info.
 * @return Pointer to the retrieved cache entry, or NULL if not found.
 */
cache_entry retrieve_entry_sharded(shard_controller controller, block_index * ind,  const char * f_n, sst_f_inf * sst);

/**
 * @brief Frees the memory allocated for the shard controller.
 * @param controller Pointer to the shard controller to free.
 */
void free_shard_controller(shard_controller *controller);
cache * get_raw_cache(shard_controller controller);
#endif