#include "cache_shrd_mnger.h"
#include <stdlib.h>
#define UUID_LEN 36
#define FIRST_CACHE 0
#define SECOND_CACHE 1
shard_controller create_shard_controller(int num_caches, int shrd_cap,  int pg_size) {
    shard_controller controller;
    controller.num_caches = num_caches;
    controller.caches = malloc(sizeof(cache) * num_caches);
    if (controller.caches == NULL){
        return controller;
    }
    
    for (int i = 0; i < num_caches; i++) {
        controller.caches[i] = create_cache(shrd_cap, pg_size);
    }
    return controller;
}
static inline uint64_t djb2(const char *str, int len) {
    uint64_t hash = 5381;
    for (int i = 0; i < len; i++)
        hash = ((hash << 5) + hash) + str[i]; 
    return hash;
}
static inline int jump_consistent_hash(uint64_t key, int num_buckets) {
    int b = -1, j = 0;
    while (j < num_buckets) {
        b = j;
        key = key * 2862933555777941757ULL + 1;
        j = (int)((b + 1) * ((double)(1LL << 31) / (double)((key >> 33) + 1)));
    }
    return b;
}
static inline int route_req(char * uuid, int num_buckets, int len){
    uint64_t hash = djb2(uuid, len);
    return jump_consistent_hash(hash, num_buckets);
}
cache_entry  retrieve_entry_sharded(shard_controller controller, block_index * ind,  const char * f_n, sst_f_inf * sst){
    int bucket = FIRST_CACHE; /*TEMP*/
    return retrieve_entry_no_prefetch(&controller.caches[bucket], ind, f_n, sst);
}
void free_shard_controller(shard_controller *controller) {
    if (controller == NULL || controller->caches == NULL) {
        return;
    }
    for (int i = 0; i < controller->num_caches; i++){
        free_cache(&controller->caches[i]);
    }
    free(controller->caches);
}
cache * get_raw_cache(shard_controller controller){
    
    return &controller.caches[FIRST_CACHE];
}
