#include "indexer.h"
#include "../../ds/sst_sl.h"
#include "../../ds/slab.h"
#include "../../ds/list.h"
#include "../../ds/circq.h"
#include "../../util/maths.h"
#include "../../util/mono_counter.h"
#ifndef SST_MANAGER_H
#define  SST_MANAGER_H
#define SST_F_XT "nbl"
typedef struct partiton_cache_element{
    uint64_t off;
    uint64_t en;
    bb_filter filter;
    block_index * block_indexs;
    uint16_t num_indexs;
    uint16_t ref_count;
    byte_buffer * b;
}partiton_cache_element;
typedef struct sst_allocator{
    slab_allocator cached;
    slab_allocator non_cached;
}sst_allocator;
typedef struct index_cache {
    size_t capacity;
    size_t page_size;
    size_t filled_pages;
    partiton_cache_element *frames;
    uint8_t *ref_bits;
    size_t  clock_hand;
    uint64_t max_pages;
} index_cache;
typedef struct {
    uint64_t block_index_size;
    level_options *levels;
} sst_man_sst_inf_cf;

typedef struct sst_manager{
    list * l_0;
    sst_sl *non_zero_l[6];
    int cached_levels;
    index_cache cache;
    sst_allocator sst_memory;
    uint64_t size_per_block;
    uint64_t parts_per;
    sst_man_sst_inf_cf config;
    counter_t name_gen;
}sst_manager;
sst_manager create_manager(sst_man_sst_inf_cf config, uint64_t mem_size);
void add_sst(sst_manager * mana, sst_f_inf* sst, int level);
void remove_sst(sst_manager * mana, sst_f_inf * sst, int level);
void free_sst_man(sst_manager * man);
int check_sst(sst_manager * mana, sst_f_inf * inf, const f_str u, int level);
uint64_t get_num_l_0(sst_manager * mana);
block_index * get_block(sst_manager * mana, sst_f_inf * inf, f_str target, int level);
block_index * try_get_relevant_block(sst_manager * mana, sst_f_inf * inf, const f_str u , int level);
sst_f_inf * allocate_non_l0(sst_manager * mana, uint64_t num_keys, int level);
size_t find_block(list * block_indexs, const f_str key);
size_t find_sst_file(list *sst_files, f_str key);
sst_f_inf * get_sst(sst_manager * mana, f_str targ, int level);
sst_f_inf *  allocate_sst(sst_manager * mana,  uint64_t num_keys, int level);
void gen_sst_fn(sst_manager * mana, char * out);
#endif