#include "sst_manager.h"


index_cache create_ind_cache(uint64_t max_partition_size, uint64_t block_index_sz, uint64_t mem_size){
    index_cache cache;
    cache.capacity = mem_size;
    cache.filled_pages = 0;
    cache.clock_hand = 0;
    uint64_t num_blocks=  ceil_int_div(max_partition_size, block_index_sz);
    uint64_t size_per_entry = num_blocks * block_ind_size();
    uint64_t num_entries = (size_per_entry > 0) ? (mem_size / size_per_entry) : 0;
    cache.frames = malloc(num_entries * sizeof(partiton_cache_element));
    cache.ref_bits = malloc(num_entries * sizeof(uint8_t));
    cache.max_pages = num_entries;
    return cache;
}
size_t ind_clock_evict(index_cache *c) {
    while (1) {
        size_t idx = c->clock_hand;
        partiton_cache_element * f = &c->frames[idx];
        if (c->ref_bits[idx] == 0 && f->ref_count == 0) {
            f->num_indexs = 0;
            c->clock_hand = (c->clock_hand + 1) % c->max_pages;
            return_buffer(f->b);
            return idx;
        }
        c->ref_bits[idx] = 0;
        c->clock_hand = (c->clock_hand + 1) % c->max_pages;
    }
}
block_index * allocate_blocks(uint64_t num, slab_allocator * block_allocator, uint64_t block_size){
    uint64_t tot = num * block_size;
    uint64_t fence_size = 40;
    block_index * alloc = slalloc(block_allocator, tot);
    for (int i = 0; i < num; i++){
        char * mem = slalloc(block_allocator, fence_size);
        alloc[i].min_key = f_str_alloc(mem);
    }
    return alloc;
}
bloom_filter * sst_make_bloom(uint64_t num_keys, uint64_t num_hash, const level_options *level_conf){
   uint8_t bits_per_word = 64;
   uint64_t required = num_keys * level_conf->bits_per_key;
   uint64_t words = ceil_int_div(required, bits_per_word);
   return bloom(num_hash,words, false, NULL);
}
bb_filter sst_make_part_bloom(uint64_t num_keys, const level_options *level_conf){
    bb_filter f;
    bb_filter_init_capacity(&f, num_keys, level_conf->bits_per_key);
    return f;
}
sst_f_inf *  allocate_sst(sst_manager * mana,  uint64_t num_keys, int level){
    sst_f_inf * base = NULL;
    uint64_t num_blocks = ceil_int_div(mana->config.levels[level].file_size, mana->config.block_index_size);
    char* mem_block = slalloc(&mana->sst_memory.cached, mana->sst_memory.cached.pagesz);
    base = (sst_f_inf*)mem_block;
    mem_block += sizeof(*base);
    *base = create_sst_filter(NULL);
    base->block_indexs->arr = allocate_blocks(num_blocks, &mana->sst_memory.non_cached, mana->size_per_block);
    base->block_indexs->cap = num_blocks;
    base->min = f_str_alloc(mem_block);
    base->max = f_str_alloc(mem_block + MAX_KEY_SIZE);
    base->file_name = mem_block + (MAX_KEY_SIZE * 2);
    base->filter = sst_make_bloom(num_keys, 2, &mana->config.levels[level]);
    return base;
}
sst_partition_ind* allocate_part(uint64_t num_keys, uint64_t num, sst_manager* mana, int level) {
    if (num == 0) return NULL;
    uint64_t keys_per_partition = ceil_int_div(num_keys, num);
    size_t size_per_part = sizeof(sst_partition_ind) + 40;
    sst_partition_ind* inds = slalloc(&mana->sst_memory.non_cached, size_per_part * num);
    char* memory_base = (char*)inds;
    for (uint64_t i = 0; i < num; i++) {
        sst_partition_ind* current_ind = (sst_partition_ind*)(memory_base + i * size_per_part);
        current_ind->min_fence = f_str_alloc((char*)(current_ind + 1));
        current_ind->filter = sst_make_part_bloom(keys_per_partition, &mana->config.levels[level]);
        current_ind->pg = NULL;
        current_ind->num_blocks = 0;
        current_ind->blocks = NULL;
    }
    return (sst_partition_ind*)memory_base;
}
sst_f_inf * allocate_non_l0(sst_manager * mana, uint64_t num_keys, int level){
    sst_f_inf * base;
    if (level <= mana->cached_levels){
        return allocate_sst(mana, num_keys, level);
    }
    char* mem_block = slalloc(&mana->sst_memory.non_cached, sizeof(sst_f_inf));
    base = (sst_f_inf*)mem_block;
    mem_block += sizeof(*base);
    const level_options *level_conf = &mana->config.levels[level];
    uint64_t parts_per = ceil_int_div(level_conf->file_size, level_conf->partition_size);
    base->sst_partitions->arr = allocate_part(num_keys, parts_per, mana, level);
    base->sst_partitions->cap = parts_per;
    base->sst_partitions->len = parts_per > 0 ? parts_per : 0;
    return base;
}
sst_f_inf * allocate_level(sst_manager * mana, uint64_t num_keys, int level){
    if (level == 0 ) return allocate_sst(mana, num_keys, level);
    return allocate_non_l0(mana, num_keys, level);
}
static size_t get_free_frame(index_cache *c) {
    if (c->filled_pages < c->max_pages) {
        return c->filled_pages++;
    }
    return ind_clock_evict(c);
}
partiton_cache_element * get_part(index_cache *c, sst_partition_ind * ind, const char * fn){
    if (ind->pg){
        return ind->pg;
    }
    db_FILE * targ = dbio_open(fn, 'r');
    byte_buffer * buffer = select_buffer(ind->len);
    set_context_buffer(targ, buffer);
    uint64_t read = dbio_read(targ, ind->off, ind->len);
    if (read < ind->len){
        dbio_close(targ);
        return NULL;
    }
    size_t free_idx = get_free_frame(c);
    partiton_cache_element * ele = &c->frames[free_idx];
    all_stream_index(ind->num_blocks, buffer, ele->block_indexs);
    deseralize_filter_head(buffer,&ele->filter);
    deseralize_filter_body(buffer, &ele->filter);
    ind->blocks = ele->block_indexs;
    ind->pg = ele;
    ele->ref_count = 1;
    ele->b = buffer;
    dbio_close(targ);
    return ele;
}
void free_ind_cache(index_cache * c){
    free(c->frames);
    free(c->ref_bits);
}
void pin_part(sst_partition_ind * ind){
    partiton_cache_element * ele = ind->pg;
    if(ele) ele->ref_count++;
}
void unpin_part(sst_partition_ind * ind){
    partiton_cache_element * ele = ind->pg;
    if(!ele) return;
    ele->ref_count--;
    if (ele->ref_count <= 0) {
        return_buffer(ele->b);
        ind->pg = NULL;
    }
}
uint64_t calculate_cached_size(const sst_man_sst_inf_cf *config, int level){
    uint64_t blocks_per_sst = ceil_int_div(config->levels[level].file_size, config->block_index_size);
    uint64_t size_from_blocks = blocks_per_sst * block_ind_size();
    uint64_t list_size = sizeof(list);
    uint64_t base_size = MAX_KEY_SIZE * 2 + MAX_F_N_SIZE + sizeof(sst_f_inf);
    return list_size + base_size + size_from_blocks;
}
static uint64_t get_num_parts(const sst_man_sst_inf_cf *config, int level){
    return ceil_int_div(config->levels[level].file_size, config->levels[level].partition_size);
}
static uint64_t calculate_non_cached_size(const sst_man_sst_inf_cf *config, int level){
    uint64_t parts_per_sst = get_num_parts(config, level);
    uint64_t list_size = sizeof(list);
    uint64_t base_size = MAX_KEY_SIZE * 2 + MAX_F_N_SIZE + sizeof(sst_f_inf);
    uint64_t part_size = sizeof(sst_partition_ind) + 40;
    return (parts_per_sst * part_size) + base_size + list_size;
}
sst_allocator create_sst_all(const sst_man_sst_inf_cf *config, int cached_levels){
    sst_allocator allocator;
    uint64_t max_cached_sz = 0;
    for (int i = 0; i <= cached_levels; i++) {
        uint64_t current_size = calculate_cached_size(config, i);
        if (current_size > max_cached_sz) max_cached_sz = current_size;
    }

    uint64_t max_non_cached_sz = 0;
    for (int i = cached_levels + 1; i < MAX_LEVEL_SETTINGS; i++) {
        uint64_t current_size = calculate_non_cached_size(config, i);
        if (current_size > max_non_cached_sz) max_non_cached_sz = current_size;
    }

    allocator.cached = create_allocator(max_cached_sz > 0 ? max_cached_sz : 1, 1024);
    allocator.non_cached = create_allocator(max_non_cached_sz > 0 ? max_non_cached_sz : 1, 1024);
    return allocator;
}
sst_manager create_manager(sst_man_sst_inf_cf config, uint64_t mem_size){
    sst_manager mana;
    mana.config = config;
    mana.l_0 = List(32, sizeof(sst_f_inf), false);
    for (int i = 0; i < 6; i++){
        mana.non_zero_l[i] = create_sst_sl(1024);
    }
    mana.cached_levels = 1;

    uint64_t max_part_size = 0;
    for (int i = 0; i < MAX_LEVEL_SETTINGS; i++) {
        if (config.levels[i].partition_size > max_part_size) {
            max_part_size = config.levels[i].partition_size;
        }
    }
    mana.cache = create_ind_cache(max_part_size, config.block_index_size, mem_size);
    mana.size_per_block = block_ind_size();
    mana.sst_memory = create_sst_all(&config, mana.cached_levels);
    return mana;
}
void free_manager(sst_manager * manager){
    free_ind_cache(&manager->cache);
    free_list(manager->l_0, NULL);
    for (int i = 0; i < 6; i++){
        freesst_sl(manager->non_zero_l[i]);
    }
}
void add_sst(sst_manager * mana, sst_f_inf* sst, int level){
    if (level <= 0){
        insert(mana->l_0, sst);
    } else {
        sst_insert_list(mana->non_zero_l[level], sst);
    }
}
void remove_sst(sst_manager * mana, sst_f_inf * sst, int level){
    if (level <= 0){
        sst_f_inf * l = mana->l_0->arr;
        for (size_t i = 0; i < mana->l_0->len; i++){
            if (&l[i] == sst){
                free_sst_inf(sst);
                remove_at(mana->l_0, i);
                return;
            }
        }
    } else {
        sst_delete_element(mana->non_zero_l[level], sst->min);
        slfree_full_slab(&mana->sst_memory.non_cached, sst);
    }
}
size_t find_sst_file(list *sst_files, f_str key) {
    size_t max_index = sst_files->len;
    size_t min_index = 0;
    sst_f_inf* l = sst_files->arr;
    while (min_index < max_index) {
        size_t middle_index = min_index + (max_index - min_index) / 2;
        sst_f_inf *sst = &l[middle_index];
        if (f_cmp(key, sst->min) >= 0 && f_cmp(key, sst->max)  <= 0) {
            return middle_index;
        }
        else if (f_cmp(key, sst->max) > 0) {
            min_index = middle_index + 1;
        }
        else {
            max_index = middle_index;
        }
    }
    return -1;
}
sst_f_inf * get_sst(sst_manager * mana, f_str targ, int level){
    if (level <= 0){
        size_t ind = find_sst_file(mana->l_0, targ);
        return (ind != -1) ? at(mana->l_0, ind) : NULL;
    } else {
        sst_node * node = sst_search_list_prefix(mana->non_zero_l[level], targ);
        return (node) ? node->inf : NULL;
    }
}
void free_sst_man(sst_manager * man){
    free_list(man->l_0, &free_sst_inf);
    for (int i = 0; i < 6; i++){
        freesst_sl(man->non_zero_l[i]);
    }
}
size_t find_block(list * block_indexs, const f_str key) {
    int left = 0, right = block_indexs->len - 1, mid = 0;
    block_index * arr = block_indexs->arr;
    while (left <= right) {
        mid = left + (right - left) / 2;
        block_index* index = &arr[mid];
        block_index* next_index = (mid + 1 < block_indexs->len) ? &arr[mid+1] : NULL;
        if (f_cmp(key, index->min_key) >= 0 && (next_index == NULL || f_cmp(key, next_index->min_key) < 0)) {
            return mid;
        }
        if (f_cmp(key, index->min_key) < 0) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return -1;
}
size_t find_parition(sst_f_inf *sst, const f_str key ){
    int left = 0, right = sst->sst_partitions->len - 1, mid = 0;
    while (left <= right) {
        mid = left + (right - left) / 2;
        sst_partition_ind *index = at(sst->sst_partitions, mid);
        sst_partition_ind *next_index = at(sst->sst_partitions, mid + 1);
        if ((f_cmp(key, index->min_fence) >= 0 && (next_index == NULL || f_cmp(key, next_index->min_fence) < 0))) {
            return mid;
        }
        if (f_cmp(key, index->min_fence) < 0) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return -1;
}
block_index * get_block(sst_manager * mana, sst_f_inf * inf, f_str target, int level){
    if (level <= mana->cached_levels){
        size_t ind = find_block(inf->block_indexs, target);
        return (ind != -1) ? &((block_index*)inf->block_indexs->arr)[ind] : NULL;
    }
    size_t ind = find_parition(inf, target);
    if (ind == -1) return NULL;
    sst_partition_ind * part = at(inf->sst_partitions, ind);
    if (!part->pg){
        get_part(&mana->cache, part, inf->file_name);
        if (!part->pg) return NULL;
    }
    list l = { .arr = part->blocks, .len = part->num_blocks, .cap = part->num_blocks };
    size_t block_ind = find_block(&l, target);
    return (block_ind != -1) ? &part->blocks[block_ind] : NULL;
}
int check_sst(sst_manager * mana, sst_f_inf * inf, const f_str u , int level){
    if (level <= mana->cached_levels){
        return check_bit(u.entry, inf->filter) ? 1 : 0;
    }
    size_t ind = find_parition(inf, u);
    if(ind == -1) return 0;
    sst_partition_ind * part = at(inf->sst_partitions, ind);
    return bb_filter_may_contain(&part->filter, u) ? (int)ind + 1 : 0;
}
block_index * try_get_relevant_block(sst_manager * mana, sst_f_inf * inf, const f_str u, int level){
    int res = check_sst(mana, inf, u, level);
    if (res == 0) return NULL;
    return get_block(mana, inf, u, level);
}
uint64_t get_num_l_0(sst_manager * mana){
    return mana->l_0->len;
}
void free_sst_sst_man(sst_manager * mana, sst_f_inf * inf, int level){
    if (level <= 0 ){
        slfree_full_slab(&mana->sst_memory.non_cached, inf);
    }
}
void gen_sst_fn(sst_manager * mana, const char * out){
    gen_file_name(out, new_value(&mana->name_gen), SST_F_XT, 3);

}