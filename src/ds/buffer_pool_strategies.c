#include "buffer_pool_stratgies.h"



int min_exp_from_bytes(uint64_t bucket_size) {
    return 63 - __builtin_clzll(bucket_size);
}
void init_vm_mapped(vm_mapped_strategy *vm, uint64_t mem_size, size_tier_config config){
    uint64_t curr = config.start;
    if (config.stop  < mem_size){
        config.stop= mem_size;
    }
    uint64_t iters =  ceil(log(config.stop / config.start) / log(config.multi)) + 1;
    vm->allocators = malloc(iters * sizeof(slab_allocator));
    for (int i = 0; i < iters; i++){
        vm->allocators[i] = create_allocator(curr, ceil_int_div(mem_size, curr));
        curr *= config.multi;
    }
    vm->config = config;
    vm->config.min_exp = min_exp_from_bytes(config.start);
}
void free_vm_mapped( buffer_pool * pool ){
    size_tier_config config = pool->vm.config;
    uint64_t iters =  ceil(log(config.stop / config.start) / log(config.multi));
    for (int i = 0; i < iters; i++){
        free_slab_allocator(&pool->vm.allocators[i]);
    }
    free(pool->vm.allocators);
}
void free_buddy(pinned_buddy_strategy pool){
    munmap((void*)pool.alloc.base, pool.alloc.arena_end - pool.alloc.base);

}
void init_buddy(pinned_buddy_strategy *pinned, uint64_t memsize,  size_tier_config config){
    pinned->pinned = mmap(NULL, memsize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    pinned->config = config;
    buddy_init(&pinned->alloc, pinned->pinned, memsize, log2(config.start), log2(config.stop));
    init_hot_caches(&pinned->alloc, config.hc_config);
}
static inline int get_bucket_idx(uint64_t size, int min_exp, int step) {
    if (size <= (1ULL << min_exp)) {
        return 0;
    }

    int exp_jump = __builtin_ctz(step);
    int exp = 64 - __builtin_clzll(size - 1);
    int rel_exp = exp - min_exp;
    int index = (rel_exp + exp_jump - 1) / exp_jump;

    return index;
}
static void * buddy_request_block(pinned_buddy_strategy pool, uint64_t size, uint64_t * real_size){
    return buddy_alloc(&pool.alloc, size, real_size);
}
void buddy_return_block(pinned_buddy_strategy pool, void * ptr, uint64_t real_size){
    buddy_free(&pool.alloc,ptr, real_size);
}
void * vm_request_block(vm_mapped_strategy pool, uint64_t size){
    uint8_t bucket=  get_bucket_idx(size, pool.config.min_exp, pool.config.multi);
    void  * chunk = slalloc(&pool.allocators[bucket], pool.allocators[bucket].pagesz);
    return chunk;
}
void vm_return_block(vm_mapped_strategy pool, uint64_t size, void * ptr){
    uint8_t bucket=  get_bucket_idx(size, pool.config.min_exp, pool.config.multi);
    slfree(&pool.allocators[bucket],ptr);
    madvise(ptr, pool.allocators[bucket].pagesz, MADV_DONTNEED);
}
byte_buffer *  get_buffer(buffer_pool * pool, uint64_t size){
    byte_buffer b;
    b.curr_bytes  = 0;
    b.read_pointer = 0;
    b.max_bytes = size;
        
    uint64_t real_size = 0;
    if (pool->allocated + size > pool->max) return NULL;
    if (pool->strat == VM_MAPPED){
        b.buffy = vm_request_block(pool->vm, size);
    }
    else if (pool->strat == PINNED_BUDDY){
        b.buffy = buddy_request_block(pool->pinned, size, &real_size);
        b.max_bytes = real_size;
    }
    if (b.buffy == NULL) return NULL;
    pool->allocated += b.max_bytes;
    byte_buffer * actual = request_struct(pool->empty_buffers);
    *actual = b;
    return actual;
}
void return_buffer_strat(buffer_pool * pool, byte_buffer * b){
    if (pool->strat == VM_MAPPED){
        vm_return_block(pool->vm, b->max_bytes, b->buffy);
    }
    else if (pool->strat == PINNED_BUDDY){
        buddy_return_block(pool->pinned, b->buffy, b->max_bytes);
    }
    pool->allocated -= b->max_bytes;
    return_struct(pool->empty_buffers, b, &reset_buffer);
}
buffer_pool make_b_p(uint8_t strategy,uint64_t mem_size, size_tier_config config){
    buffer_pool pool = {0};
    pool.strat = strategy;
    switch (strategy){
        case FIXED :
            break;
        case VM_MAPPED:
            init_vm_mapped(&pool.vm, mem_size, config);
            break;
        case PINNED_BUDDY:
            config.stop = mem_size;
            init_buddy(&pool.pinned, mem_size, config);
            break;
        default:
            break;
    }
    if (pool.strat != FIXED){
        uint64_t res = ceil_int_div(mem_size, config.start);
        pool.empty_buffers = create_pool(res);
        for (int i = 0; i < res; i++){
            insert_struct(pool.empty_buffers,create_empty_buffer());
        }
    }
    pool.max = mem_size;
    pool.allocated = 0;
    return pool;
}
void end_b_p(buffer_pool * pool){
    switch (pool->strat){
        case FIXED :
            break;
        case PINNED_BUDDY:
            free_buddy(pool->pinned);
            break;
        case VM_MAPPED:
            free_vm_mapped(pool);
            break;
        default:
            break;
    }
    free(pool->empty_buffers);
}