
#define _GNU_SOURCE
#include "buddy_page.h"

#define NIL_OFF 0xFFFFFFFFu     

static inline void hdr_set_free(void *blk, uint8_t order){ 
    ((uint8_t *)blk)[0] = 0x80 | order; 
}

static inline bool hdr_is_free(const void *blk){ 
    return ((const uint8_t *)blk)[0] & 0x80; 
}

static inline uint8_t hdr_order(const void *blk){ 
    return ((const uint8_t *)blk)[0] & 0x7F; 
}


static inline uint32_t rel_off(const buddy_allocator *ba, const void *p){ 
    return (uint32_t)((uintptr_t)p - ba->base); 
}

static inline void *abs_ptr(const buddy_allocator *ba, uint32_t off){ 
    return (void *)(ba->base + off); 
}
static inline size_t order_to_size(uint8_t order) {
    return 1ULL << order;
}
static inline unsigned ctz64(uint64_t x)
{
#if defined(__GNUC__) || defined(__clang__)
    return (unsigned)__builtin_ctzll(x);
#else
    static const int idx[64] = {};
    return idx[((x & -x) * 0x07EDD5E59A4E28C2ULL) >> 58];
#endif
}

static inline void list_push(buddy_allocator *ba, uint8_t order, void *blk){
    struct freelink *node = (struct freelink *)((uint8_t *)blk + 8);
    uint32_t off = rel_off(ba, blk);

    node->prev = NIL_OFF;
    node->next = ba->head[order].next;

    if (node->next != NIL_OFF)
        ((struct freelink *)((uint8_t *)abs_ptr(ba, node->next) + 8))->prev = off;

    ba->head[order].next = off;
    ba->available_mask  |= 1ULL << order;
}

static inline void list_remove(buddy_allocator *ba, uint8_t order, void *blk){
    struct freelink *node = (struct freelink *)((uint8_t *)blk + 8);

    if (node->prev != NIL_OFF){
        ((struct freelink *)((uint8_t *)abs_ptr(ba, node->prev) + 8))->next = node->next;
    }
    else{
        ba->head[order].next = node->next;
    }
    if (node->next != NIL_OFF){
        ((struct freelink *)((uint8_t *)abs_ptr(ba, node->next) + 8))->prev = node->prev;
    }   

    if (ba->head[order].next == NIL_OFF){
        ba->available_mask &= ~(1ULL << order);
    }
}

static inline void *list_pop(buddy_allocator *ba, uint8_t order){
    uint32_t off = ba->head[order].next;
    if (off == NIL_OFF) return NULL;

    void *blk = abs_ptr(ba, off);
    list_remove(ba, order, blk);
    return blk;
}
static inline uint8_t order_for(size_t size, uint8_t min_order){
    if (size == 0) size = 1;
    size_t v = size - 1;

#if defined(__GNUC__) || defined(__clang__)
    uint8_t o = 8 * sizeof(unsigned long long) - (uint8_t)__builtin_clzll(v);
#else
    uint8_t o = 0; while (v >>= 1) ++o;
#endif
    return o < min_order ? min_order : o;
}


void buddy_init(buddy_allocator *ba, void *heap, size_t heap_size, uint8_t min_order, uint8_t max_order){
    assert(max_order <= HARD_MAX_ORDER && "max_order too large");
    assert(min_order <= max_order           && "min_order > max_order");
    assert((heap_size & ((1ULL << min_order) - 1)) == 0 &&
           "heap must be aligned to min block size");

    memset(ba, 0, sizeof(*ba));
    ba->base       = (uintptr_t)heap;
    ba->min_order  = min_order;
    ba->max_order  = max_order;

    for (int i = 0; i < ORDER_COUNT; i++){
        ba->head[i].next = ba->head[i].prev = NIL_OFF;
    }
        

    uintptr_t curr = ba->base;
    uintptr_t end  = ba->base + heap_size;

    while (curr < end) {
        uintptr_t remaining = end - curr;
        uint8_t   k = max_order;
        while ((1ULL << k) > remaining) --k;

        hdr_set_free((void *)curr, k);
        list_push(ba, k, (void *)curr);

        curr += 1ULL << k;
    }

    ba->arena_end = end;
}
static void load_pool(buddy_allocator * A, uint64_t per, struct_pool * pool, uint64_t memsize){
    uint64_t added = 0;
    uint64_t dummy = 0;
    while (added + per < memsize){
        void * ptr = buddy_alloc(A, per, &dummy);
        insert_struct(pool, ptr);
        added+= per;
    }
}
void init_hot_caches(buddy_allocator *ba, hotcache_config config ){

    for (int i = 0; i < config.num ; i++){
        uint64_t memsize = config.total_per[i];
        ba->caches[i].order = config.size_tiers[i];
        uint64_t size = 1 << config.size_tiers[i];
        struct_pool * t = create_pool(size);
        load_pool(ba, size, t, memsize);
        ba->caches[i].queue =*t;
        free(t);
        ba->hot_cache_mask |= 1ULL << config.size_tiers[i];
    }
}
static inline int cache_index(uint8_t order, uint64_t hot_mask){
    uint64_t below = hot_mask & ((1ULL << order) - 1);
    return (int)__builtin_popcountll(below);
}

void * cache_get(buddy_allocator *ba, uint8_t order){
    struct_pool * targ = &ba->caches[cache_index(order, ba->hot_cache_mask)].queue; 
    return request_struct(targ);
}
static int cache_free(buddy_allocator *ba, void * ptr, uint8_t order){
    struct_pool * targ = &ba->caches[cache_index(order, ba->hot_cache_mask)].queue; 
    return return_struct(targ,ptr, NULL);
}
static inline bool is_allowed_pow2_rt(const buddy_allocator *ba, uint64_t order){
    return (ba->hot_cache_mask >>  order) & 1U;
}
void *buddy_alloc(buddy_allocator *ba, size_t size, uint64_t * real_size){
    uint8_t need = order_for(size, ba->min_order);
    if (is_allowed_pow2_rt(ba, need)) {
        void *p = cache_get(ba, need);                 
        if (p) return p;
    }
    if (need > ba->max_order) return NULL;           
    uint64_t mask = ba->available_mask >> need;

    uint8_t order = (uint8_t)(ctz64(mask) + need);

    if (!mask) return NULL;
    void *blk = list_pop(ba, order);

    while (order > need) {
        --order;
        void *buddy = (void *)((uintptr_t)blk + (1ULL << order));
        hdr_set_free(buddy, order);
        list_push(ba, order, buddy);
    }
    *real_size = order_to_size(order);
    return blk;
}

static inline void *buddy_of(const buddy_allocator *ba, const void *blk, uint8_t order){
    uintptr_t offset = (uintptr_t)blk - ba->base;
    return (void *)(ba->base + (offset ^ (1ULL << order)));
}

void buddy_free(buddy_allocator *ba, void *ptr, uint64_t real_size){
    uint8_t order = order_for(real_size, ba->min_order);
    if (is_allowed_pow2_rt(ba, order) && cache_free(ba, ptr, order)) {
        return;
    }
    void * blk = ptr;

    while (order < ba->max_order) {
        void *buddy = buddy_of(ba, blk, order);
        if (!hdr_is_free(buddy) || hdr_order(buddy) != order) break;
        list_remove(ba, order, buddy);
        blk = buddy < blk ? buddy : blk;
        ++order;
    }

    hdr_set_free(blk, order);
    list_push(ba, order, blk);
}
