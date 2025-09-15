#ifndef SLAB_H
#define SLAB_H
#include "structure_pool.h"
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#define _GNU_SOURCE
#include <sys/mman.h>
typedef struct slab_allocator {
    struct_pool *pool;
    char *region;
    size_t pagesz, count;
    uint8_t *rc;
    char *curr;
    size_t curr_off;
    size_t pagesz_shift;
} slab_allocator;

static void *portable_mremap(void *old_ptr, size_t old_size, size_t new_size) {
    if (new_size == 0) {
        munmap(old_ptr, old_size);
        return NULL;
    }
    void *new_ptr = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANON, -1, 0);

    if (new_ptr == MAP_FAILED)
        return NULL;

    size_t copy_size = old_size < new_size ? old_size : new_size;
    memcpy(new_ptr, old_ptr, copy_size);
    munmap(old_ptr, old_size);

    return new_ptr;
}
static inline slab_allocator create_allocator(size_t pagesz, size_t slab_count){
    slab_allocator A={0};
    pagesz = round_up_pow2(pagesz);
    A.pagesz = pagesz;
    A.count = slab_count;
    A.region= mmap(NULL, pagesz*slab_count, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);
    A.pool = create_pool(slab_count);
    A.rc = calloc(slab_count, sizeof (*A.rc));
    for(size_t i=0; i<slab_count;i++){
        insert_struct(A.pool, A.region + i*pagesz);\
    }
    A.pagesz_shift = __builtin_ctz(pagesz);
    return A;
}

static inline void *slalloc(slab_allocator *A, size_t size){
    if(size == A->pagesz){
        char *s = request_struct(A->pool);
        if(!s) return NULL;
        A->rc[(s - A->region)/A->pagesz] = 1;
        return s;
    }
    if(!A->curr || A->curr_off + size > A->pagesz){
        A->curr = request_struct(A->pool);
        if(!A->curr) return NULL;
        A->curr_off = 0;
    }
    size_t idx = (A->curr - A->region) >> A->pagesz_shift;
    A->rc[idx]++;
    void *r = A->curr + A->curr_off;
    A->curr_off += size;
    return r;
}
static inline void slfree_full_slab(slab_allocator *A, void *ptr){
    if(!ptr) return;
    char  *base = (char*)((uintptr_t)ptr & ~(A->pagesz - 1));
    if(base == A->curr){
        A->curr = NULL; 
        A->curr_off = 0; 
    }
    return_struct(A->pool, base, NULL);
}
static inline void slfree(slab_allocator *A, void *ptr){
    if(!ptr) return;
    char  *base = (char*)((uintptr_t)ptr & ~(A->pagesz - 1));
    size_t idx  = (base - A->region)  >> A->pagesz_shift;
    if(--A->rc[idx]) return;
    if(base == A->curr){
        A->curr = NULL; 
        A->curr_off = 0; 
    }
    return_struct(A->pool, base, NULL);
}

static inline int expand_allocator(slab_allocator *A, size_t more){
    size_t oldc = A->count, newc = oldc + more;
    size_t oldsz = A->pagesz * oldc, newsz = A->pagesz * newc;
    char *oldr = A->region;
    char *newr = portable_mremap(oldr, oldsz, newsz);
    if(newr == MAP_FAILED) return -1;
    A->region = newr;
    uint8_t *nrc = realloc(A->rc, newc * sizeof *nrc);
    if(!nrc) return -1;
    A->rc = nrc;
    memset(A->rc + oldc, 0, more * sizeof *A->rc);
    struct_pool *p = A->pool;
    p->queue_buffer= realloc(p->queue_buffer, newc * sizeof *p->queue_buffer);
    p->all_ptrs= realloc(p->all_ptrs,  newc * sizeof *p->all_ptrs);
    p->capacity =  newc;
    for(size_t i=oldc;i<newc;i++){
        insert_struct(p, newr + i*A->pagesz);
    }
    A->count = newc;
    return 0;
}

static inline void free_slab_allocator(slab_allocator * A){
    
    munmap(A->region, A->pagesz * A->count);
    free(A->rc);
    free_pool(A->pool, NULL);
    A->region = NULL;
    A->rc = NULL;
    A->pool = NULL;
}

#endif
