#include "speedy_bloom.h"
#include <immintrin.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#ifndef BB_HAS_AVX2
#define BB_HAS_AVX2 1
#endif


#ifndef C_LINE
#define C_LINE 64
#endif

#ifndef BB_MASK_IDX_BITS
#define BB_MASK_IDX_BITS 12
#endif


static inline uint64_t bb_mix64(uint64_t h) {
    h ^= h >> 32;
    h *= 0x4cf2d24a9f24e7a5ULL;
    h ^= h >> 32;
    return h;
}
static inline uint32_t bb_ceil_pow2_u32_impl(uint32_t x) {
    if (x <= 1) return 1;
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x++;
    return x;
}

uint32_t bb_ceil_pow2_u32(uint32_t x){
    return bb_ceil_pow2_u32_impl(x);
}
void seralize_filter(byte_buffer * b, bb_filter f){
    write_int32(b, (int32_t)f.bucket_mask);
    write_int32(b, (int32_t)f.bucket_cnt);
    const uint64_t size = (uint64_t)(BB_BUCKET_BITS/8u) * (uint64_t)f.bucket_cnt;
    write_buffer(b, f.data, (size_t)size);
}

void deseralize_filter_head(byte_buffer * b, bb_filter * f){
    f->bucket_mask = (uint32_t)read_int32(b);
    f->bucket_cnt  = (uint32_t)read_int32(b);
}

void deseralize_filter_body(byte_buffer * b, bb_filter * f){
    f->data = (uint32_t *)get_curr(b);
}

static inline int bb_alloc_table(bb_filter *f){
    const size_t bytes = (size_t)f->bucket_cnt * (size_t)(BB_BUCKET_BITS/8u);
    void *ptr = NULL;
    int rc = posix_memalign(&ptr, C_LINE, bytes);
    if (rc != 0 || !ptr) return -1;
    memset(ptr, 0, bytes);
    f->data = (uint32_t*)ptr;
    return 0;
}
int bb_filter_init_pow2(bb_filter *f, uint32_t log2_buckets){
    f->bucket_cnt  = 1u << log2_buckets;
    f->bucket_mask = f->bucket_cnt - 1u;
    return bb_alloc_table(f);
}

int bb_filter_init_capacity(bb_filter *f, size_t keys, double bits_per_key){
    const double total_bits = (double)keys * bits_per_key;
    uint64_t buckets = (uint64_t)(total_bits / (double)BB_BUCKET_BITS + 0.5);
    if (buckets == 0) buckets = 1;
    buckets = bb_ceil_pow2_u32_impl((uint32_t)buckets);
    return bb_filter_init_pow2(f, (uint32_t)__builtin_ctz((unsigned)buckets));
}

static inline void bb_derive_bits_km(uint64_t h, uint32_t bits_out[BB_K]){
    uint64_t h1 = h;
    uint64_t h2 = (bb_mix64(h ^ 0xD6E8FEB86659FD93ULL) | 1ull);
    for (uint32_t i = 1; i < BB_K; ++i){
        uint64_t gi = h1 + (uint64_t)i * h2;
        gi = bb_mix64(gi);

        bits_out[i] = (uint32_t)(gi >> 32);
    }
}

static inline uint64_t bb_construct_mask(uint64_t h) {

    const uint64_t increment = 0x9E3779B97F4A7C15ULL;
    uint64_t m = 0;
    for (uint32_t i = 0; i < BB_K; ++i) {
        h += increment;
        uint32_t bit = (uint32_t)(bb_mix64(h) >> 32);
        m |= 1ULL << (bit & 63);
    }
    return m ? m : 1ULL;

}
void bb_calc_bucket_bits(uint64_t h, const bb_filter *f, uint32_t *bucket_out, uint32_t bits[BB_K]){
    *bucket_out = (uint32_t)h & f->bucket_mask;
    const uint64_t hash_for_mask = h >> 32;
    bb_derive_bits_km(hash_for_mask, bits);
}

static inline uint64_t* bb_bucket_ptr64(const bb_filter *f, uint32_t bucket_idx){
    return (uint64_t*)((uint8_t*)f->data + ((size_t)bucket_idx * (BB_BUCKET_BITS/8u)));
}

bool bb_bucket_test_scalar(uint32_t *bucket, const uint32_t bits[BB_K]){
    uint64_t mask = 0;
    for (uint32_t i = 0; i < BB_K; ++i) mask |= (1ull << (bits[i] & 63u));
    uint64_t v = *(const uint64_t*)bucket;
    return (v & mask) == mask;
}

void bb_bucket_set_scalar(uint32_t *bucket, const uint32_t bits[BB_K]){
    uint64_t mask = 0;
    for (uint32_t i = 0; i < BB_K; ++i) mask |= (1ull << (bits[i] & 63u));
    uint64_t v = *(const uint64_t*)bucket;
    v |= mask;
    *(uint64_t*)bucket = v;
}

#if BB_HAS_AVX2
__attribute__((target("avx2")))
bool bb_bucket_test_avx2(uint32_t *bucket, const uint32_t bits[BB_K]){
    uint64_t mask = 0;
    for (uint32_t i = 0; i < BB_K; ++i) mask |= (1ull << (bits[i] & 63u));
    uint64_t v = *(const uint64_t*)bucket;
    return (v & mask) == mask;
}

__attribute__((target("avx2")))
void bb_bucket_set_avx2(uint32_t *bucket, const uint32_t bits[BB_K]){
    uint64_t mask = 0;
    for (uint32_t i = 0; i < BB_K; ++i) mask |= (1ull << (bits[i] & 63u));
    uint64_t v = *(const uint64_t*)bucket;
    v |= mask;
    *(uint64_t*)bucket = v;
}
#else
bool bb_bucket_test_avx2(uint32_t *bucket, const uint32_t bits[BB_K]){
    return bb_bucket_test_scalar(bucket, bits);
}
void bb_bucket_set_avx2(uint32_t *bucket, const uint32_t bits[BB_K]){
    bb_bucket_set_scalar(bucket, bits);
}
#endif

bool bb_filter_may_contain(const bb_filter *f, f_str key){
    const uint64_t h = rapidhash(key.entry, key.len);
    const uint32_t bucket_idx = (uint32_t)h & f->bucket_mask;
    const uint64_t hash_for_mask = h >> 32;
    uint64_t *p = bb_bucket_ptr64(f, bucket_idx);
    const uint64_t mask = bb_construct_mask(hash_for_mask);
    return ((*p) & mask) == mask;
}

void bb_filter_add(bb_filter *f, f_str key){
    const uint64_t h = rapidhash(key.entry, key.len);
    const uint32_t bucket_idx = (uint32_t)h & f->bucket_mask;
    const uint64_t hash_for_mask = h >> 32;
    uint64_t *p = bb_bucket_ptr64(f, bucket_idx);
    const uint64_t mask = bb_construct_mask(hash_for_mask);
    *p |= mask;
}