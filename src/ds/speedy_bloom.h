#ifndef BLOCKED_BLOOM_H
#define BLOCKED_BLOOM_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include "hashtbl.h"
#include <immintrin.h>
#include "../db/backend/key-value.h"
#define BB_HAS_AVX2 1

/*register blocked bloom filter with no bulk tests*/
typedef struct {
    uint32_t *data;
    uint32_t bucket_mask;
    uint32_t bucket_cnt;
} bb_filter;
#define BB_K 8u
uint64_t bb_mix64(uint64_t x);
uint32_t bb_ceil_pow2_u32(uint32_t x);
void seralize_filter(byte_buffer * b, bb_filter f);
void deseralize_filter_head(byte_buffer * b, bb_filter * f);
void deseralize_filter_body(byte_buffer * b, bb_filter * f);
int bb_filter_init_pow2(bb_filter *f, uint32_t log2_buckets);
int bb_filter_init_capacity(bb_filter *f, size_t keys, double bits_per_key);
void bb_calc_bucket_bits(uint64_t h, const bb_filter *f, uint32_t *bucket_out, uint32_t bits[BB_K]);
bool bb_bucket_test_scalar(uint32_t *bucket, const uint32_t bits[BB_K]);
void bb_bucket_set_scalar(uint32_t *bucket, const uint32_t bits[BB_K]);
bool bb_bucket_test_avx2(uint32_t *bucket, const uint32_t bits[BB_K]);
void bb_bucket_set_avx2(uint32_t *bucket, const uint32_t bits[BB_K]);
bool bb_filter_may_contain(const bb_filter *f,  f_str key);
void bb_filter_add(bb_filter *f, f_str key);

#endif