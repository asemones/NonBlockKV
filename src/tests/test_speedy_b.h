#include "unity/src/unity.h"
#include "../ds/speedy_bloom.h"
#include "../ds/bloomfilter.h"
#include "../ds/byte_buffer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
/*playing around with ai gen for tests
turns out it really sucks at writing them
code reuse for tests matters too!*/
#define C_LINE 64
#define BB_BUCKET_WORDS 16u

static bb_filter test_filter;
static uint32_t* filter_data = NULL;
static uint32_t* bucket_scalar = NULL;
static uint32_t* bucket_avx = NULL;

void start_test_resources(void) {
    bucket_scalar = (uint32_t*)aligned_alloc(C_LINE, C_LINE);
    bucket_avx = (uint32_t*)aligned_alloc(C_LINE, C_LINE);
    memset(bucket_scalar, 0, C_LINE);
    memset(bucket_avx, 0, C_LINE);
}

void stop_test_resources(void) {
    if (filter_data) {
        free(filter_data);
        filter_data = NULL;
    }
    free(bucket_scalar);
    free(bucket_avx);
    bucket_scalar = NULL;
    bucket_avx = NULL;
}

void test_bb_ceil_pow2_u32(void) {
    TEST_ASSERT_EQUAL_UINT32(1, bb_ceil_pow2_u32(0));
    TEST_ASSERT_EQUAL_UINT32(1, bb_ceil_pow2_u32(1));
    TEST_ASSERT_EQUAL_UINT32(2, bb_ceil_pow2_u32(2));
    TEST_ASSERT_EQUAL_UINT32(4, bb_ceil_pow2_u32(3));
    TEST_ASSERT_EQUAL_UINT32(1024, bb_ceil_pow2_u32(1023));
    TEST_ASSERT_EQUAL_UINT32(1024, bb_ceil_pow2_u32(1024));
    TEST_ASSERT_EQUAL_UINT32(2048, bb_ceil_pow2_u32(1025));
}

void test_bb_filter_init(void) {
    bb_filter_init_pow2(&test_filter, 10);
    TEST_ASSERT_EQUAL_UINT32(1024, test_filter.bucket_cnt);
    TEST_ASSERT_EQUAL_UINT32(1023, test_filter.bucket_mask);

    bb_filter_init_capacity(&test_filter, 10000, 10.0);
    TEST_ASSERT_EQUAL_UINT32(2048, test_filter.bucket_cnt);
    TEST_ASSERT_EQUAL_UINT32(2047, test_filter.bucket_mask);
}

void test_bucket_scalar_operations(void) {
    start_test_resources();
    uint32_t bits[BB_K] = {1, 33, 65, 97, 129, 257, 385, 511};
    TEST_ASSERT_FALSE(bb_bucket_test_scalar(bucket_scalar, bits));
    bb_bucket_set_scalar(bucket_scalar, bits);
    TEST_ASSERT_TRUE(bb_bucket_test_scalar(bucket_scalar, bits));

    uint32_t other_bits[BB_K] = {2, 34, 66, 98, 130, 258, 386, 510};
    TEST_ASSERT_FALSE(bb_bucket_test_scalar(bucket_scalar, other_bits));
    stop_test_resources();
}

void test_bucket_avx2_operations(void) {
    start_test_resources();
    uint32_t bits[BB_K] = {1, 33, 65, 97, 129, 257, 385, 511};
    TEST_ASSERT_FALSE(bb_bucket_test_avx2(bucket_avx, bits));
    bb_bucket_set_avx2(bucket_avx, bits);
    TEST_ASSERT_TRUE(bb_bucket_test_avx2(bucket_avx, bits));
    
    uint32_t other_bits[BB_K] = {2, 34, 66, 98, 130, 258, 386, 510};
    TEST_ASSERT_FALSE(bb_bucket_test_avx2(bucket_avx, other_bits));
    stop_test_resources();
}

void test_consistency_scalar_vs_avx2(void) {
    start_test_resources();
    uint32_t bits[BB_K] = {10, 20, 30, 40, 500, 480, 321, 123};
    bb_bucket_set_scalar(bucket_scalar, bits);
    bb_bucket_set_avx2(bucket_avx, bits);
    TEST_ASSERT_EQUAL_MEMORY(bucket_scalar, bucket_avx, C_LINE);
    TEST_ASSERT_TRUE(bb_bucket_test_scalar(bucket_avx, bits));
    TEST_ASSERT_TRUE(bb_bucket_test_avx2(bucket_scalar, bits));
    stop_test_resources();
}

void test_full_filter_add_and_contain(void) {
    start_test_resources();
    bb_filter_init_capacity(&test_filter, 100, 10.0);
    size_t data_size = (size_t)test_filter.bucket_cnt * C_LINE;
    filter_data = (uint32_t*)aligned_alloc(C_LINE, data_size);
    memset(filter_data, 0, data_size);
    test_filter.data = filter_data;

    // FIX: Use designated initializers to avoid ambiguity and warnings.
    f_str key1 = {.mem = "hello", .len = 5};
    f_str key2 = {.mem = "world", .len = 5};
    f_str key_absent = {.mem = "not_present", .len = 11};

    TEST_ASSERT_FALSE(bb_filter_may_contain(&test_filter, key1));
    TEST_ASSERT_FALSE(bb_filter_may_contain(&test_filter, key2));

    bb_filter_add(&test_filter, key1);
    TEST_ASSERT_TRUE(bb_filter_may_contain(&test_filter, key1));
    TEST_ASSERT_FALSE(bb_filter_may_contain(&test_filter, key2));
    TEST_ASSERT_FALSE(bb_filter_may_contain(&test_filter, key_absent));
    
    bb_filter_add(&test_filter, key2);
    TEST_ASSERT_TRUE(bb_filter_may_contain(&test_filter, key1));
    TEST_ASSERT_TRUE(bb_filter_may_contain(&test_filter, key2));
    TEST_ASSERT_FALSE(bb_filter_may_contain(&test_filter, key_absent));
    stop_test_resources();
}

void test_serialization(void) {
    start_test_resources();
    bb_filter_init_capacity(&test_filter, 100, 10.0);
    size_t data_size = (size_t)test_filter.bucket_cnt * sizeof(uint64_t);

    filter_data = (uint32_t*)aligned_alloc(C_LINE, data_size);
    TEST_ASSERT_NOT_NULL(filter_data); // Crucial check!
    test_filter.data = filter_data;

    for(size_t i = 0; i < test_filter.bucket_cnt * 2; ++i) {
        test_filter.data[i] = (uint32_t)i;
    }
    size_t buffer_size = sizeof(int32_t) * 2 + data_size;
    byte_buffer * b = create_buffer(buffer_size);

    seralize_filter(b, test_filter);
    TEST_ASSERT_EQUAL_UINT64(buffer_size, b->curr_bytes);

    bb_filter f_new = {0};
    f_new.data =  filter_data;
    deseralize_filter_head(b, &f_new);
    
    TEST_ASSERT_EQUAL_UINT32(test_filter.bucket_mask, f_new.bucket_mask);
    TEST_ASSERT_EQUAL_UINT32(test_filter.bucket_cnt, f_new.bucket_cnt);

    deseralize_filter_body(b, &f_new);

    TEST_ASSERT_NOT_NULL(f_new.data);
    TEST_ASSERT_EQUAL_MEMORY(test_filter.data, f_new.data, data_size);
    
    free_buffer(b);
    stop_test_resources();
}

double get_time_sec() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

#define CPU_GHZ 3.6  

void test_performance(void) {
    #define NUM_KEYS 1000000
    #define NUM_QUERIES 5000000
    #define BITS_PER_KEY 10.0

    f_str* keys = (f_str*)malloc(NUM_KEYS * sizeof(f_str));
    TEST_ASSERT_NOT_NULL(keys);

    char key_buffer[32];
    for (int i = 0; i < NUM_KEYS; ++i) {
        int len = sprintf(key_buffer, "performance-key-%d", i);
        keys[i].entry = strdup(key_buffer);
        keys[i].len = len;
        TEST_ASSERT_NOT_NULL(keys[i].entry);
    }

    printf("\n--- Performance Test ---\n");
    printf("[Speedy Bloom Filter]\n");
    printf("Filter Config: %d keys, %.1f bits per key, %d queries.\n", NUM_KEYS, BITS_PER_KEY, NUM_QUERIES);

    bb_filter perf_filter;
    bb_filter_init_capacity(&perf_filter, NUM_KEYS, BITS_PER_KEY);
    TEST_ASSERT_NOT_NULL(perf_filter.data);

    printf("Phase 1: Adding %d keys...\n", NUM_KEYS);
    double start_time = get_time_sec();
    for (int i = 0; i < NUM_KEYS; ++i) {
        bb_filter_add(&perf_filter, keys[i]);
    }
    double end_time = get_time_sec();

    double add_time = end_time - start_time;
    double ns_per_op = (add_time / NUM_KEYS) * 1e9;
    double cycles_per_op = ns_per_op * CPU_GHZ;
    printf("  -> Done in %.4f seconds.\n", add_time);
    printf("  -> Approx. %.2f million adds/sec\n", (double)NUM_KEYS / add_time / 1e6);
    printf("  -> %.2f ns/op (%.2f cycles/op @ %.2f GHz)\n",
           ns_per_op, cycles_per_op, CPU_GHZ);

    printf("Phase 2: Checking for %d existing keys...\n", NUM_QUERIES);
    volatile bool result;
    start_time = get_time_sec();
    for (int i = 0; i < NUM_QUERIES; ++i) {
        result = bb_filter_may_contain(&perf_filter, keys[i % NUM_KEYS]);
    }
    end_time = get_time_sec();
    
    double query_time = end_time - start_time;
    ns_per_op = (query_time / NUM_QUERIES) * 1e9;
    cycles_per_op = ns_per_op * CPU_GHZ;
    printf("  -> Done in %.4f seconds.\n", query_time);
    printf("  -> Approx. %.2f million queries/sec\n", (double)NUM_QUERIES / query_time / 1e6);
    printf("  -> %.2f ns/op (%.2f cycles/op @ %.2f GHz)\n",
           ns_per_op, cycles_per_op, CPU_GHZ);

    for (int i = 0; i < NUM_KEYS; ++i) {
        free(keys[i].entry);
    }
    free(keys);
    free(perf_filter.data);
}
static inline uint64_t xorshift64(uint64_t *s){
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    return x;
}

static inline f_str make_key64(const uint64_t *p){
    f_str k;
    k.entry = (void*)p;
    k.len   = 8;
    return k;
}
void test_bloom_fpr_and_correctness(size_t n_keys, size_t n_probes, double target_bpk, double tol_factor){
    bb_filter f = {0};
    int rc = bb_filter_init_capacity(&f, n_keys, target_bpk);
    assert(rc == 0);

    uint64_t *ins_keys = (uint64_t*)malloc(n_keys * sizeof(uint64_t));
    uint64_t *probe_keys = (uint64_t*)malloc(n_probes * sizeof(uint64_t));
    assert(ins_keys && probe_keys);
    uint64_t seed = (uintptr_t)probe_keys;
    for (size_t i = 0; i < n_keys; i++) ins_keys[i] = xorshift64(&seed);
    for (size_t i = 0; i < n_probes; i++) probe_keys[i] = xorshift64(&seed);

    for (size_t i = 0; i < n_keys; i++){
        f_str k = make_key64(&ins_keys[i]);
        bb_filter_add(&f, k);
    }

    for (size_t i = 0; i < n_keys; i++){
        f_str k = make_key64(&ins_keys[i]);
        assert(bb_filter_may_contain(&f, k) && "CRITICAL: False negative detected!");
    }

    size_t fp_count = 0;
    for (size_t i = 0; i < n_probes; i++){
        f_str k = make_key64(&probe_keys[i]);
        if (bb_filter_may_contain(&f, k)) {
            fp_count++;
        }
    }
    double observed_fpr = (double)fp_count / (double)n_probes;

    double k = (double)BB_K;
    double actual_total_bits = (double)f.bucket_cnt * BB_BUCKET_BITS;
    double actual_bpk = target_bpk;

    double B = (double)BB_BUCKET_BITS;
    double k_effective = B * (1.0 - pow(1.0 - 1.0/B, k));
    double expected_fpr_adj = pow(1.0 - exp(-k_effective / actual_bpk), k_effective);

    printf("FPR observed=%.6f expected(adj)=%.6f (BB_K=%u, target_bpk=%.2f, actual_bpk=%.2f)\n",
           observed_fpr, expected_fpr_adj, (unsigned)BB_K, target_bpk, actual_bpk);


    free(ins_keys);
    free(probe_keys);
    free(f.data);
}
void run_fpr_test(void){
    test_bloom_fpr_and_correctness(1000000, 1000000, 10, 1.5);
}

