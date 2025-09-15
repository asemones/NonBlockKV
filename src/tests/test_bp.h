#include "../ds/buffer_pool_stratgies.h"
#include "unity/src/unity.h"
#include "../ds/byte_buffer.h"
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "../ds/structure_pool.h"
size_tier_config test_config_simple(){
    size_tier_config config;
    config.start = 256;
    config.multi = 2;
    config.stop  = 1024 * 1024;
    config.min_exp =2;
    return config;
}
hotcache_config hc_test_cf(uint64_t total_memsize){
    hotcache_config config;
    config.num = 3;
    double taken = 0.6f;
    double per= taken/config.num;
    config.size_tiers = malloc(sizeof(uint64_t) * config.num);
    config.total_per = malloc(sizeof(uint64_t) * config.num);
    uint64_t start = 4096;
    for(int i = 0; i < config.num; i++){
        config.size_tiers[i]= log2(start);
        config.total_per[i] = per * total_memsize;
    }
    return config;
}
void core_test(uint8_t type){
    uint64_t memsize = 1024 * 1024 * 256;
    size_tier_config config = test_config_simple();
    config.hc_config.num = 0;
    buffer_pool pool = make_b_p(type,memsize, config);
    byte_buffer * b=  get_buffer(&pool, 256 * 1024);
    write_buffer(b, "hello", strlen("hello"));
    TEST_ASSERT_NOT_NULL(b);
    return_buffer_strat(&pool, b);
    b = get_buffer(&pool, memsize);
    TEST_ASSERT_NOT_NULL_MESSAGE(b, "attempted_max_size");
    byte_buffer * b2 = get_buffer(&pool, 1);
    TEST_ASSERT_NULL_MESSAGE(b2, "size of 1 from empty bp");
    return_buffer_strat(&pool,b);
    end_b_p(&pool);
}
static void warmup_pool(buffer_pool* pool) {
    const int warmup_ops = 5000;
    const int warmup_slots = 100;
    const size_t max_alloc_size = 128 * 1024;
    const size_t PAGE_SIZE = 4096;

    byte_buffer** buffers = malloc(sizeof(byte_buffer*) * warmup_slots);
    if (!buffers) {
        return; 
    }

    for (int i = 0; i < warmup_slots; i++) {
        size_t alloc_size = ((rand() % (max_alloc_size / PAGE_SIZE)) + 1) * PAGE_SIZE;
        buffers[i] = get_buffer(pool, alloc_size);
        if (buffers[i] != NULL) {
            *((volatile char*)buffers[i]->buffy) = 1;
        }
    }

    for (int i = 0; i < warmup_ops; i++) {
        int index = rand() % warmup_slots;

        if (buffers[index] != NULL) {
            return_buffer_strat(pool, buffers[index]);
        }

        size_t new_alloc_size = ((rand() % (max_alloc_size / PAGE_SIZE)) + 1) * PAGE_SIZE;
        buffers[index] = get_buffer(pool, new_alloc_size);
        if (buffers[index] != NULL) {
            *((volatile char*)buffers[index]->buffy) = 1;
        }
    }

    for (int i = 0; i < warmup_slots; i++) {
        if (buffers[i] != NULL) {
            return_buffer_strat(pool, buffers[i]);
        }
    }

    free(buffers);
}

static void run_churn_test(const char* test_name, buffer_pool* pool, int num_operations, int active_slots, size_t* alloc_sizes, int num_alloc_sizes) {
    printf("  Running test: %s\n", test_name);

    byte_buffer** buffers = malloc(sizeof(byte_buffer*) * active_slots);
    if (!buffers) {
        TEST_FAIL_MESSAGE("Failed to allocate test harness memory");
        return;
    }
    
    printf("    Warming up... ");
    fflush(stdout);
    warmup_pool(pool);
    printf("Done.\n");

    int filled_slots = 0;
    for (int i = 0; i < active_slots; i++) {
        size_t size = alloc_sizes[rand() % num_alloc_sizes];
        buffers[i] = get_buffer(pool, size);
        if (buffers[i] == NULL) {
            printf("    Pool filled to capacity. Starting churn with %d of %d requested active slots.\n", filled_slots, active_slots);
            break;
        }
        *((volatile char*)buffers[i]->buffy) = 1;
        filled_slots++;
    }

    if (filled_slots == 0) {
        free(buffers);
        TEST_FAIL_MESSAGE("Initial allocation failed completely. Could not allocate any buffers.");
        return;
    }

    clock_t start = clock();

    for (int i = 0; i < num_operations; i++) {
        int index = rand() % filled_slots;
        
        return_buffer_strat(pool, buffers[index]);

        size_t new_alloc_size = alloc_sizes[rand() % num_alloc_sizes];
        buffers[index] = get_buffer(pool, new_alloc_size);
        
        if (buffers[index] == NULL || buffers[index]->buffy == NULL) {
            printf("    Re-allocation failed during churn test at operation %d.\n", i);
            num_operations = i;
            break; 
        }
        *((volatile char*)buffers[index]->buffy) = 1;
    }

    clock_t end = clock();

    for (int i = 0; i < filled_slots; i++) {
        if(buffers[i] != NULL) {
            return_buffer_strat(pool, buffers[i]);
        }
    }
    free(buffers);

    if (num_operations == 0) {
        printf("    No operations completed.\n");
        return;
    }

    double time_spent = (double)(end - start) / CLOCKS_PER_SEC;
    double ops_per_sec = time_spent > 0 ? num_operations / time_spent : 0;
    double ns_per_op = time_spent > 0 ? (time_spent * 1000000000.0) / num_operations : 0;
    
    printf("    Operations (Get/Return pairs): %d\n", num_operations);
    printf("    Total Time: %.4f seconds\n", time_spent);
    printf("    Operations Per Second: %.2f\n", ops_per_sec);
    printf("    Nanoseconds Per Op: %.2f ns\n", ns_per_op / 2);
}

void performance_test(uint8_t type){
    const uint64_t memsize = 1024 * 1024 * 1024;
    const int num_operations = 4000000;
    const int active_slots = 7000;
    const size_t PAGE_SIZE = 4096;
    
    char* type_name = "Unknown";
    if (type == VM_MAPPED) type_name = "VM_MAPPED";
    if (type == PINNED_BUDDY) type_name = "PINNED_BUDDY";
    printf("\n--- Performance Suite: %s ---\n", type_name);
    
    srand(time(NULL));

    {
        const int num_rand_sizes = 1000;
        size_t* rand_sizes = malloc(sizeof(size_t) * num_rand_sizes);
        const int max_pages = (128 * 1024) / PAGE_SIZE;

        for(int i = 0; i < num_rand_sizes; i++) {
            int num_pages = (rand() % max_pages) + 1;
            rand_sizes[i] = num_pages * PAGE_SIZE;
        }

        size_tier_config config = test_config_simple();
        config.hc_config.num = 0;
        buffer_pool pool = make_b_p(type, memsize, config);

        run_churn_test("Random 4KB-Multiple Churn (up to 128KB)", &pool, num_operations, active_slots, rand_sizes, num_rand_sizes);

        end_b_p(&pool);
        free(rand_sizes);
    }
    
    if (type == PINNED_BUDDY) {
        size_tier_config config = test_config_simple();
        config.hc_config = hc_test_cf(memsize);
        buffer_pool pool = make_b_p(type, memsize, config);
        
        size_t* hot_sizes = malloc(sizeof(size_t) * config.hc_config.num);
        for (int i = 0; i < config.hc_config.num; i++) {
            hot_sizes[i] = 1ULL << config.hc_config.size_tiers[i];
        }

        run_churn_test("Hot Cache Hit Churn", &pool, num_operations, active_slots, hot_sizes, config.hc_config.num);
        
        end_b_p(&pool);
        free(hot_sizes);
        free(config.hc_config.size_tiers);
        free(config.hc_config.total_per);
    } else if (type == VM_MAPPED) {
        size_tier_config config = test_config_simple();
        config.hc_config.num = 0;
        buffer_pool pool = make_b_p(type, memsize, config);
        
        size_t stable_size = PAGE_SIZE;

        run_churn_test("Single Slab Churn (4KB)", &pool, num_operations, active_slots, &stable_size, 1);
        
        end_b_p(&pool);
    }

    printf("--- End Performance Suite ---\n");
}

static uint64_t calculate_total_allocated(buffer_pool* pool) {
    return pool->allocated;
}

static void release_random_buffers(buffer_pool* pool, struct_pool * allocated, uint64_t mem) {
    uint64_t released = 0;
    while (released <  mem) {
        byte_buffer * end = request_struct(allocated);
        return_buffer_strat(pool,end);
        released += end->max_bytes;
    }
}

static void run_load_test(const char* test_name, buffer_pool* pool, int num_operations, int active_slots, size_t* alloc_sizes, int num_alloc_sizes) {
    printf("  Running test: %s\n", test_name);
    struct_pool * allocated=  create_pool(num_operations);

    uint64_t total_load_memory = 0;
    int load_point_count = 0;
    const double release_fraction = 0.05;

    clock_t start = clock();
    for (int i = 0; i < num_operations; i++) {

        size_t new_alloc_size = alloc_sizes[rand() % num_alloc_sizes];
     
        byte_buffer * temp  = get_buffer(pool, new_alloc_size);
        if (temp== NULL) {
            load_point_count++;
            total_load_memory += calculate_total_allocated(pool);
    
            uint64_t mem_to_release = release_fraction * pool->max;
            release_random_buffers(pool, allocated, mem_to_release);
            
            temp = get_buffer(pool, new_alloc_size);

            if (temp== NULL) {
                printf("    Re-allocation failed after release at operation %d. Test stopped.\n", i);
                num_operations = i;
                break;
            }
        }
        insert_struct(allocated, temp);
        
        *((volatile char*)temp->buffy) = 1;
    }

    clock_t end = clock();

    if (num_operations == 0) {
        printf("    No operations completed.\n");
        return;
    }

    double time_spent = (double)(end - start) / CLOCKS_PER_SEC;
    double ops_per_sec = time_spent > 0 ? num_operations / time_spent : 0;
    double ns_per_op = time_spent > 0 ? (time_spent * 1000000000.0) / num_operations : 0;
    double avg_load_mb = 0;
    if (load_point_count > 0) {
        avg_load_mb = ((double)total_load_memory / load_point_count) / (1024.0 * 1024.0);
    }
    
    printf("    Operations (Get/Return pairs): %d\n", num_operations);
    printf("    Total Time: %.4f seconds\n", time_spent);
    printf("    Operations Per Second: %.2f\n", ops_per_sec);
    printf("    Nanoseconds Per Op: %.2f ns\n", ns_per_op / 2);
    printf("    Load points (alloc failures): %d\n", load_point_count);
    if(load_point_count > 0){
        printf("    Average Memory at Load: %.2f MB\n", avg_load_mb);
    }
}

void load_test(uint8_t type){
    const uint64_t memsize = 1024 * 1024 * 1024;
    const int num_operations = 2000000;
    const int active_slots = 20000;
    const size_t PAGE_SIZE = 4096;
    const size_t MAX_ALLOC_SIZE = 1 * 1024 * 1024;
    
    char* type_name = "Unknown";
    if (type == VM_MAPPED) type_name = "VM_MAPPED";
    if (type == PINNED_BUDDY) type_name = "PINNED_BUDDY";
    printf("\n--- Load Suite: %s ---\n", type_name);
    
    srand(time(NULL));

    const int num_rand_sizes = 1000;
    size_t* rand_sizes = malloc(sizeof(size_t) * num_rand_sizes);
    const int max_pages = MAX_ALLOC_SIZE / PAGE_SIZE;

    for(int i = 0; i < num_rand_sizes; i++) {
        int num_pages = (rand() % max_pages) + 1;
        rand_sizes[i] = num_pages * PAGE_SIZE;
    }

    size_tier_config config = test_config_simple();
    config.hc_config.num = 0;
    buffer_pool pool = make_b_p(type, memsize, config);

    run_load_test("Sustained High-Load Churn (up to 5MB)", &pool, num_operations, active_slots, rand_sizes, num_rand_sizes);

    //end_b_p(&pool);
    free(rand_sizes);
    
    printf("--- End Load Suite ---\n");
}

void test_vm(){
    core_test(VM_MAPPED);
    //performance_test(VM_MAPPED);
    //load_test(VM_MAPPED);
}
void test_buddy(){
    core_test(PINNED_BUDDY);
    performance_test(PINNED_BUDDY);
    load_test(PINNED_BUDDY);
}