#include "unity/src/unity.h"
#include "../ds/bloomfilter.h"
#include "../util/alloc_util.h"
#pragma once

void test_create_bit_array(void) {
    bit_array* ba  = create_bit_array(2);
    TEST_ASSERT_NOT_NULL(ba);
    TEST_ASSERT_EQUAL(2, ba->size);
    TEST_ASSERT_NOT_NULL(ba->array);
    free_bit(ba);
}

void test_set_and_get_bit(void) {
    bit_array* ba  = create_bit_array(2);
    set_bit(ba, 0);
    TEST_ASSERT_TRUE(get_bit(ba, 0));
    
    set_bit(ba, 127);
    TEST_ASSERT_TRUE(get_bit(ba, 127));
    
    TEST_ASSERT_FALSE(get_bit(ba, 64));
    free_bit(ba);
}

void test_clear_bit(void) {
    bit_array* ba  = create_bit_array(2);
    set_bit(ba, 50);
    TEST_ASSERT_TRUE(get_bit(ba, 50));
    
    clear_bit(ba, 50);
    TEST_ASSERT_FALSE(get_bit(ba, 50));
    free_bit(ba);
}

void test_multiple_operations(void) {
    bit_array* ba  = create_bit_array(2);
    for (int i = 0; i < 128; i += 2) {
        set_bit(ba, i);
    }
    
    for (int i = 0; i < 128; i++) {
        if (i % 2 == 0) {
            TEST_ASSERT_TRUE(get_bit(ba, i));
        } else {
            TEST_ASSERT_FALSE(get_bit(ba, i));
        }
    }
    free_bit(ba);
}

void test_bloom_creation(void) {
    bloom_filter *filter = bloom(3, 2,false, NULL); 
    TEST_ASSERT_NOT_NULL(filter);
    TEST_ASSERT_EQUAL(3, filter->num_hash);
    TEST_ASSERT_NOT_NULL(filter->ba);
    TEST_ASSERT_EQUAL(2, filter->ba->size);
    free_filter(filter);
}

void test_map_and_check_bit(void) {
    bloom_filter *filter = bloom(3, 2,false, NULL); 
    char *url1 = "http://example.com";
    char *url2 = "http://example.org";
    
    map_bit(url1, filter);
    TEST_ASSERT_TRUE(check_bit(url1, filter));
    TEST_ASSERT_FALSE(check_bit(url2, filter));
    free_filter(filter);
}

void test_multiple_urls(void) {
    bloom_filter *filter = bloom(3, 2,false, NULL); 
    char *urls[] = {
        "http://example.com",
        "http://example.org",
        "http://example.net",
        "http://example.edu"
    };
    int num_urls = sizeof(urls) / sizeof(urls[0]);
    
    for (int i = 0; i < num_urls; i++) {
        map_bit(urls[i], filter);
    }
    
    for (int i = 0; i < num_urls; i++) {
        TEST_ASSERT_TRUE(check_bit(urls[i], filter));
    }
    
    TEST_ASSERT_FALSE(check_bit("http://example.io", filter));
    free_filter(filter);
}

void test_null_and_empty_url(void) {
    bloom_filter *filter = bloom(3, 2,false,NULL); 
    map_bit(NULL, filter);
    map_bit("", filter);
    
    TEST_ASSERT_FALSE(check_bit(NULL, filter));
    TEST_ASSERT_FALSE(check_bit("", filter));
    free_filter(filter);
}
void test_dumpFilter(void){
     bloom_filter *filter = bloom(3, 20,false, NULL);
     char *urls[] = {
        "http://example.com",
        "http://example.org",
        "http://example.net",
        "http://example.edu"
    };
    int num_urls = sizeof(urls) / sizeof(urls[0]);
    
    for (int i = 0; i < num_urls; i++) {
        map_bit(urls[i], filter);
    }
    dump_filter(filter, "test_output2.txt");
    bloom_filter *nextTest =  bloom(3, 20,true,"test_output2.txt");

    TEST_ASSERT_EQUAL(20, nextTest->ba->size);
    TEST_ASSERT_EQUAL(3, nextTest->num_hash);
    for (int i = 0; i < num_urls; i++) {
        TEST_ASSERT_TRUE(check_bit(urls[i], nextTest));
    }
    free_filter( nextTest);

}
