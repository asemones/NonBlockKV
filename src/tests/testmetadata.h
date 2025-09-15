#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "unity/src/unity.h"
#include "../db/backend/dal/metadata.h"
#pragma once
void test_create_metadata_fresh(void) {
    meta_data *meta = load_meta_data("meta.bin", "bloom.bin");
    TEST_ASSERT_NOT_NULL(meta);
    destroy_meta_data ("meta.bin", "bloom.bin", meta);
    remove("meta.bin"); 
    remove("bloom.bin");
}
void test_use_and_load(void){
    meta_data *meta = load_meta_data("meta.bin", "bloom.bin");
 

    for(size_t i = 0; i < 100; i++){
        sst_f_inf * sst = (sst_f_inf*)wrapper_alloc((sizeof(sst_f_inf)), NULL,NULL);
        sst->file_name = malloc(100);
        sst->max = f_str_alloc(malloc(100));
        sst->min = f_str_alloc(malloc(100));
        gen_sst_fname(i, 0, sst->file_name);
        sst->length = 0;
        sst->block_start = 0;
        sst->block_indexs = NULL;
        sst->filter = NULL;
        
        insert(meta->sst_files[0], sst);

        free(sst);
        meta->num_sst_file++;
    }
    meta->file_ptr = 1000;
    meta->db_length = 1000;
    
    destroy_meta_data ("meta.bin", "bloom.bin", meta);

    meta = load_meta_data("meta.bin", "bloom.bin");
    TEST_ASSERT_NOT_NULL(meta);
    TEST_ASSERT_EQUAL_INT(100, meta->num_sst_file);
    TEST_ASSERT_EQUAL_INT(1000, meta->file_ptr);
    TEST_ASSERT_EQUAL_INT(1000, meta->db_length);
   
    remove("meta.bin");
    remove("bloom.bin");
}
