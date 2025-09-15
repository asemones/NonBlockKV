#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "unity/src/unity.h"
#include "../ds/arena.h"
#include "../util/alloc_util.h"
#include "test_util_funcs.h"
#include "../db/backend/iter.h"
#include "../db/backend/lsm.h"
#pragma once


/* really just want no crashing*/
void test_init_iters(){
    clean_test_files();
    aseDB_iter * it=  create_aseDB_iter();
    // create_a_babybase(); // Don't call this, manage shard lifecycle here
    db_shard shard = db_shard_create();
    create_a_bigbase(&shard); // Populate the shard with data
    init_aseDB_iter(it, shard.lsm); // Pass the lsm engine from the shard
    remove_ssts(shard.lsm->meta->sst_files); // Get sst_files from shard's metadata
    db_end(&shard);
    clean_test_files();
    free_aseDB_iter(it);
}
void test_seek(){
    aseDB_iter * it=  create_aseDB_iter();
    // create_a_babybase();
    db_shard shard = db_shard_create();
    create_a_bigbase(&shard);
    init_aseDB_iter(it, shard.lsm);
    seek(it, make_fstr("k", strlen("k")));
    // clean_test_files(); // Cleanup happens at the end
    remove_ssts(shard.lsm->meta->sst_files);
    free_aseDB_iter(it);
    db_end(&shard);
    clean_test_files(); // Clean up files after db_end
}
void test_next(){
    aseDB_iter * it=  create_aseDB_iter();
    // create_a_babybase();
    db_shard shard = db_shard_create();
    create_a_bigbase(&shard);
    init_aseDB_iter(it, shard.lsm);
    seek(it,  make_fstr("k", strlen("k")));
    merge_data m = aseDB_iter_next(it);
    char * next= m.value.entry;
    TEST_ASSERT_EQUAL_STRING("value1",next);
    m = aseDB_iter_next(it);
    next = m.value.entry;
    TEST_ASSERT_EQUAL_STRING("value10",next);
    remove_ssts(shard.lsm->meta->sst_files);
    // clean_test_files();
    free_aseDB_iter(it);
    db_end(&shard);
    clean_test_files();
}
void test_run_scan(){
    aseDB_iter * it=  create_aseDB_iter();
    // create_a_babybase();
    db_shard shard = db_shard_create();
    create_a_bigbase(&shard);
    init_aseDB_iter(it, shard.lsm);
    seek(it, make_fstr("c", strlen("c")));
    for (merge_data next = aseDB_iter_next(it); ((char*)next.key.entry)[0] < 'k'; next = aseDB_iter_next(it)){
     
        if (next.value.entry == NULL){
            break;
        }
    }
    remove_ssts(shard.lsm->meta->sst_files);
    free_aseDB_iter(it);
    db_end(&shard);
    clean_test_files();
}
void test_full_table_scan_messy(){
    db_shard shard = create_messy_db(); // Returns a db_shard now
    aseDB_iter * it=  create_aseDB_iter();
    init_aseDB_iter(it, shard.lsm); // Pass the lsm engine from the shard
    seek(it, make_fstr("c", strlen("c")));
    for(merge_data next = aseDB_iter_next(it); ;next = aseDB_iter_next(it)){
        if (next.key.entry== EODB){
            break;
        }
    }
    remove_ssts(shard.lsm->meta->sst_files); // Get sst_files from shard's metadata
    free_aseDB_iter(it);
    db_end(&shard);
    clean_test_files();
}























