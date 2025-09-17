#include "unity/src/unity.h"
#include "../db/backend/db.h"
#include "../util/multitask.h"
#include "../db/backend/lsm.h"
#include <string.h>
#include "../util/alloc_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>

void submit_and_wait(db_shard *shard, f_str key, f_str value) {
    future_t write_future;
    _Atomic uint64_t counter = 0;
    db_write_args args = {.shard = shard, .key = key, .value = value};
    cascade_submit_external(shard->rt, &write_future, &counter, db_task_write_record, &args);
    poll_rpc_external(shard->rt, &counter);
}

void test_db_create(void) {
    db_shard shard = db_shard_create();
    TEST_ASSERT_NOT_NULL(shard.lsm);
    TEST_ASSERT_NOT_NULL(shard.manager);
    TEST_ASSERT_NOT_NULL(shard.rt);
    db_end(&shard);


    remove("meta.bin");
    remove("bloom.bin");
    remove("db.bin");
    remove("WAL_0.bin");
    remove("WAL_M.bin");
}

void test_db_write_read_single(void) {
    db_shard shard = db_shard_create();
    TEST_ASSERT_NOT_NULL(shard.lsm);

    f_str key = {.mem = "testkey1", .len = strlen("testkey1") + 1};
    f_str value = {.mem = "testvalue1", .len = strlen("testvalue1") + 1};

    submit_and_wait(&shard, key, value);

    char *found = read_record(shard.lsm, key);
    TEST_ASSERT_NOT_NULL(found);
    TEST_ASSERT_EQUAL_STRING(value.entry, found);

    db_end(&shard);


    remove("meta.bin");
    remove("bloom.bin");
    remove("db.bin");
    remove("sst_0_0.bin");
    remove("WAL_0.bin");
    remove("WAL_M.bin");
}


void test_db_write_read_multiple_persistent(void) {
    const int size = 100;
    f_str keys[size];
    f_str values[size];
    char* found_values[size];


    db_shard shard1 = db_shard_create();
    TEST_ASSERT_NOT_NULL(shard1.lsm);

    _Atomic uint64_t counter = 0;
    future_t write_futures[size];
    db_write_args write_args[size];


    for (int i = 0; i < size; i++) {
        char *key_str = (char*)wrapper_alloc(16, NULL, NULL);
        char *value_str = (char*)wrapper_alloc(16, NULL, NULL);
        sprintf(key_str, "mkey%d", i);
        sprintf(value_str, "mvalue%d", i);
        keys[i].entry = key_str;
        keys[i].len = strlen(key_str) + 1;
        values[i].entry = value_str;
        values[i].len = strlen(value_str) + 1;

        write_args[i].shard = &shard1;
        write_args[i].key = keys[i];
        write_args[i].value = values[i];
        cascade_submit_external(shard1.rt, &write_futures[i], &counter, db_task_write_record, &write_args[i]);
    }

    poll_rpc_external(shard1.rt, &counter);


    for (int i = 0; i < size; i++) {
         char * found = read_record(shard1.lsm, keys[i]);
         TEST_ASSERT_NOT_NULL(found);
         TEST_ASSERT_EQUAL_STRING(values[i].entry, found);
         //free(found);
    }


    db_end(&shard1);


    db_shard shard2 = db_shard_create();
    TEST_ASSERT_NOT_NULL(shard2.lsm);
    counter = 0;
    future_t fut;
    for (int i = 0; i < size; i++) {
        db_read_args arg;
        arg.key = keys[i];
        arg.shard = &shard2;
        cascade_submit_external(shard1.rt, &fut, &counter, db_task_read_record, &arg);
        poll_rpc_external(shard2.rt, &counter);
        char * found = fut.value;
        TEST_ASSERT_NOT_NULL(found);
        TEST_ASSERT_EQUAL_STRING(values[i].entry, found);
        found_values[i] = found;
    }

    db_end(&shard2);


    remove("meta.bin");
    remove("bloom.bin");
    remove("db.bin");

    remove("sst_0_0.bin");
    remove("sst_1_0.bin");
    remove("sst_2_0.bin");
    remove("WAL_0.bin");
    remove("WAL_M.bin");

    for (int i = 0; i < size; i++) {
        free((void*)keys[i].entry);
        free((void*)values[i].entry);
        //zero copy, dont free kernel memory free(found_values[i]);
    }
}