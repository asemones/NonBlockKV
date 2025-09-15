#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../db/backend/compactor.h"
#include "../db/backend/lsm.h"
#include "../ds/byte_buffer.h"
#include "unity/src/unity.h"
#include "../ds/arena.h"
#include "../util/alloc_util.h"
#include "test_util_funcs.h"
#include"../db/backend/key-value.h"
#include "../db/backend/compat_schedule.h"
#pragma once


db_shard shard; // Use db_shard instead of separate engine/cm/thrd

void prep_test(){
    shard = db_shard_create();
    compactor_args * args = malloc(sizeof(*args));
    args->cm = shard.manager;
    args->rt = shard.rt;
    cascade_submit_external(shard.rt, NULL, NULL, run_compactor, args);
}
void end_test(){
    sleep(2);
    remove_ssts(shard.lsm->meta->sst_files);
    db_end(&shard);
    remove(GLOB_OPTS.META_F_N);
    remove(GLOB_OPTS.BLOOM_F_N);
    remove ("WAL_0.bin");
    remove("WAL_1.bin");
    remove("WAL_M.bin");
}
void fat_db_passive(){
    prep_test();
    create_a_bigbase(&shard); // Pass shard pointer
    end_test();
}
void random_values_short(){
    const int size_per_add =5000;
    prep_test();
    create_a_bigbase(&shard); // Pass shard pointer
    add_random_records_a_z(&shard,size_per_add); // Pass shard pointer
    end_test();
}
void random_values_long(){
    const int size_per_add =50000;
    prep_test();
    create_a_bigbase(&shard); // Pass shard pointer
    add_random_records_a_z(&shard,size_per_add); // Pass shard pointer
    end_test();
}
