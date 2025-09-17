#include "db.h"
#include "lsm.h" // Needed for write_record
#include "../../util/multitask.h" // Needed for future_t and cascade_return_int
option GLOB_OPTS;
#define MINI_STACK 256
future_t init_db(void * arg){
    db_shard * a = arg;
    storage_engine * lsm = create_engine(GLOB_OPTS.META_F_N, GLOB_OPTS.BLOOM_F_N);
    compact_manager * manager= init_cm(lsm->meta, &lsm->cach);
    lsm->cm_ref = &manager->check_meta_cond;
    a->lsm = lsm;
    a->manager=  manager;
    cascade_return_ptr(NULL);
}

db_shard db_shard_create(){
    db_shard shard;
    set_debug_defaults(&GLOB_OPTS);
    io_config  io;
    const double overflow_multplier = 1.2;
    /*come back to this*/
    io.big_buf_s = 0;
    io.max_concurrent_io = 12000;
    io.huge_buf_s  = GLOB_OPTS.MEM_TABLE_SIZE * overflow_multplier;
    io.small_buff_s = GLOB_OPTS.BLOCK_INDEX_SIZE;
    io.coroutine_stack_size = MINI_STACK;
    io.num_big_buf = GLOB_OPTS.NUM_COMPACTOR_UNITS;
    io.num_huge_buf = GLOB_OPTS.num_memtable;
    io.max_tasks = 100000;

    shard.rt =cascade_spawn_runtime_config(NULL, NULL, io);
    future_t test;
    _Atomic uint64_t counter = 0;
    cascade_submit_external(shard.rt, &test, &counter ,init_db, &shard);
    poll_rpc_external(shard.rt, &counter);
    return shard;
}
full_db * db(int num_shards){
    full_db * db = malloc(sizeof(*db));

    db->num_shard = num_shards;
    db->shard = malloc(sizeof(db_shard) * num_shards);
    for (int i = 0; i < db->num_shard; i++){
        db->shard[i] = db_shard_create();
    }
    return db;
}
future_t db_task_write_record(void *arg) {
    db_write_args *write_args = (db_write_args *)arg;
    
    // Call the original write_record function using the lsm from the shard
    int result = write_record(write_args->shard->lsm, write_args->key, write_args->value);

    cascade_return_int(result);
}

future_t db_task_read_record(void * arg){
    db_read_args * args = arg;
    char * result = read_record(args->shard->lsm, args->key);
    cascade_return_ptr(result);
}
future_t db_task_read_slice(void * arg){
    db_slice_read_args * args = arg;
    *(args->out) = read_and_pin(args->shard->lsm, args->key);
    cascade_return_ptr(args->out);
}
future_t kill_db(void * arg){
    db_shard * shard = arg;
    shard->manager->exit =  true;
    storage_engine * lsm = shard->lsm;
    free_engine(lsm, GLOB_OPTS.META_F_N, GLOB_OPTS.BLOOM_F_N);
    free_cm(shard->manager);
    cascade_return_none();
}
void db_end(db_shard * shard) {
    _Atomic uint64_t counter = 0;
    cascade_submit_external(shard->rt, NULL, &counter, kill_db ,shard);
    poll_rpc_external(shard->rt, &counter);
    end_runtime(shard->rt);
}

