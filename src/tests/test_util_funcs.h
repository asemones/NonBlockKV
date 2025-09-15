 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../db/backend/db.h"
#include "../db/backend/option.h"
#include "../util/multitask.h"
#include <unistd.h>
#include "../db/backend/compactor.h"
#include "../db/backend/lsm.h"
#include "../ds/byte_buffer.h"
#include "unity/src/unity.h"
#include <stdatomic.h> // Include for atomics
#include "../ds/arena.h"
#include "../util/alloc_util.h"
#include "../db/backend/key-value.h"
#include <unistd.h>
#include <time.h>
#pragma once
#define PREFIX_LENGTH 5


void create_a_babybase(void) {
    db_shard shard = db_shard_create();
    const int size = 6000;
    f_str *k =  calloc(sizeof(f_str) * size * 2, 1); // Allocate buffer for both loops
    f_str *v= calloc(sizeof(f_str) * size * 2, 1); // Allocate buffer for both loops
    db_write_args * write_args = calloc(sizeof(*write_args) * size * 2, 1); // Allocate buffer for args
    _Atomic uint64_t wait_counter = 0; // Change to atomic
    for (int i = 0; i < size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        k[i].entry = key;
        v[i].entry=  value;

        if (i % 3 == 0) {
            sprintf(key, "common%d", i / 3); 
            sprintf(value, "first_value%d", i / 3);
        } else {
            sprintf(key, "hello%d", i);
            sprintf(value, "world%d", i);
        }
        k[i].len = strlen(key);
        v[i].len = strlen(value);

        // Use pre-allocated args buffer
        write_args[i].shard = &shard;
        write_args[i].key = k[i];
        write_args[i].value = v[i];
        // Submit task using wait counter
        cascade_submit_external(shard.rt, NULL, &wait_counter, &db_task_write_record, &write_args[i]);
    }
    // Removed sleep(1);
    // Removed individual free loop - will free at the end
    for (int i = 0; i < size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        // Use second half of the allocated buffers
        k[i + size].entry = key;
        v[i + size].entry=  value;
        if (i % 3 == 0) {
            sprintf(key, "common%d", i / 3); 
            sprintf(value, "second_value%d", i / 3);  
        } else {
            sprintf(key, "key%d", i);
            sprintf(value, "value%d", i);
        }

        k[i + size].len = strlen(key);
        v[i + size].len = strlen(value);
        // Use pre-allocated args buffer (second half)
        write_args[i + size].shard = &shard;
        write_args[i + size].key = k[i + size];
        write_args[i + size].value = v[i + size];
        // Submit task using wait counter
        cascade_submit_external(shard.rt, NULL, &wait_counter, &db_task_write_record, &write_args[i + size]);
    }
    // Removed sleep(1);
    // Wait for tasks to likely complete and poll
    usleep(11000); // Adjust timing if needed
    poll_rpc_external(shard.rt, &wait_counter);

    // Free individual key/value entries
    for (int i = 0; i < size * 2; i++) {
         // Check if entry was actually allocated in the loops
         if (k[i].entry) free(k[i].entry);
         if (v[i].entry) free(v[i].entry);
    }
    // Free the main buffers
    free(k);
    free(v);
    free(write_args);

    // Removed sleep(1)
    db_end(&shard); // Keep db_end for local shard
}
void create_a_bigbase(db_shard * shard) {
    const int size = 60000;
    f_str *k =  calloc(sizeof(f_str) * size * 2, 1);
    f_str *v= calloc(sizeof(f_str) * size * 2, 1);
    db_write_args * write_args = calloc(sizeof(*write_args) * size * 2, 1);
    _Atomic uint64_t wait_counter = 0; // Change to atomic
    for (int i = 0; i < size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        k[i].entry = key;
        v[i].entry=  value;

        if (i % 3 == 0) {
            sprintf(key, "common%d", i / 3); 
            sprintf(value, "first_value%d", i / 3);
        } else {
            sprintf(key, "hello%d", i);
            sprintf(value, "world%d", i);
        }
        k[i].len = strlen(key);
        v[i].len = strlen(value);

        write_args[i].shard = shard;
        write_args[i].key = k[i];
        write_args[i].value = v[i];
        cascade_submit_external(shard->rt, NULL, &wait_counter, &db_task_write_record, &write_args[i]);
    }
    for (int i = 0; i < size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        k[i].entry = key;
        v[i].entry=  value;
        if (i % 3 == 0) {
            sprintf(key, "common%d", i / 3); 
            sprintf(value, "second_value%d", i / 3);  
        } else {
            sprintf(key, "key%d", i);
            sprintf(value, "value%d", i);
        }

        k[i].len = strlen(key);
        v[i].len = strlen(value);
        write_args[i + size].shard = shard;
        write_args[i+size].key = k[i];
        write_args[i + size].value = v[i];
        cascade_submit_external(shard->rt, NULL, &wait_counter, &db_task_write_record, &write_args[i+ size]);
    }
    poll_rpc_external(shard->rt, &wait_counter);
    //future_t futures[size * 2];
    //poll_rpc_external(shard->rt, fds,futures,  size * 2);
}
db_shard create_messy_db(){
    db_shard shard = db_shard_create();
    const int baby_size = 6000;
    const int messy_size = 3000; // 9000 - 6000
    const int total_writes = baby_size * 2 + messy_size;

    // Allocate buffers for all writes upfront
    f_str *k = calloc(sizeof(f_str) * total_writes, 1);
    f_str *v = calloc(sizeof(f_str) * total_writes, 1);
    db_write_args *write_args = calloc(sizeof(*write_args) * total_writes, 1);
    _Atomic uint64_t wait_counter = 0; // Change to atomic
    for (int i = 0; i < baby_size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        k[i].entry = key; // Use combined buffer
        v[i].entry=  value; // Use combined buffer
        if (i % 3 == 0) { sprintf(key, "common%d", i / 3); sprintf(value, "first_value%d", i / 3); }
        else { sprintf(key, "hello%d", i); sprintf(value, "world%d", i); }
        k[i].len = strlen(key);
        v[i].len = strlen(value);
        // Use pre-allocated args buffer
        write_args[i].shard = &shard;
        write_args[i].key = k[i];
        write_args[i].value = v[i];
        // Submit task using wait counter
        cascade_submit_external(shard.rt, NULL, &wait_counter, &db_task_write_record, &write_args[i]);
    }
    sleep(1);
     // Removed individual free loop
    for (int i = 0; i < baby_size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        // Use second part of the combined buffer (offset by baby_size)
        k[i + baby_size].entry = key;
        v[i + baby_size].entry=  value;
        if (i % 3 == 0) { sprintf(key, "common%d", i / 3); sprintf(value, "second_value%d", i / 3); }
        else { sprintf(key, "key%d", i); sprintf(value, "value%d", i); }
        k[i + baby_size].len = strlen(key);
        v[i + baby_size].len = strlen(value);
        // Use pre-allocated args buffer (offset by baby_size)
        write_args[i + baby_size].shard = &shard;
        write_args[i + baby_size].key = k[i + baby_size];
        write_args[i + baby_size].value = v[i + baby_size];
        // Submit task using wait counter
        cascade_submit_external(shard.rt, NULL, &wait_counter, &db_task_write_record, &write_args[i + baby_size]);
    }
    sleep(1);
     // Removed individual free loop

    for (int i = 0; i < messy_size; i++) { // Loop from 0 to messy_size
        char *key = (char*)wrapper_alloc(60, NULL, NULL);
        char *value = (char*)wrapper_alloc(60, NULL, NULL);
        // Use third part of the combined buffer (offset by baby_size * 2)
        int current_index = i + baby_size * 2;
        k[current_index].entry = key;
        v[current_index].entry= value;
        int original_i = i + 6000; // Keep original logic for sprintf
        if (original_i % 3 == 0) {
            sprintf(key, "common%d", original_i / 3);
            sprintf(value, "third_value%d", original_i / 3);
        }
        else if (original_i % 2 == 0) {
            sprintf(key, "i want to break your database%d", original_i / 2);
            sprintf(value, "break break break%d", original_i / 2);
        }
        else {
            sprintf(key, "key%d", original_i);
            sprintf(value, "value%d", original_i);
        }
        k[current_index].len = strlen(key);
        v[current_index].len= strlen(value);
        // Use pre-allocated args buffer (offset by baby_size * 2)
        write_args[current_index].shard = &shard;
        write_args[current_index].key = k[current_index];
        write_args[current_index].value = v[current_index];
        // Submit task using wait counter
        cascade_submit_external(shard.rt, NULL, &wait_counter, &db_task_write_record, &write_args[current_index]);
        // Removed individual free(k.entry) and free(v.entry)
    }
    // Wait and poll
    usleep(11000); // Adjust timing if needed
    poll_rpc_external(shard.rt, &wait_counter);

    // Free individual key/value entries
    for (int i = 0; i < total_writes; i++) {
         if (k[i].entry) free(k[i].entry);
         if (v[i].entry) free(v[i].entry);
    }
    // Free the main buffers
    free(k);
    free(v);
    free(write_args);
    // Removed sleep(1)
    return shard;
}
void clean_test_files(void){
    remove("db.bin");
    remove("meta.bin");
    remove("bloom.bin");
    remove ("sst_0_0");
    remove ("sst_1_0");
    remove ("sst_2_0");
    remove ("sst_3_0");
    remove ("sst_4_0");
    remove ("sst_0_1");
    remove ("sst_1_1");
    remove ("sst_2_1");
    remove ("sst_3_1");
    remove ("sst_4_1");
    remove ("sst_4_7");
    remove("sst_5_7");
    remove ("in_prog_sst_3_0");
    remove ("in_prog_sst_4_0");
    remove ("in_prog_sst_5_0");
    remove ("in_prog_sst_6_0");
    remove ("WAL_0.bin");
    remove("WAL_1.bin");
    remove("WAL_M.bin");
}


void generate_random_prefix(char* buffer, size_t prefix_len) {
    const char *allowed = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (size_t i = 0; i < prefix_len; i++) {
        buffer[i] = allowed[rand() % 36];
    }
    buffer[prefix_len] = '\0';
}
void write_random_units(byte_buffer *b, const int iters, const int prefix_size) {
    for (int i = 0; i < iters; i++){
        char buf[256];  

        f_str key_unit;
        f_str value_unit;

        generate_random_prefix(buf, prefix_size);
        key_unit.entry = buf;
        key_unit.len = strlen(buf);
        write_disk_format(b, key_unit);

        memset(buf, 0, sizeof(buf));

        generate_random_prefix(buf, prefix_size);
        value_unit.entry = buf;
        value_unit.len = strlen(buf);
        write_disk_format(b, value_unit);
    }
}
void read_entire_sst(byte_buffer * b, byte_buffer * decompress_into, sst_f_inf * inf){
    FILE * file=  fopen(inf->file_name, "rb");
    int read=  fread(b->buffy, 1, inf->length,file);
    b->curr_bytes += read;
    regular_decompress(&inf->compr_info, b, decompress_into, read);
}
void add_random_records_a_z(db_shard *shard, const int size) {
    srand((unsigned int)time(NULL));

    // Allocate buffers for both loops upfront
    f_str *k = calloc(sizeof(f_str) * size * 2, 1);
    f_str *v = calloc(sizeof(f_str) * size * 2, 1);
    db_write_args *write_args = calloc(sizeof(*write_args) * size * 2, 1);
    _Atomic uint64_t wait_counter = 0; // Change to atomic

    for (int i = 0; i < size; i++) {
        // Use second half of buffers
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        k[i + size].entry = key;
        v[i + size].entry = value;

        
        char prefix[PREFIX_LENGTH + 1];
        generate_random_prefix(prefix, PREFIX_LENGTH);

    
        sprintf(key, "%s_key_%d", prefix, i);
        sprintf(value, "first_value%d", i);

        k[i + size].len = strlen(key);
        v[i + size].len = strlen(value);

        // Use pre-allocated args buffer
        write_args[i].shard = shard;
        write_args[i].key = k[i];
        write_args[i].value = v[i];
        // Submit task using wait counter
        cascade_submit_external(shard->rt, NULL, &wait_counter, &db_task_write_record, &write_args[i]);
    }

    for (int i = 0; i < size; i++) {
        char *key = (char*)wrapper_alloc(30, NULL, NULL);
        char *value = (char*)wrapper_alloc(30, NULL, NULL);
        k[i].entry = key;
        v[i].entry = value;

    
        char prefix[PREFIX_LENGTH + 1];
        generate_random_prefix(prefix, PREFIX_LENGTH);

        sprintf(key, "%s_key_%d", prefix, i);
        sprintf(value, "second_value%d", i);

        k[i].len = strlen(key);
        v[i].len = strlen(value);

        // Use pre-allocated args buffer (second half)
        write_args[i + size].shard = shard;
        write_args[i + size].key = k[i + size];
        write_args[i + size].value = v[i + size];
        // Submit task using wait counter
        cascade_submit_external(shard->rt, NULL, &wait_counter, &db_task_write_record, &write_args[i + size]);
    }
    // Wait and poll
    usleep(11000); // Adjust timing if needed
    poll_rpc_external(shard->rt, &wait_counter);

    // Free individual key/value entries
    for (int i = 0; i < size * 2; i++) {
         if (k[i].entry) free(k[i].entry);
         if (v[i].entry) free(v[i].entry);
    }
    // Free the main buffers
    free(k);
    free(v);
    free(write_args);
}
void remove_ssts(list ** sst2){
    for(int j = 0; j < LEVELS; j++){
        list * ssts= sst2[j];
        for(int i = 0; i <ssts->len; i++){
            sst_f_inf * sst= at(ssts, i);
            int res = remove(sst->file_name);
            if (res!=0){
                fprintf(stdout, "%s failed at %d level\n",sst->file_name, j);
            }
        }
    }
}
