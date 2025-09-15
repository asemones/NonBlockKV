#include "../util/multitask.h"
#include "../util/io.h"
#include "unity/src/unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <assert.h>
#include <errno.h>
#include "../util/aco.h"
#include <unistd.h>
#pragma once
future_t call_me2(void * arg){
    cascade_return_int(1);
}
future_t call_me3(void * arg){
    cascade_return_ptr(0);
}
future_t callme4(void * arg){
    task_t * task = aco_get_arg();
    fprintf(stdout, "mail on %s shard %d\n", (char*)arg, task->scheduler->cpu);
    cascade_return_ptr(arg);
}
future_t cascade_main(void * arg){
    future_t * arr = pad_allocate(sizeof (future_t) * 1);
    cascade_sub_intern_nowait(call_me2, NULL, arr);
    future_t result2 = cascade_sub_intern_wait(call_me3, NULL);
    TEST_ASSERT_EQUAL_INT(1, arr->return_code);
    assert(result2.value == NULL);
    cascade_return_int(0); 
}
void test_framework_base(){
    cascade_runtime_t* rt=  cascade_spawn_runtime_default(cascade_main, NULL);
    sleep(1);
    end_runtime(rt);
}
future_t cascade_main2(void * arg){
    sleep(1);
    cascade_framework_t * frame = arg;
    const char * main1 = strdup("mai1");
    uint32_t node = find_node_hash_internal(arg, main1, strlen(main1));
    uint64_t id = cascade_rpc_wait(frame->runtimes[node], callme4, (void*)main1);
    future_t fut = get_return_val(id);
    TEST_ASSERT_EQUAL_STRING(main1,fut.value);
    cascade_return_none();
}
future_t cascade_main3(void * arg){
    sleep(1);
    cascade_framework_t * frame = arg;
    const char * main3 = strdup("mai33");
    uint32_t node = find_node_hash_internal(arg, main3, strlen(main3));
    int64_t id = cascade_rpc_wait(frame->runtimes[node], callme4, (void*)main3);
    future_t fut = get_return_val(id);
    TEST_ASSERT_EQUAL_STRING(main3,fut.value);
    cascade_return_none();
}
future_t cascade_main4(void * arg){
    sleep(1);
    cascade_framework_t * frame = arg;
    const char * main4 = strdup("mai4");
    uint32_t node = find_node_hash_internal(arg, main4, strlen("mai4"));
    int64_t id = cascade_rpc_wait(frame->runtimes[node], callme4,("mai4"));
    future_t fut = get_return_val(id);
    TEST_ASSERT_EQUAL_STRING(main4,fut.value);
    cascade_return_none();
}
void test_framework_main(){
    cascade_framework_t * frame = create_framework();
    int num_links = 5;
    for (int i = 0; i < num_links; i++){
        add_link_to_framework(frame, cascade_spawn_runtime_default(cascade_main2, frame));
    }
    sleep(1);
    for (int i = 0; i < num_links; i++){
        end_runtime(frame->runtimes[i]);
    }

}
