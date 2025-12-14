#include "../db/backend/manifest.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "unity/src/unity.h"
#include "../util/io.h"

static manifest * m;
static byte_buffer * b;
cascade_runtime_t * rt;
static future_t test_entry(void * arg){
    m = init_manifest(b, 4096);
    cascade_return_none();
}
static future_t run_manifest_start(void * arg){
    TEST_ASSERT_NOT_NULL(m);
    TEST_ASSERT_NOT_NULL(m->manifest_buffer);
    TEST_ASSERT_EQUAL_INT64(0, m->snapshot_ptr);
    TEST_ASSERT_EQUAL_INT64(4096, m->flush_cadence);
    end_test();

}

static void start_test(){
    b = create_buffer(1024 * 1024);
    rt = cascade_spawn_runtime_default(test_entry,NULL);
}
static void end_test(){
    delete_manifest(m);
    free_buffer(b);
}
void test_manifest_start(){
    start_test();
    c_thread_call_sync(rt, run_manifest_start, NULL);
    end_runtime(rt);

}
