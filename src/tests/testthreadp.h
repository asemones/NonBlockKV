#include "./unity/src/unity.h"  
#include <string.h>
#include <stdlib.h>
#include "../ds/threadpool.h"
#pragma once
void mockWorkFunction(void *arg, void **result, thread* p) {
    int *input = (int *)arg;
    int *output = (int *)wrapper_alloc((sizeof(int)), NULL,NULL);
    *output = (*input) * 2;
    if (result!= NULL){
        *result = output;
    }

}


void test_threadPool_initialization(void) {
    thread_p *pool = thread_Pool(4, 0);
    TEST_ASSERT_NOT_NULL(pool);
    TEST_ASSERT_EQUAL_INT(4, pool->num_threads);
    TEST_ASSERT_FALSE(pool->killSig);
    destroy_pool(pool);
}


void test_addWork_to_threadPool(void) {
    thread_p *pool = thread_Pool(4,0);
    int* arg = wrapper_alloc((sizeof(int)), NULL,NULL);
    *arg = 5;
    int* priority = wrapper_alloc((sizeof(int)), NULL,NULL);
    *priority = 1;
    add_work(pool, mockWorkFunction, arg,NULL, priority);
    TEST_ASSERT_EQUAL_INT(1, pool->work_pq->queue->len);
    destroy_pool(pool);
}


void test_execute_work_in_threadPool(void) {
    thread_p *pool = thread_Pool(4,0);
    int* arg = wrapper_alloc((sizeof(int)), NULL,NULL);
    *arg = 5;
    int* priority = wrapper_alloc((sizeof(int)), NULL,NULL);
    *priority = 1;
    int ** retV = wrapper_alloc((sizeof(int*)), NULL,NULL);
    add_work(pool, mockWorkFunction, arg,(void**)retV, priority);
    TEST_ASSERT_EQUAL_INT(0, pool->work_pq->queue->len);
    TEST_ASSERT_EQUAL_INT(1, pool->out_pq->queue->len);
    kv resultItem;
    dequeue(pool->out_pq, &resultItem);
    work *resultWork = (work *)resultItem.value;
    int **result = (int **)resultWork->reV;

    TEST_ASSERT_EQUAL_INT(10, **result); 
    destroy_pool(pool);
    free(result);
    free(resultWork);
}

void test_destroy_threadPool(void) {
    thread_p *pool = thread_Pool(4,0);
    destroy_pool(pool);
}