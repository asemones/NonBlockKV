#include "../ds/frontier.h"
#include "unity/src/unity.h"
#include <stdlib.h>
#include <string.h>
#pragma once

void test_enqueue(void) {
    frontier *front = Frontier(sizeof(kv), true,  &compare_kv_v);
    for (int i = 1 ; i < 4; i++){
        int *p = wrapper_alloc((sizeof(int)), NULL,NULL);
        int *q = wrapper_alloc((sizeof(int)), NULL,NULL);
        *q = i*20;
        *p = i*10;
        kv * temp = dynamic_kv(p, q);
        enqueue(front,temp);
    }

    kv *peekedElement = peek(front);

    TEST_ASSERT_EQUAL_INT(10, *(int *)peekedElement->key);

    free_front(front);
}

void test_dequeue(void) {
  
  frontier *front = Frontier(sizeof(kv),true, &compare_kv_v);
  
  for (int i = 1 ; i < 4; i++){
        int *p = wrapper_alloc((sizeof(int)), NULL,NULL);
        int *q = wrapper_alloc((sizeof(int)), NULL,NULL);
        *q = i*20;
        *p = i*10;
        kv * temp = dynamic_kv(p, q);
        enqueue(front,temp);
    }

    kv dequeuedElement;
    dequeue(front, &dequeuedElement);
    TEST_ASSERT_EQUAL_INT(10, *(int *)dequeuedElement.key); 


    kv *peekedElement = peek(front);
    TEST_ASSERT_EQUAL_INT(20, *(int *)peekedElement->key); 



    free_front(front);
}

void test_peek(void) {
    frontier *front = Frontier(sizeof(kv),true,&compare_kv_v);
    for (int i = 1 ; i < 4; i++){
        int *p = wrapper_alloc((sizeof(int)), NULL,NULL);
        int *q = wrapper_alloc((sizeof(int)), NULL,NULL);
        *q = i*20;
        *p = i*10;
        kv * temp = dynamic_kv(p, q);
        enqueue(front,temp);
        free(temp);
    }
    

    kv *peekedElement = peek(front);
    TEST_ASSERT_EQUAL_INT(10, *(int *)peekedElement->key); 

    free_front(front);
}

void test_freeFront(void) {
    frontier *front = Frontier(sizeof(kv),false,  &compare_kv_v);
    int one =3, two =4;
    kv element1 = KV(&one, &two);

    enqueue(front, &element1);

    free_front(front);
}