#include "./unity/src/unity.h" 
#include <stdlib.h>
#include <string.h>
#pragma once
#include "../ds/list.h"
#include "../util/alloc_util.h"

// Helper function to create a list of strings


// Helper function to free list elements (strings)

void test_create_list() {
    list *myList =  List(0, sizeof(char *), true);
    TEST_ASSERT_NOT_NULL(myList);
    TEST_ASSERT_EQUAL_INT(0, myList->len);
    TEST_ASSERT_EQUAL_INT(16, myList->cap);
    free_list(myList, NULL);
}
void test_list_expansions(){
    list *myList =  List(0, sizeof(int *), true);
    for (int i = 0; i < 10000; i++){
        int * temp = wrapper_alloc((sizeof(temp)), NULL,NULL);
        *temp = i;
        insert(myList,&temp);
    }
    TEST_ASSERT_NOT_NULL(myList);
    TEST_ASSERT_EQUAL_INT(10000, myList->len);
    TEST_ASSERT_EQUAL_INT(16384, myList->cap);
    for (int i = 0; i < 10000; i++){
         TEST_ASSERT_EQUAL_INT(i ,*(*((int**)at(myList, i))));
    }
    free_list(myList, NULL);
}

void test_insert_elements() {
    list *myList =  List(0, sizeof(char *),true);
    char *str1 = strdup("Hello");
    char *str2 = strdup("World");
    char *str3 = strdup("Test");
    insert(myList, &str1);
    insert(myList, &str2);
    insert(myList, &str3);
    TEST_ASSERT_EQUAL_INT(3, myList->len);
    char **element = (char **)at(myList, 0);
    TEST_ASSERT_EQUAL_STRING("Hello", *element);
    element = (char **)at(myList, 1);
    TEST_ASSERT_EQUAL_STRING("World", *element);
    element = (char **)at(myList, 2);
    TEST_ASSERT_EQUAL_STRING("Test", *element);
    free_list(myList, NULL);
}

void test_insert_at_specific_index() {
    list *myList =  List(0, sizeof(char *),false);
    char *str1 = strdup("Hello");
    char *str2 = strdup("World");
    char *str3 = strdup("Test");
    char *str4 = strdup("Insert");
    insert(myList, &str1);
    insert(myList, &str2);
    insert(myList, &str3);
    inset_at(myList, &str4, 2);
    TEST_ASSERT_EQUAL_INT(3, myList->len);
    char **element = (char **)at(myList, 2);
    TEST_ASSERT_EQUAL_STRING("Insert", *element);
    element = (char **)at(myList, 3);
    TEST_ASSERT_NULL(element);
    free_list(myList, NULL);
}

void test_remove_element_at_specific_index() {
    list *myList =  List(0, sizeof(char *),true);
    char *str1 = strdup("Hello");
    char *str2 = strdup("World");
    char *str3 = strdup("Test");
    insert(myList, &str1);
    insert(myList, &str2);
    insert(myList, &str3);
    remove_at(myList, 1);
    TEST_ASSERT_EQUAL_INT(2, myList->len);
    char **element = (char **)at(myList, 1);
    TEST_ASSERT_EQUAL_STRING("Test", *element);;
    free_list(myList, NULL);
}

void test_access_elements() {
    list *myList =List(0, sizeof(char *),true);
    char *str1 = strdup("Hello");
    char *str2 = strdup("World");
    char *str3 = strdup("Test");
    insert(myList, &str1);
    insert(myList, &str2);
    insert(myList, &str3);
    char **element = (char **)at(myList, 0);
    TEST_ASSERT_EQUAL_STRING("Hello", *element);
    element = (char **)at(myList, 1);
    TEST_ASSERT_EQUAL_STRING("World", *element);
    element = (char **)at(myList, 2);
    TEST_ASSERT_EQUAL_STRING("Test", *element);
    free_list(myList, NULL);
}

void test_free_list_elements() {
    list *myList = List(0, sizeof(char *),true);
    char *str1 = strdup("Hello");
    char *str2 = strdup("World");
    char *str3 = strdup("Test");
    insert(myList, &str1);
    insert(myList, &str2);
    insert(myList, &str3);
    free_list(myList, NULL);
    TEST_PASS_MESSAGE("Freed list elements");
}

void test_free_list() {
    list *myList =List(0, sizeof(char *),true);
    free_list(myList, NULL);
    TEST_PASS_MESSAGE("Freed list");
}

void test_insert_into_NULL_list() {
    char *str1 = strdup("Hello");
    insert(NULL, &str1); 
    TEST_PASS_MESSAGE("Handled insert into NULL list");
    free(str1); 
}

void test_insert_NULL_element() {
    list *myList = List(0, sizeof(char *),true);
    insert(myList, NULL); 
    TEST_ASSERT_EQUAL_INT(0, myList->len);
    free_list(myList, NULL);
    TEST_PASS_MESSAGE("Handled insert NULL element");
}

void test_insert_at_invalid_index() {
    list *myList = List(0, sizeof(char *),true);
    char *str1 = strdup("Invalid");
    inset_at(myList, &str1, -1); 
    TEST_ASSERT_EQUAL_INT(0, myList->len);
    inset_at(myList, &str1, 100); 
    TEST_ASSERT_EQUAL_INT(0, myList->len);
    free_list(myList, NULL);
    free(str1); 
    TEST_PASS_MESSAGE("Handled insert at invalid index");
}

void test_remove_from_NULL_list() {
    remove_at(NULL, 0); 
    TEST_PASS_MESSAGE("Handled remove from NULL list");
}

void test_remove_at_invalid_index() {
    list *myList = List(0, sizeof(char *),true);
    remove_at(myList, -1);
    TEST_ASSERT_EQUAL_INT(0, myList->len);
    remove_at(myList, 100); 
    TEST_ASSERT_EQUAL_INT(0, myList->len);
    free_list(myList, NULL);
    TEST_PASS_MESSAGE("Handled remove at invalid index");
}

void test_at_from_NULL_list() {
    list *myList = NULL;
    void *element = at(myList, 2);
    TEST_ASSERT_NULL(element);
    TEST_PASS_MESSAGE("Handled get element from NULL list");
}

void test_at_at_invalid_index() {
    list *myList = List(0, sizeof(char *),true);
    void *element = at(myList, -1); 
    TEST_ASSERT_NULL(element);
    element = at(myList, 100); 
    TEST_ASSERT_NULL(element);
    free_list(myList, NULL);
    TEST_PASS_MESSAGE("Handled get element at invalid index");
}
void test_sortList_ascending(void) {
    kv items[] = { { .key = "b" }, { .key = "a" }, { .key = "c" } };
    list myList = { .arr = items, .len = 3, .cap = 3, .dtS = sizeof(kv) };
    sort_list(&myList, true, compare_values_by_key);
    TEST_ASSERT_EQUAL_STRING("a", ((kv*)at(&myList, 0))->key);
    TEST_ASSERT_EQUAL_STRING("b", ((kv*)at(&myList, 1))->key);
    TEST_ASSERT_EQUAL_STRING("c", ((kv*)at(&myList, 2))->key);
}

void test_sortList_descending(void) {
    kv items[] = { { .key = "b" }, { .key = "a" }, { .key = "c" } };
    list myList = { .arr = items, .len = 3, .cap = 3, .dtS = sizeof(kv) };
    sort_list(&myList, false, compare_values_by_key);
    TEST_ASSERT_EQUAL_STRING("c", ((kv*)at(&myList, 0))->key);
    TEST_ASSERT_EQUAL_STRING("b", ((kv*)at(&myList, 1))->key);
    TEST_ASSERT_EQUAL_STRING("a", ((kv*)at(&myList, 2))->key);
}
void test_sortList_empty(void) {
    list myList = { .arr = NULL, .len = 0, .cap = 0, .dtS = sizeof(kv) };
    sort_list(&myList, true, compare_values_by_key);
    TEST_ASSERT_EQUAL(0, myList.len);
}

void test_sortList_single_element(void) {
    kv item = { .key = "a" };
    list myList = { .arr = &item, .len = 1, .cap = 1, .dtS = sizeof(kv) };
    sort_list(&myList, true, compare_values_by_key);
    TEST_ASSERT_EQUAL_STRING("a", ((kv*)myList.arr)->key);
}
