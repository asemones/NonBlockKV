#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "hashtbl.h"
#include <pthread.h>
#include "byte_buffer.h"

#ifndef LIST_H
#define LIST_H

/**
* Simple List Structure
* @param arr the base dynamic array
* @param len the number of elements in the array
* @param cap the maxiumum size of the array
* @param dtS the size of element datatype. Get this from sizeof(list_element_type)
*/
typedef struct Lists{
    void* arr;
    int len;
    int cap;
    int dtS;
    bool isAlloc;
    bool thread_safe;
}list;
typedef int (*compare)( void* , void*);

/**
 * @brief Creates a new list.
 * @param Size Initial capacity of the list.
 * @param dataTypeSize Size of each element in the list.
 * @param alloc Flag indicating whether the list should allocate memory for elements.
 * @return Pointer to the newly created list.
 */
list* List(int Size, int dataTypeSize, bool alloc);

/**
 * @brief Creates a new thread-safe list.
 * @param Size Initial capacity of the list.
 * @param dataTypeSize Size of each element in the list.
 * @param alloc Flag indicating whether the list should allocate memory for elements.
 * @return Pointer to the newly created thread-safe list.
 */
list* thread_safe_list(int Size, int dataTypeSize, bool alloc);

/**
 * @brief Expands the capacity of the list.
 * @param my_list Pointer to the list.
 */
void expand(list * my_list);

/**
 * @brief Inserts an element at the end of the list.
 * @param my_list Pointer to the list.
 * @param element Pointer to the element to insert.
 */
void insert(list * my_list, void * element);

/**
 * @brief Retrieves an element at a given index without locking (internal use).
 * @param my_list Pointer to the list.
 * @param index Index of the element to retrieve.
 * @return Pointer to the element at the specified index.
 */
void* internal_at_unlocked(list * my_list, int index);

/**
 * @brief Inserts an element at a given index without locking (internal use).
 * @param my_list Pointer to the list.
 * @param element Pointer to the element to insert.
 * @param index Index at which to insert the element.
 */
void inset_at_unlocked(list * my_list, void * element, int index);

/**
 * @brief Inserts an element at a given index.
 * @param my_list Pointer to the list.
 * @param element Pointer to the element to insert.
 * @param index Index at which to insert the element.
 */
void inset_at(list * my_list, void * element, int index);

/**
 * @brief Frees the memory allocated for the list.
 * @param my_list Pointer to the list.
 * @param free_func Function pointer to free individual elements.
 */
void free_list(list * my_list, void (*free_func)(void*));

/**
 * @brief Removes an element at a given index.
 * @param my_list Pointer to the list.
 * @param index Index of the element to remove.
 */
void remove_at(list * my_list, int index);

/**
 * @brief Removes an element at a given index without locking (internal use).
 * @param my_list Pointer to the list.
 * @param index Index of the element to remove.
 */
void remove_at_unlocked(list * my_list, int index);

/**
 * @brief Removes an element at a given index without shifting subsequent elements.
 * @param my_list Pointer to the list.
 * @param index Index of the element to remove.
 * @param free_func Function pointer to free the removed element.
 */
void remove_no_shift(list * my_list, int index, void (*free_func)(void*));

/**
 * @brief Retrieves an element at a given index.
 * @param my_list Pointer to the list.
 * @param index Index of the element to retrieve.
 * @return Pointer to the element at the specified index.
 */
void * at(list * my_list, int index);

/**
 * @brief Copies an element at a given index to a provided storage location.
 * @param my_list Pointer to the list.
 * @param storage Pointer to the storage location.
 * @param index Index of the element to copy.
 */
void at_copy(list * my_list, void * storage, int index);

/**
 * @brief Swaps two elements.
 * @param a Pointer to the first element.
 * @param b Pointer to the second element.
 * @param len Size of the elements.
 */
void swap(void * a, void * b, size_t len);

/**
 * @brief Shifts elements in the list to the left starting from a given index.
 * @param my_list Pointer to the list.
 * @param start_index Index from which to start shifting.
 */
void shift_list(list * my_list, int start_index);

/**
 * @brief Retrieves the last element of the list.
 * @param my_list Pointer to the list.
 * @return Pointer to the last element.
 */
void * get_last(list * my_list);

/**
 * @brief Compares two values by their keys (implementation-specific).
 * @param one Pointer to the first value.
 * @param two Pointer to the second value.
 * @return Comparison result (negative if one < two, 0 if equal, positive if one > two).
 */
int compare_values_by_key(void * one, void * two);

/**
 * @brief Compares two string values.
 * @param one Pointer to the first string.
 * @param two Pointer to the second string.
 * @return Comparison result (negative if one < two, 0 if equal, positive if one > two).
 */
int compare_string_values(void * one, void * two);

/**
 * @brief Sorts the list.
 * @param my_list Pointer to the list.
 * @param ascending Flag indicating whether to sort in ascending order.
 * @param func Comparison function.
 */
void sort_list(list * my_list, bool ascending, compare func);

/**
 * @brief Checks if an element exists in the list.
 * @param my_list Pointer to the list.
 * @param element Pointer to the element to search for.
 * @param func Comparison function.
 * @return true if the element is found, false otherwise.
 */
bool check_in_list(list * my_list, void * element, bool (*func)(void*, void*));

/**
 * @brief Compares two size_t values.
 * @param one Pointer to the first size_t value.
 * @param two Pointer to the second size_t value.
 * @return true if the values are equal, false otherwise.
 */
bool compare_size_t(void * one, void * two);

/**
 * @brief Dumps a specified number of list elements to a byte buffer.
 * @param my_list Pointer to the list.
 * @param item_write_func Function pointer to write individual elements.
 * @param stream Byte buffer to write to.
 * @param num Number of elements to dump.
 * @param start Starting index.
 * @return 0 on success, error code on failure.
 */
int dump_list_ele(list * my_list, int (*item_write_func)(byte_buffer*, void*),
                  byte_buffer * stream, int num, int start);

/**
 * @brief Merges two lists.
 * @param merge_into Pointer to the list to merge into.
 * @param merge_from Pointer to the list to merge from.
 * @param func Comparison function.
 * @return 0 on success, error code on failure.
 */
int merge_lists(list * merge_into, list * merge_from, compare func);

#endif





