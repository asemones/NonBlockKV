#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "list.h"
#include "stdbool.h"
#include "stdint.h"
#include "hashtbl.h"
#pragma once

/**
 * @brief Priority queue implementation using a heap data structure
 * @struct frontier
 * @param queue Pointer to the list storing the elements
 * @param compare Function pointer for comparing elements to maintain heap property
 */
typedef struct frontier{
    list* queue;
    int (*compare)(const void*, const void*); // Comparison function pointer
} frontier;

/**
 * @brief Creates a new frontier priority queue structure.
 *
 * @param dts Size of the elements being added
 * @param is_alloc Boolean indicating if allocation is needed.
 * @param compare Function pointer for comparing two elements.
 * @return Pointer to the newly created frontier.
 */
frontier* Frontier(size_t dts, bool is_alloc, int (*compare)(const void*, const void*));

/**
 * @brief Ensures the heap property is maintained at the specified index.
 *
 * @param front Pointer to the frontier.
 * @param index The index at which to ensure the heap property.
 */
void heapify(frontier *front, int index);

/**
 * @brief Adds a new element to the frontier (enqueue operation).
 *
 * @param front Pointer to the frontier.
 * @param element Pointer to the element to be added to the frontier.
 */
void enqueue(frontier *front, void* element);

/**
 * @brief Removes and returns the element with the highest priority from the frontier (dequeue operation).
 * The returned element is copied to the provided buffer.
 *
 * @param front Pointer to the frontier.
 * @param out_buffer Buffer where the removed element will be copied.
 */
int dequeue(frontier * front, void* out_buffer);

/**
 * @brief Returns the element with the highest priority without removing it from the frontier.
 *
 * @param front Pointer to the frontier.
 * @return Pointer to the element with the highest priority.
 */
void* peek(frontier * front);

/**
 * @brief Frees all resources associated with the frontier.
 *
 * @param front Pointer to the frontier to be freed.
 */
void free_front(frontier * front);
