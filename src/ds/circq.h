#ifndef CIRCQ_H
#define CIRCQ_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>


// Macro to define a circular queue for a specific type
#define DEFINE_CIRCULAR_QUEUE(T, NAME) \
\
typedef struct { \
    T* array;         \
    int capacity;      \
    int front;         \
    int rear;         \
    int size;          \
} NAME; \
\
/* Function to create a new circular queue with given capacity */ \
static inline NAME* NAME##_create(int capacity) { \
    NAME* queue = (NAME*)malloc(sizeof(NAME)); \
    if (!queue) return NULL;   \
    \
    queue->capacity = capacity; \
    queue->front = -1; \
    queue->rear = -1; \
    queue->size = 0; \
    queue->array = (T*)malloc(capacity * sizeof(T)); \
    \
    if (!queue->array) { \
        free(queue); \
        return NULL; \
    } \
    \
    return queue; \
} \
\
 \
 static inline void NAME##_init(NAME* queue, int capacity) { \
    \
    queue->capacity = capacity; \
    queue->front = -1; \
    queue->rear = -1; \
    queue->size = 0; \
    queue->array = (T*)malloc(capacity * sizeof(T)); \
    \
    if (!queue->array) { \
        free(queue); \
        return; \
    } \
    \
    return; \
} \
\
 \
static inline bool NAME##_is_full(NAME* queue) { \
    return queue->size == queue->capacity; \
} \
\
/* Function to check if the queue is empty */ \
static inline bool NAME##_is_empty(NAME* queue) { \
    return queue->size == 0; \
} \
\
 \
static inline bool NAME##_enqueue(NAME* queue, T item) { \
    if (NAME##_is_full(queue)) { \
        return false;  /* Queue overflow */ \
    } \
    \
    /* If queue is initially empty */ \
    if (queue->front == -1) { \
        queue->front = 0; \
    } \
    \
    /* Move rear in a circular way */ \
    queue->rear = (queue->rear + 1) % queue->capacity; \
    queue->array[queue->rear] = item; \
    queue->size++; \
    \
    return true; \
} \
\
 \
static inline bool NAME##_dequeue(NAME* queue, T* item) { \
    if (NAME##_is_empty(queue)) { \
        return false;  /* Queue underflow */ \
    } \
    \
    *item = queue->array[queue->front]; \
    \
     \
    if (queue->front == queue->rear) { \
        queue->front = -1; \
        queue->rear = -1; \
    } \
    else { \
        \
        queue->front = (queue->front + 1) % queue->capacity; \
    } \
    \
    queue->size--; \
    return true; \
} \
\
 \
static inline bool NAME##_peek(NAME* queue, T* item) { \
    if (NAME##_is_empty(queue)) { \
        return false; \
    } \
    \
    *item = queue->array[queue->front]; \
    return true; \
} \
\
 \
 static inline bool NAME##_peek_slot_x(NAME* queue, T* item, int slot) { \
    if (NAME##_is_empty(queue)) { \
        return false; \
    } \
    \
    int slot_num = (queue->front + slot ) % queue->capacity;\
    *item = queue->array[slot_num]; \
    return true; \
} \
\
 \
static inline void NAME##_destroy(NAME* queue) { \
    if (queue) { \
        if (queue->array) { \
            free(queue->array); \
        } \
        free(queue); \
    } \
} \
\

#endif /* CIRCQ_H */
