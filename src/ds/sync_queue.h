#ifndef SYNC_CIRCQ_H
#define SYNC_CIRCQ_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>

// Macro to define a synchronized circular queue for a specific type
#define DEFINE_SYNC_CIRCULAR_QUEUE(T, NAME) \
\
typedef struct { \
    T* array; \
    int capacity; \
    int front; \
    int rear; \
    int size; \
    pthread_mutex_t lock; \
} NAME; \
\
/* Function to create a new synchronized circular queue with given capacity */ \
static inline NAME* NAME##_create(int capacity) { \
    NAME* queue = (NAME*)malloc(sizeof(NAME)); \
    if (!queue) return NULL; \
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
    if (pthread_mutex_init(&queue->lock, NULL) != 0) { \
        free(queue->array); \
        free(queue); \
        return NULL; \
    } \
\
    return queue; \
} \
\
static inline bool NAME##_is_full(NAME* queue) { \
    return queue->size == queue->capacity; \
} \
\
static inline bool NAME##_is_empty(NAME* queue) { \
    return queue->size == 0; \
} \
\
static inline void NAME##_lock(NAME* queue) { \
    pthread_mutex_lock(&queue->lock); \
} \
\
static inline void NAME##_unlock(NAME* queue) { \
    pthread_mutex_unlock(&queue->lock); \
} \
\
/* Function to add an item to the queue with regular locking */ \
static inline bool NAME##_enqueue(NAME* queue, T item) { \
    /* Acquire the lock */ \
    NAME##_lock(queue); \
\
    bool result = false; \
    if (!NAME##_is_full(queue)) { \
        /* If queue is initially empty */ \
        if (queue->front == -1) { \
            queue->front = 0; \
        } \
\
        /* Move rear in a circular way */ \
        queue->rear = (queue->rear + 1) % queue->capacity; \
        queue->array[queue->rear] = item; \
        queue->size++; \
        result = true; \
    } \
\
    /* Release the lock */ \
    NAME##_unlock(queue); \
    return result; \
} \
\
/* Function to remove an item from the queue with regular locking */ \
static inline bool NAME##_dequeue(NAME* queue, T* item) { \
    /* Acquire the lock */ \
    NAME##_lock(queue); \
\
    bool result = false; \
    if (!NAME##_is_empty(queue)) { \
        *item = queue->array[queue->front]; \
\
        /* If this is the last element */ \
        if (queue->front == queue->rear) { \
            queue->front = -1; \
            queue->rear = -1; \
        } else { \
            queue->front = (queue->front + 1) % queue->capacity; \
        } \
\
        queue->size--; \
        result = true; \
    } \
\
    /* Release the lock */ \
    NAME##_unlock(queue); \
    return result; \
} \
\
/* Function to peek at the front item without removing it, with regular locking */ \
static inline bool NAME##_peek(NAME* queue, T* item) { \
    /* Acquire the lock */ \
    NAME##_lock(queue); \
\
    bool result = false; \
    if (!NAME##_is_empty(queue)) { \
        *item = queue->array[queue->front]; \
        result = true; \
    } \
\
    /* Release the lock */ \
    NAME##_unlock(queue); \
    return result; \
} \
\
/* Function to destroy the queue and free allocated memory */ \
static inline void NAME##_destroy(NAME* queue) { \
    if (queue) { \
        pthread_mutex_destroy(&queue->lock); \
        if (queue->array) { \
            free(queue->array); \
        } \
        free(queue); \
    } \
} \
static inline int NAME##_dequeue_bulk(NAME* q, T* dst, int count){\
    if(count<=0) return 0;\
    NAME##_lock(q); \
    int removed=0; \
    if(!NAME##_is_empty(q)){ \
        if(count>q->size) count=q->size; \
        int cap=q->capacity; \
        int f  =q->front;\
        /* contiguous chunk #1 */  \
        int first = (f+count<=cap) ? count : cap - f;\
        memcpy(dst, &q->array[f], first*sizeof(T));\
        removed = first;\
        f = (f+first) % cap;\
        if(removed < count){\
            int second = count-removed;\
            memcpy(dst+removed,&q->array[f], second*sizeof(T));\
            removed += second;\
            f = (f+second) % cap;\
        }\
        if(removed == q->size){\
            q->front = q->rear = -1;\
        }else{\
            q->front = f;\
        }\
        q->size -= removed;\
    }\
    NAME##_unlock(q);\
    return removed;\
}                 
#endif 