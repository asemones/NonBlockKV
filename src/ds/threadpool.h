#include <pthread.h>
#include "frontier.h"
#include <unistd.h>
#include <stdio.h>
#include "../util/alloc_util.h"
#include "arena.h"
#include "hashtbl.h"
#ifndef THREADPOOL_H
#define THREADPOOL_H





/**
 * @brief Thread structure representing a worker thread
 * @struct thread
 * @param thread The pthread identifier
 * @param ID Thread identifier within the pool
 * @param thrpool Pointer to the thread pool this thread belongs to
 * @param buf Buffer for thread-specific data
 */
typedef struct thread{
    pthread_t thread;
    int ID;
    struct thread_p * thrpool;
    char ** buf;
}thread;
/**
 * @brief Work item structure representing a task to be executed
 * @struct work
 * @param function Function pointer to the task to be executed
 * @param arg Argument to be passed to the function
 * @param reV Pointer to store the result of the function
 */
typedef struct work{
    void (*function)(void* arg, void** result, thread* thread);
    void * arg;
    void ** reV;
}work;

/**
 * @brief Priority queue value structure for work items
 * @struct thrd_pq_value
 * @param priority Priority level of the work item
 * @param item The work item
 */
typedef struct thrd_pq_value{
    int priority;
    work item;
}thrd_pq_value;

/**
 * @brief Thread pool structure managing a collection of worker threads
 * @struct thread_p
 * @param threads Array of pointers to worker threads
 * @param num_threads Number of threads in the pool
 * @param num_killed Number of threads that have been killed
 * @param counter_lock Mutex for thread counter operations
 * @param queue_cond Condition variable for queue operations
 * @param out_lock Mutex for output operations
 * @param queue_lock Mutex for queue operations
 * @param freeze Condition variable for freezing thread operations
 * @param work_pq Priority queue for work items
 * @param out_pq Priority queue for output items
 * @param killSig Signal to kill all threads
 */
typedef struct thread_p{
    thread** threads;
    volatile int  num_threads;
    int num_killed;
    pthread_mutex_t counter_lock;
    pthread_cond_t queue_cond;
    pthread_mutex_t out_lock;
    pthread_mutex_t queue_lock;
    pthread_cond_t freeze;
    frontier *work_pq;
    frontier * out_pq;
    bool killSig;

}thread_p;

/**
 * @brief Executes the threads
 *
 * @param p Pointer to the thread to be executed.
 */
void execute(thread *p);

/**
 * @brief Creates a new thread with the given id and adds it to the specified thread pool.
 *
 * @param id The identifier for the new thread.
 * @param pool Pointer to the thread pool to which the new thread will be added.
 * @return Pointer to the newly created thread.
 */
thread* Thread(int id, thread_p* pool, size_t dts);

/**
 * @brief Creates a thread pool with a specified number of threads.
 *
 * @param num_threads The number of threads to create in the thread pool.
 * @return Pointer to the newly created thread pool.
 */
thread_p *thread_Pool(int num_threads, size_t thrd_mem_size);

/**
 * @brief Adds a new work item to the thread pool.
 *
 * @param p Pointer to the thread pool.
 * @param functions Pointer to the function to be executed.
 * @param args Pointer to the arguments to be passed to the function.
 * @param retV Pointer to store the return value of the function.
 * @param priority Pointer to the priority of the work item.
 */
void add_work(thread_p *p,  void (*functions)(void*, void **, thread *p), void* args, void ** retV, int * priority);

/**
 * @brief Retrieves the result of a work item executed by a thread.
 *
 * @param p Pointer to the thread.
 * @param work_item Pointer to store the result of the work item.
 */
void get_result(thread * p , thrd_pq_value work_item);
/**
 * @brief Terminates all threads in the specified thread pool.
 *
 * @param p Pointer to the thread pool.
 */
void kill_thr(thread_p * p);

/**
 * @brief Destroys the thread pool and frees all associated resources.
 *
 * @param p Pointer to the thread pool.
 */
void destroy_pool(thread_p *p);
/**
 * @brief Destroys the thread pool and frees all associated resources.
 *
 * @param temp pointer to the kv element of the queue
 * @param void(*function)(void* arg, void *arg2) a function to free the elemenets (whatever is stored as kv->value)
 * @param arg2 the second arguement of the function to free the work elements
 */
void free_work_elements(kv* temp, void(*function)(void* arg, void *arg2), void *arg2);
/**
 * @brief Compares two priority queue values for sorting
 * @param pq_v Pointer to the first priority queue value
 * @param pq_v2 Pointer to the second priority queue value
 * @return Negative if pq_v < pq_v2, 0 if equal, positive if pq_v > pq_v2
 */
int compare_pq_v(const void * pq_v, const void * pq_v2);


#endif