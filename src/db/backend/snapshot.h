#include <stdlib.h>
#include <stdio.h>
#include "../../ds/list.h"
#include <pthread.h>
#include "../../util/error.h"
#include "../../util/stringutil.h"
#include "indexer.h"
#include "../../ds/cache.h"
#include "../../ds/cache_shrd_mnger.h"
#define LEVELS 7
#define TIME_STAMP_SIZE 64
#pragma once





/*need to replace current metadata raw list with snapshots. Snapshots = the db, minus meetables for now. Using this, we can effectively track and exchange database snapshots while controlling
freeing of resources. It's the solution for allowing concurrenct compaction and required for mvcc*/
/*problems to solve:
1. How can we ensure that compactations can occur without locking reads/writes
*/
/**
 * @brief Database snapshot structure for point-in-time recovery and MVCC
 * @struct snapshot
 * @param timestamp String representation of the snapshot creation time
 * @param reference_counter Number of references to this snapshot
 * @param counter_lock Mutex for thread-safe reference counting
 * @param sst_files Array of lists containing SST files for each level
 * @param cache_ref Pointer to the shard controller for cache operations
 */
typedef struct snapshot{
    char * timestamp;
    int reference_counter;
    pthread_mutex_t counter_lock;
    list * sst_files[LEVELS];
    shard_controller* cache_ref; /*for reading and writing*/
}snapshot;


/**
 * @brief Creates a new empty snapshot
 * @return Pointer to the newly created snapshot
 */
snapshot * create_snap(void);

/**
 * @brief Initializes a snapshot from a master snapshot
 * @param master Pointer to the master snapshot to copy from
 * @param s Pointer to the snapshot to initialize
 * @param c Pointer to the shard controller for cache operations
 * @return 0 on success, error code on failure
 */
int init_snapshot(snapshot * master, snapshot * s, shard_controller* c);

/**
 * @brief Destroys a snapshot and frees its resources
 * @param s Pointer to the snapshot to destroy
 * @return 0 on success, error code on failure
 */
int destroy_snapshot(snapshot * s);

/**
 * @brief Increments the reference counter of a snapshot
 * @param s Pointer to the snapshot
 */
void ref_snapshot(snapshot * s);

/**
 * @brief Decrements the reference counter of a snapshot
 * @param s Pointer to the snapshot
 */
void deref_snapshot(snapshot * s);

/**
 * @brief Decrements the reference counter and frees the snapshot if counter reaches zero
 * @param s Pointer to the snapshot
 */
void deref_with_free(snapshot * s);
