#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "../../ds/byte_buffer.h"
#include "../../util/stringutil.h"
#include <math.h>
#include "../../util/maths.h"
#include "../../ds/arena.h"
#include "../../ds/frontier.h"
#include "../../util/alloc_util.h"
#include "option.h"
#include "iter.h"
#include "../../ds/threadpool.h"
#include "../../ds/cache_shrd_mnger.h"
#include <time.h>
#include "../../util/io.h"
#include <unistd.h>
#include "../../util/multitask.h"
#ifndef COMPACTOR_H
#define COMPACTOR_H
#define NUM_THREADP 1

/**
 * @brief Enumeration of compaction operation statuses
 * @enum compact_status
 */
enum compact_status{
    COMPACTION_SUCCESS,      /**< Compaction completed successfully */
    COMPACTION_FAILED,       /**< Compaction failed */
    COMPACTION_IN_PROGRESS,  /**< Compaction is currently in progress */
    COMPACTION_NOT_STARTED,  /**< Compaction has not been started yet */
};
/*i was very short sighted and created this struct, and frankly i do not want to change my 20+ tests on this
so i have two different "copies", one for the pq and one for the regular system*/
//compaction algorithm:
/*
leveled compaction with the goal of cutting down on high latency spikes: We can assume that b bytes of io bandwith are assigned to the dbms
by the user. This assumption allows us to create a scheduler that dynamically allocates io bandwith. From this, we can
take advantage of io bandwith when it is avaliable, avoiding latency spikes from compaction triggers lining up. Instead, 
we can use this scheduler to preform single-file compactations when there is "free" bandwith.
We can split all io operations into three priority levels, each one managed by thread(s). The first level
is for any memtable flushes and WAL. The second level is for the "shallow" level compactations (l_0, l_1)
and the third level for deeper level compactions. To prevent l_0 being filled with a bunch of tiny ssts, second level
compactions will always occur even if memtable flush rate is high. In such events, the bandwith will be assigned in a way
to maximize overall throughput to moving ssts from memory to l_1.

General compactation process:
1. Check if bandwith is avaliable for compaction SKIP FOR BASE
2. If bandwith is avaliable, check if a compactation is beneficial. This can be set as a "miniumum compactation size"
   . This is different from the maxiumum size of a sst level. If too small, return. SKIP FOR BASE
3. Then check the size of the next level. If it has room, continue. If not, return and call compact on next level down  
4  If a compactation is needed, find the sst files to merge by targeting files with overlapping key-ranges. 
   . This can be done by checking the metadata of each sst file. 
5. Then preform the compactation in the following manner:
    a. Load the sst files into memory
    b. Merge the sst files
    c. Write sst to a new file
    d. Update the metadata and bloom filter
    e. Delete the old sst files
6. Repeat 1-5 until level is empty
*/
/*More in depth algorithm:
Every x seconds run a basic compactation check on the compact thread. 
If needed utilize a thread from the threadpool to complete the job
*/

/*
*/
/**
 * @brief Structure representing an internal compaction job
 * @struct compact_job_internal
 * @param id Unique identifier for the job
 * @param index Index of the job in the queue
 * @param start_level Starting level for compaction
 * @param end_level Ending level for compaction
 * @param working_files Number of files being worked on
 * @param search_level Current level being searched
 * @param target Target SST file for compaction
 * @param to_merge List of SST files to merge
 * @param new_sst_files List of newly created SST files
 */
typedef struct compact_job_internal{
    size_t id;
    size_t start_level;
    size_t end_level;
    size_t working_files;
    size_t search_level;
    sst_f_inf * target;
    list * to_merge;
    list * new_sst_files;
} compact_job_internal;

/**
 * @brief Structure managing compaction operations
 * @struct compact_manager
 * @param pool Array of thread pools for compaction operations
 * @param compact_pool Pool of compaction resources
 * @param sst_files Array of lists containing SST files for each level
 * @param arena_pool Pool of memory arenas
 * @param base_level_size Size of the base level
 * @param wait_mtx Mutex for waiting on compaction
 * @param wait Condition variable for waiting on compaction
 * @param compact_lock Mutex for compaction operations
 * @param resource_lock Mutex for resource allocation
 * @param resource_sig Condition variable for resource availability
 * @param min_compact_ratio Minimum compaction ratio
 * @param dict_buffer_pool Pool of page buffers
 * @param big_buffer_pool Pool of large buffers
 * @param job_queue Priority queue of compaction jobs
 * @param check_meta_cond Flag to check metadata condition
 * @param lvl0_job_running Flag indicating if a level 0 job is running
 * @param exit Flag indicating if the compactor should exit
 * @param c Pointer to the shard controller
 */
typedef struct compact_manager{
    list ** sst_files;
    size_t base_level_size;
    size_t min_compact_ratio;
    struct_pool * dict_buffer_pool;
    frontier * job_queue;
    bool check_meta_cond;
    bool lvl0_job_running;
    bool exit;
    shard_controller * c; // of type bloom_filter, FROM META DATA
}compact_manager;
/**
 * @brief Frees a compaction manager and its resources
 * @param manager Pointer to the compaction manager to free
 */
/**
 * @brief Frees a compaction manager and its resources.
 * @param manager Pointer to the compaction manager to free.
 */
void free_cm(compact_manager * manager);

/**
 * @brief Creates a new compaction info structure.
 * @param page_size Size of the page buffer.
 * @return Pointer to the newly created compaction info.
 */

/**
 * @brief Sets the SST files in the compaction manager from metadata.
 * @param meta Pointer to the metadata.
 * @param cm Pointer to the compaction manager.
 */
void set_cm_sst_files(meta_data * meta, compact_manager * cm);

/**
 * @brief Compares two compaction jobs for priority queue ordering.
 * @param job1 Pointer to the first job.
 * @param job2 Pointer to the second job.
 * @return Comparison result for sorting (negative if job1 < job2, 0 if equal, positive if job1 > job2).
 */
int compare_jobs(const void * job1, const void * job2);

/**
 * @brief Initializes a compaction manager.
 * @param meta Pointer to the metadata.
 * @param c Pointer to the shard controller.
 * @return Pointer to the initialized compaction manager.
 */
compact_manager * init_cm(meta_data * meta, shard_controller * c);

/**
 * @brief Compares two timestamps.
 * @param t1 First timestamp.
 * @param t2 Second timestamp.
 * @return Negative if t1 < t2, 0 if equal, positive if t1 > t2.
 */
int compare_time_stamp(struct timeval t1, struct timeval t2);

/**
 * @brief Calculates the length of a merge data entry.
 * @param entry The merge data entry.
 * @return Length of the entry in bytes.
 */
int entry_len(merge_data entry);

/**
 * @brief Discards duplicate keys, keeping only the most recent value.
 * @param pq Priority queue of entries.
 * @param its SST iterator.
 * @param c Pointer to the shard controller.
 * @param initial Initial merge data entry.
 * @return Merge data with duplicates removed.
 */
merge_data discard_same_keys(frontier *pq, sst_iter *its, shard_controller *c, merge_data initial);

/**
 * @brief Completes a block by updating its metadata.
 * @param curr_sst Pointer to the current SST file.
 * @param current_block Pointer to the current block.
 * @param block_b_count Block byte count.
 */
void complete_block(sst_f_inf * curr_sst, block_index * current_block, int block_b_count);

/**
 * @brief Resets block counters to their initial values.
 * @param block_b_count Pointer to the block byte count.
 * @param sst_b_count Pointer to the SST byte count.
 * @param num_block_counter Pointer to the block counter.
 */
void reset_block_counters(int *block_b_count, int *sst_b_count);

/**
 * @brief Initializes a new block.
 * @param mem_store Memory arena for allocation.
 * @param off_track Pointer to track offset.
 * @return Initialized block index.
 */
block_index init_block(arena * mem_store, size_t* off_track);

/**
 * @brief Merges tables during compaction.
 * @param dest_buffer Destination buffer.
 * @param job Compaction job.
 * @param a Memory arena.
 * @param c Shard controller.
 * @param cm Compaction manager.
 * @return 0 on success, error code on failure.
 */
int merge_tables(byte_buffer *dest_buffer, byte_buffer * compression_buffer, compact_job_internal * job, byte_buffer * dict_buffer, shard_controller * c);

/**
 * @brief Calculates the overlap between two key ranges.
 * @param min1 Minimum key of first range.
 * @param max1 Maximum key of first range.
 * @param min2 Minimum key of second range.
 * @param max2 Maximum key of second range.
 * @return Degree of overlap.
 */
int calculate_overlap(const f_str min1,  const f_str max1, const f_str min2, const f_str max2);

/**
 * @brief Gets the size of a level.
 * @param level Level number.
 * @return Size of the level.
 */
int get_level_size(int level);

/**
 * @brief Removes an SST file from a level if it matches the target.
 * @param target Target SST file.
 * @param target_lvl List of SST files in the level.
 * @param level Level number.
 * @return 0 on success, error code on failure.
 */
int remove_same_sst(sst_f_inf * target, list * target_lvl, int level);

/**
 * @brief Removes an SST file from a list.
 * @param target Target SST file.
 * @param target_lvl List of SST files.
 * @param level Level number.
 * @return 0 on success, error code on failure.
 */
int remove_sst_from_list(sst_f_inf * target, list * target_lvl, int level);

/**
 * @brief Compacts a single table.
 * @param cm Pointer to the compaction manager.
 * @param job Compaction job.
 * @param victim Victim SST file to compact.
 */
void compact_one_table(compact_manager * cm, compact_job_internal job, sst_f_inf * victim);

/**
 * @brief Integrates newly created tables into the database.
 * @param cm Pointer to the compaction manager.
 * @param job Pointer to the compaction job.
 * @param old_ssts List of old SST files.
 */
void integrate_new_tables(compact_manager *cm, compact_job_internal *job, list *old_ssts);
/**
 * @brief Shuts down the compaction manager.
 * @param cm Pointer to the compaction manager.
 */
void shutdown_cm(compact_manager * cm);

/**
 * @brief Frees a compaction manager and its resources.
 * @param manager Pointer to the compaction manager to free.
 */
void free_cm(compact_manager * manager);

#endif