#include "lsm.h"
#include <stdio.h>
#include "indexer.h"
#include <stdlib.h>
#include "../../ds/associative_array.h"
#include "../../ds/frontier.h"
#include "../../ds/hashtbl.h"
#pragma once
#define EODB 0

/* write a proper implementation plan*/
/*The read path involves block_indexs with values, encased by sst tables, which are seperated into
seven levels. The iterator is used for range scans/deletes and allows an abstraction of as if 
every key-value pair can be accessed linearly in memeory. Since every sst table outside of level 0
has a gurantee of no range overlaps, only one sst_iter is needed per level and one for memetable\
level 0. Since blocks also have a gurantee of being ordered, only one block index cursor is required
per level. This enables each sub-iter to return their "next" and have that compared to every
other "next".*/

/**
 * @brief Iterator for traversing a block of key-value pairs
 * @struct block_iter
 * @param arr Array of key-value pairs
 * @param curr_key_ind Current index in the key-value array
 * @param index Pointer to the block index
 */
typedef struct block_iter {
    cache_entry ce;
    int curr_key_ind;
    block_index *index;
} block_iter;

/**
 * @brief Iterator for traversing an SST (Sorted String Table) file
 * @struct sst_iter
 * @param cursor Block iterator for the current block
 * @param file Pointer to the SST file information
 * @param curr_index Current index in the SST file
 */
typedef struct sst_iter {
    block_iter cursor;
    sst_f_inf *file; //pointer to the sst_f_inf
    int curr_index;
} sst_iter;

/**
 * @brief Iterator for traversing a level in the LSM tree
 * @struct level_iter
 * @param cursor SST iterator for the current SST file
 * @param level Pointer to the list of SST files in this level
 * @param curr_index Current index in the level's SST file list
 */
typedef struct level_iter {
    sst_iter cursor;
    list *level; //simply a pointer to the real sst_list of the level
    int curr_index;
} level_iter;

/**
 * @brief Iterator for traversing a memory table
 * @struct mem_table_iter
 * @param cursor Pointer to the current node in the memory table
 * @param t Pointer to the memory table being traversed
 */
typedef struct mem_table_iter {
    Node *cursor;
    mem_table *t;
} mem_table_iter;
/**
 * @brief A single database iterator for traversing the entire database
 * @struct aseDB_iter
 * @param mem_table Array of memory table iterators (2 elements)
 * @param l_1_n_level_iters Array of level iterators for levels 1-6
 * @param l_0_sst_iters List of SST iterators for level 0
 * @param pq Priority queue (frontier) for merging results
 * @param ret List for storing results
 * @param c Pointer to the shard controller
 */
typedef struct aseDB_iter {
    mem_table_iter mem_table[2];
    level_iter l_1_n_level_iters [6];
    list * l_0_sst_iters;
    frontier * pq;
    list * ret;
    shard_controller * c;
} aseDB_iter;
/**
 * @brief Compares two merge_data structures for sorting
 * @param one Pointer to the first merge_data structure
 * @param two Pointer to the second merge_data structure
 * @return Integer less than, equal to, or greater than zero if one is found to be less than, equal to, or greater than two
 */
int compare_merge_data(const void * one, const void * two);

/**
 * @brief Creates a new database iterator
 * @return Pointer to the newly created database iterator
 */
aseDB_iter * create_aseDB_iter(void);

/**
 * @brief Initializes a block iterator
 * @param b_cursor Pointer to the block iterator to initialize
 * @param file Pointer to the block index
 */
void init_block_iter(block_iter *b_cursor, block_index *file);

/**
 * @brief Initializes an SST iterator
 * @param cursor Pointer to the SST iterator to initialize
 * @param level Pointer to the SST file information
 */
void init_sst_iter(sst_iter * cursor, sst_f_inf* level);

/**
 * @brief Initializes memory table iterators
 * @param mem_table_iters Array of memory table iterators to initialize
 * @param s Pointer to the storage engine
 * @param num_tables Number of memory tables to initialize
 */
void init_mem_table_iters(mem_table_iter *mem_table_iters, storage_engine *s, size_t num_tables);
/**
 * @brief Initializes level iterators
 * @param level_iters Array of level iterators to initialize
 * @param s Pointer to the storage engine
 * @param num_levels Number of levels to initialize
 */
void init_level_iters(level_iter *level_iters, storage_engine *s, size_t num_levels);

/**
 * @brief Initializes level 0 SST iterators
 * @param l_0_sst_iters List to store the initialized level 0 SST iterators
 * @param s Pointer to the storage engine
 */
void init_level_0_sst_iters(list *l_0_sst_iters, storage_engine *s);

/**
 * @brief Initializes a database iterator
 * @param dbi Pointer to the database iterator to initialize
 * @param s Pointer to the storage engine
 */
void init_aseDB_iter(aseDB_iter *dbi, storage_engine *s);

/**
 * @brief Gets the next entry from a block iterator
 * @param b Pointer to the block iterator
 * @param c Pointer to the shard controller
 * @param file_name Name of the file being iterated
 * @return The next merge_data entry
 */
merge_data next_entry(block_iter *b,   shard_controller * c, sst_f_inf * inf);

/**
 * @brief Gets the next key block from an SST iterator
 * @param sst Pointer to the SST iterator
int write_record(storage_engine *engine, f_str key, f_str value)
Writes a key-value pair to the storage engine.

Parameters:
engine – A pointer to the storage engine.
key – The key to write.
value – The value to write.

Returns:
An integer indicating success (0) or failure (non-zero). * @param c Pointer to the shard controller
 * @return The next merge_data entry
 */
merge_data next_key_block(sst_iter *sst,  shard_controller * c);
merge_data next_sst_block(level_iter *level,   shard_controller *c);

/**
 * @brief Gets the current key-value pair from an SST iterator
 * @param sst_it The SST iterator
 * @return The current merge_data entry
 */
merge_data get_curr_kv(sst_iter sst_it);

/**
 * @brief Gets the next entry from a memory table iterator
 * @param iter Pointer to the memory table iterator
 * @return The next merge_data entry
 */
merge_data next_entry_memtable(mem_table_iter * iter);

/**
 * @brief Seeks to a specific prefix in an SST iterator
 * @param sst_it Pointer to the SST iterator
 * @param c Pointer to the shard controller
 * @param prefix The prefix to seek to
 */

void seek(aseDB_iter * iter , f_str prefix);
/**
 * @brief Seeks to a specific prefix in a database iterator
 * @param iter Pointer to the database iterator
 * @param prefix The prefix to seek to
 */
void seek_sst(sst_iter* sst_it, shard_controller * cach ,f_str prefix);
/**
 * @brief Checks if two merge_data entries have the same key
 * @param next The next merge_data entry
 * @param last The last merge_data entry
 * @return A merge_data entry with the same key but updated value
 */
merge_data same_merge_key(const merge_data next, const merge_data last);

/**
 * @brief Gets the next entry from a database iterator
 * @param iter Pointer to the database iterator
 * @return The next merge_data entry
 */
merge_data aseDB_iter_next(aseDB_iter * iter);

/**
 * @brief Writes a database entry to a byte buffer
 * @param b Pointer to the byte buffer
 * @param element Pointer to the element to write
 * @return Number of bytes written or error code
 */
int write_db_entry(byte_buffer * b, void * element);

/**
 * @brief Frees a database iterator and its resources
 * @param iter Pointer to the database iterator to free
 */
void free_aseDB_iter(aseDB_iter *iter);



