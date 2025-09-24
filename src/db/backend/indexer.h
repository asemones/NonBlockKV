#include <stdio.h>
#include<stdlib.h>
#include <stdint.h>
#include <stddef.h> // For size_t
#include "../../ds/list.h"
#include "../../ds/bloomfilter.h"
#include "../../util/alloc_util.h"
#include "../../ds/arena.h"
#include "../../ds/checksum.h"
#include "../../util/error.h"
#include <time.h>
#include <zstd.h>
#include "../../util/stringutil.h"
#include "key-value.h"
#include "../../ds/associative_array.h"
#include "../../ds/hashtbl.h"
#include "compression.h"
#include "../../ds/speedy_bloom.h"
#define MAX_KEY_SIZE 100
#define MAX_F_N_SIZE 16
#pragma once
/*
Each SST (Sorted String Table) will have its own Bloom filter and min/max key.
These indexes are created and managed by the indexer.

Index Details:
- The index will be sparse, with a default block size of 16KB, but it can be smaller.
- Indexes should be stored at the bottom of the SST file to maintain organization and efficiency.
- Indexes are created both during SST table building and compaction.

Read Path Hierarchy:
0. Cache
1. Indexer
2. First-level filter
3. SST file index
4. SST block Bloom filter/ checking min/max key
5. Reading the actual data

Index Structure:
- Each SST file will have:
  - A primary Bloom filter.
  - Pointers to block-specific indexes, which include their Bloom filter and the max key
  - Min/max key for the entire SST file.
- The sst_index will be a list of sorted block_indexs that can be binary searched

*/

/**
 * @brief Structure representing an SST (Sorted String Table) file
 * @struct sst_file_info
 * @param file_name Name of the SST file
 * @param length Length of the SST file in bytes
 * @param block_indexs List of block indexes in the SST file
 * @param max Maximum key in the SST file
 * @param min Minimum key in the SST file
 * @param time Timestamp of when the SST file was created
 * @param block_start Starting offset of the first block
 * @param filter Bloom filter for the SST file
 * @param mem_store Memory arena used for allocations
 * @param marked Flag indicating if the SST file is marked for deletion
 * @param in_cm_job Flag indicating if the SST file is part of a compaction job
 * @param use_dict_compression Flag indicating if dictionary compression is used
 * @param compr_info Compression information for the SST file
 */
typedef struct sst_partition_ind{
    f_str min_fence;
    block_index * blocks;
    uint16_t num_blocks;
    bb_filter filter;
    uint64_t off;
    uint64_t len;
    void * pg;
}sst_partition_ind;

typedef struct sst_file_info{
    char file_name[MAX_F_N_SIZE]; /*accessed on io ops*/
    size_t iter_ref_count; /*accessed on pins*/
    size_t length;
    size_t compressed_len;
    uint64_t level;
    size_t block_start;
    size_t part_start;
    bool marked;
    bool in_cm_job;
    bool use_dict_compression; 
    struct timeval time;
    union{
        list *block_indexs; //type:  block_index, l_0 or maybe l1 too?
        list* sst_partitions; // Reverted back to pointer
    };

    
    f_str max;/*accessed when searching*/
    f_str min;/*accessed when searching*/
    bloom_filter * filter;
    arena * mem_store; /*REMOVE*/
    sst_cmpr_inf compr_info;

}sst_f_inf;
/**
 * @brief Structure representing a block index in an SST file
 * @struct block_index
 * @param offset Starting offset of the block in the SST file
 * @param filter Bloom filter for the block
 * @param min_key Minimum key in the block
 * @param len Length of the block in bytes
 * @param num_keys Number of keys in the block
 * @param checksum Checksum for the block data
 */
typedef enum block_type{
  PHYSICAL_PAGE_HEADER,
  MIDDLE,
  PHYSICAL_PAGE_END
}block_type;
typedef struct block_index{
    size_t offset; //a pointer to the starting location of the block index
    f_str min_key;
    size_t len;
    uint16_t num_keys;
    uint16_t footer_start;
    void * page;
    block_type type;
    uint32_t checksum;
} block_index;
block_index * create_block_index(size_t est_num_keys);
inline int num_blocks_in_pg(block_index * blocks);
static inline block_index * get_pg_header(block_index * blocks){
    int start = 0;
    while(blocks[start].type != PHYSICAL_PAGE_HEADER){
        start --;
    }
    return &blocks[start];
}
static inline int compare_sst(sst_f_inf * one, sst_f_inf * two){
    return f_cmp(one->min, two->min);
}
/**
 * @brief Frees a block index structure
 * @param index Pointer to the block index to free
 */
void free_block_index(void * index);

/**
 * @brief Creates an empty SST file info structure
 * @return Newly created empty SST file info
 */
sst_f_inf create_sst_empty(void);

/**
 * @brief Creates an SST file info structure with a Bloom filter
 * @param b Bloom filter to associate with the SST file
 * @return Newly created SST file info
 */
sst_f_inf create_sst_filter();

/**
 * @brief Performs a deep copy of an SST file info structure
 * @param master Source SST file info to copy from
 * @param copy Destination SST file info to copy to
 * @return 0 on success, non-zero on failure
 */
int sst_deep_copy(sst_f_inf * master, sst_f_inf * copy);

/**
 * @brief Creates a block index stack
 * @param est_num_keys Estimated number of keys in the block
 * @return Newly created block index
 */
block_index create_ind_stack(size_t est_num_keys);

/**
 * @brief Creates a block index from a byte stream
 * @param stream Byte buffer containing the block index data
 * @param index Block index structure to populate
 * @return Pointer to the populated block index
 */
block_index * block_from_stream(byte_buffer * stream, block_index * index);

/**
 * @brief Writes a block index to a byte stream
 * @param targ Target byte buffer to write to
 * @param index Block index to serialize
 * @return 0 on success, non-zero on failure
 */
int block_to_stream(byte_buffer * targ, block_index index);
/**
 * @brief Reads a block index from an SST file
 * @param file SST file info structure
 * @param stream Byte buffer to read from
 * @return 0 on success, non-zero on failure
 */
int read_index_block(sst_f_inf * file, byte_buffer * stream);

/**
 * @brief Writes all block indexes to a byte stream
 * @param num_table Number of tables to write
 * @param stream Byte buffer to write to
 * @param indexes List of block indexes to write
 * @return 0 on success, non-zero on failure
 */
int all_index_stream(size_t num_table, byte_buffer * stream, list * indexes);

/**
 * @brief Reuses a block index structure
 * @param b Block index to reuse
 */
void reuse_block_index(void * b);

/**
 * @brief Compares two SST file info structures
 * @param one First SST file info
 * @param two Second SST file info
 * @return Comparison result
 */
int cmp_sst_f_inf(void * one, void * two);

/**
 * @brief Checks if two SST file info structures are equal
 * @param one First SST file info
 * @param two Second SST file info
 * @return 1 if equal, 0 otherwise
 */
int sst_equals(sst_f_inf * one, sst_f_inf * two);

/**
 * @brief Finds an SST file by name in a list
 * @param sst_files List of SST files to search
 * @param num_files Number of files in the list
 * @param file_n File name to search for
 * @return Index of the matching file, or -1 if not found
 */
int find_sst_file_eq_iter(list * sst_files, size_t num_files, const char * file_n);



/**
 * @brief Builds an index for an SST file
 * @param sst SST file info structure
 * @param index Block index to build
 * @param b Byte buffer containing the data
 * @param num_entries Number of entries in the block
 * @param block_offsets Offset of the block in the SST file
 */
void build_index(sst_f_inf * sst, block_index * index, byte_buffer * b, size_t num_entries, size_t block_offsets);

/**
 * @brief Performs binary search on a JSON array
 * @param json JSON array to search
 * @param target Target key to search for
 * @return Index of the matching key, or -1 if not found
 */
int block_b_search(const uint16_t *offsets, int num_entries, const f_str target, const char * block_start);

/**
 * @brief Performs prefix-based binary search on a JSON array
 * @param json JSON array to search
 * @param target Target prefix to search for
 * @return Index of the matching key, or -1 if not found
 */
int prefix_b_search(const uint16_t *offsets, int num_entries, f_str prefix, const char *block_start);

/**
 * @brief Frees an SST file info structure
 * @param ss SST file info to free
 */
void free_sst_inf(void * ss);

/**
 * @brief Extracts the minimum key from a block
 * @param index Block index structure
 * @param b Byte buffer containing the block data
 * @param loc Location of the key in the buffer
 */
void grab_min_b_key(block_index * index, byte_buffer *b, int loc);
bool use_compression(sst_f_inf * f);
uint64_t block_ind_size();
int all_stream_index(size_t num_table, byte_buffer* stream, block_index * indexes);
void dump_sst_meta_part(sst_f_inf * to_write, byte_buffer * b, uint64_t part_start_4k_alligned, list * blocks);
f_str block_key_decode(const char * block_start, uint16_t * offsets, uint16_t ind);
f_str decode_val_from_k(f_str decoded_key);
merge_data decode_next_pair_iter(const char * block_start, uint16_t * offsets, uint16_t ind_to_get);
uint64_t sst_md_serialized_len(const sst_f_inf *s);
uint64_t sst_md_str(const sst_f_inf *s);
void seralize_sst_md_all(byte_buffer * b, sst_f_inf * in);