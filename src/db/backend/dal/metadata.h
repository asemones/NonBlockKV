#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../../../util/io.h"
#include "../../../ds/hashtbl.h"
#include "../../../ds/list.h"
#include "../../../ds/bloomfilter.h"
#include "../../../ds/byte_buffer.h"
#include "../../../util/alloc_util.h"
#include"../indexer.h"
#include "../../../util/error.h"
#include "../snapshot.h"
#include <zdict.h>
#include "../../../util/io.h"
#include "../../../util/io_types.h"
#ifndef METADATA_H
#define METADATA_H
#define MAX_LEVELS 7



/*This is the meta_data struct. The meta_data struct is the equvialent of the cataologue in most dbms. 
It containes all the information about the database including all indexes and the bloom filters for each level,
although stored in one fat list. */
/**
 * @brief Database metadata structure (catalog) containing information about the database state
 * @struct meta_data
 * @param num_sst_file Total number of SST files in the database
 * @param file_ptr Current file pointer position
 * @param db_length Total length of the database
 * @param base_level_size Size of the base level
 * @param min_c_ratio Minimum compaction ratio
 * @param sst_files Array of lists containing SST files for each level
 * @param levels_with_data Array indicating which levels contain data
 * @param current_tx_copy Current transaction snapshot
 * @param shutdown_status Status code for shutdown
 * @param err_code Error code
 */
typedef struct meta_data{
    size_t num_sst_file;
    size_t file_ptr;
    size_t db_length;
    size_t base_level_size;
    size_t min_c_ratio; //minimum compact ratio
    list * sst_files[MAX_LEVELS];
    int levels_with_data[MAX_LEVELS];
    snapshot * current_tx_copy;
    int shutdown_status;
    int err_code;
    db_FILE * file;
}meta_data;
/**
 * @brief Reads a list of SST files from a buffer
 * @param sst_files Pointer to the list to populate
 * @param tempBuffer Buffer containing the SST file data
 * @param num_sst Number of SST files to read
 * @return 0 on success, error code on failure
 */
int read_sst_list(list * sst_files, byte_buffer * tempBuffer, size_t num_sst);

/**
 * @brief Saves the current database snapshot
 * @param meta Pointer to the metadata structure
 * @return 0 on success, error code on failure
 */
int save_snap(meta_data * meta);

/**
 * @brief Initializes a fresh metadata structure
 * @param meta Pointer to the metadata structure to initialize
 */
void fresh_meta_load(meta_data * meta);

/**
 * @brief Loads metadata from files
 * @param file Path to the metadata file
 * @param bloom_file Path to the bloom filter file
 * @return Pointer to the loaded metadata structure
 */
meta_data * load_meta_data(char * file, char * bloom_file);

/**
 * @brief Dumps a list of SST files to a file
 * @param sst_files Pointer to the list of SST files
 * @param f File pointer to write to
 */
void dump_sst_list(list * sst_files, FILE * f);

/**
 * @brief Writes metadata to a file
 * @param file Path to the file
 * @param meta Pointer to the metadata structure
 * @param temp_buffer Temporary buffer for writing
 */
void write_md_d(char * file, meta_data * meta, byte_buffer * temp_buffer);

/**
 * @brief Frees a metadata structure
 * @param meta Pointer to the metadata structure to free
 */
void free_md(meta_data * meta);

/**
 * @brief Destroys metadata and associated files
 * @param file Path to the metadata file
 * @param bloom_file Path to the bloom filter file
 * @param meta Pointer to the metadata structure
 */
void destroy_meta_data(char * file, char * bloom_file, meta_data * meta);

#endif