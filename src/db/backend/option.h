
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#pragma  once

/**
 * @brief Configuration options for the database
 * @struct option
 * @param SST_TABLE_SIZE Size of SST tables in bytes
 * @param MEM_TABLE_SIZE Size of memory tables in bytes
 * @param BLOCK_INDEX_SIZE Size of block indices in bytes
 * @param LEVEL_0_SIZE Size of level 0 in bytes
 * @param LEVEL_SCALAR Scaling factor for level sizes
 * @param NUM_COMPACTOR_UNITS Number of compaction units
 * @param READ_WORKERS Number of read worker threads
 * @param WRITE_WORKERS Number of write worker threads
 * @param LRU_CACHE_SIZE Size of LRU cache in bytes
 * @param WAL_BUFFERING_SIZE Size of WAL buffer in bytes
 * @param SST_TABLE_SIZE_SCALAR Scaling factor for SST table size
 * @param WAL Flag to enable/disable Write-Ahead Logging
 * @param WAL_SIZE Size of WAL in bytes
 * @param WAL_M_F_N WAL metadata file name
 * @param META_F_N Metadata file name
 * @param BLOOM_F_N Bloom filter file name
 * @param MAX_WAL_FILES Maximum number of WAL files
 * @param NUM_FILES_COMPACT_ZER0 Number of files to compact at level 0
 * @param dict_size_ratio Ratio of file size to dictionary size for compression
 * @param compress_level Compression level. set this to less than -5 to disable compression completely
 * @param num_cache Number of cache shards
 */
typedef struct option{
    size_t SST_TABLE_SIZE;
    size_t MEM_TABLE_SIZE;
    size_t BLOCK_INDEX_SIZE;
    size_t LEVEL_0_SIZE;
    double LEVEL_SCALAR;
    size_t NUM_COMPACTOR_UNITS;
    size_t READ_WORKERS;
    size_t WRITE_WORKERS;
    size_t LRU_CACHE_SIZE;
    size_t WAL_BUFFERING_SIZE;
    double SST_TABLE_SIZE_SCALAR;
    bool WAL;
    size_t WAL_SIZE;
    char * WAL_M_F_N;
    char * META_F_N;
    char * BLOOM_F_N;
    size_t MAX_WAL_FILES;
    size_t NUM_FILES_COMPACT_ZER0;
    int dict_size_ratio; /*sets the ratio of file size to dict size, will determine real dictionary size. set to 0 for no dict */
    int compress_level;
    int num_cache;
    int num_memtable;
    int num_to_flush;
    int index_cache_mem;
    int bits_per_key;
    int partition_size; /* in terms of block index data*/
    long value_log_threshold;
    long v_l_large_threshold;
}option;

/**
 * @brief Global options instance
 */
extern option GLOB_OPTS;

/**
 * @brief Sets default configuration options for production use
 * @param opt Pointer to the options structure to initialize
 */
static inline void set_defaults(option * opt){
    opt->SST_TABLE_SIZE = 64 * 1024 * 1024;
    opt->MEM_TABLE_SIZE = 64 * 1024 * 1024;
    opt->SST_TABLE_SIZE_SCALAR = 1.5;
    opt->BLOCK_INDEX_SIZE = 4 * 1024;
    opt->LEVEL_0_SIZE = 256 * 1024 * 1024;
    opt->LEVEL_SCALAR =2;
    opt->NUM_COMPACTOR_UNITS = 3;
    opt->READ_WORKERS = 3;
    opt->WRITE_WORKERS = 3;
    opt->WAL_BUFFERING_SIZE =4096;
    opt->LRU_CACHE_SIZE = 32 * 1024 * 1024;
    opt->WAL =true;
    opt->WAL_M_F_N = "WAL_M.bin";
    opt->MAX_WAL_FILES = 5;
    opt->WAL_SIZE = opt->MEM_TABLE_SIZE;
    opt->META_F_N = "meta.bin";
    opt->BLOOM_F_N = "bloom.bin";
    opt->NUM_FILES_COMPACT_ZER0 = 5;
    opt->num_cache = 128;
    opt->num_memtable =4;
    opt->num_to_flush = 1;
    opt->index_cache_mem = 1024 * 1024 * 64;
    opt->bits_per_key = 10;
    opt->partition_size = 16384;
    opt->value_log_threshold = 512 ;
    opt->v_l_large_threshold = 1024 * 128;

}
static inline void set_debug_defaults(option * opt){
    opt->SST_TABLE_SIZE =  256* 1024;
    opt->MEM_TABLE_SIZE = 512 * 1024;
    opt->SST_TABLE_SIZE_SCALAR = 1;
    opt->BLOCK_INDEX_SIZE = 4 * 1024;
    opt->LEVEL_0_SIZE = 400 * 1024;
    opt->LEVEL_SCALAR = 2;
    opt->NUM_COMPACTOR_UNITS = 3;
    opt->READ_WORKERS = 3;
    opt->WRITE_WORKERS = 3;
    opt->LRU_CACHE_SIZE =1024 * 1024;
    opt->WAL_BUFFERING_SIZE = 4096;
    opt->WAL = true;
    opt->WAL_M_F_N = "WAL_M.bin";
    opt->META_F_N = "meta.bin";
    opt->BLOOM_F_N = "bloom.bin";
    opt->MAX_WAL_FILES = 2;
    opt->WAL_SIZE = opt->MEM_TABLE_SIZE;
    opt->NUM_FILES_COMPACT_ZER0 = 2;
    opt->dict_size_ratio = 100;
    opt->compress_level =-10;
    opt->num_cache = 8;
    opt->num_memtable =7;
    opt->num_to_flush = 1;
    opt->index_cache_mem = 1024 * 1024;
    opt->bits_per_key = 10;
    opt->partition_size = 16384;
    opt->value_log_threshold = 512;
    opt->v_l_large_threshold = 1024 * 64;
}


