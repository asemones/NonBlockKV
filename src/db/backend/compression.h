#include <zstd.h>
#include <zdict.h>
#include "../../ds/byte_buffer.h"
#include "key-value.h"
#include "../../ds/list.h"
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#define MAX_SAMPLE 128
#pragma once
typedef struct block_index block_index;

/**
 * @brief Structure containing compression information for an SST file.
 * @struct sst_cmpr_inf
 * @param decompress_dict Zstandard decompression dictionary.
 * @param compression_dict Zstandard compression dictionary.
 * @param compr_context Zstandard compression context.
 * @param decompr_context Zstandard decompression context.
 * @param dict_buffer Buffer for the dictionary.
 */
typedef struct sst_cmpr_inf{
    ZSTD_DDict* decompress_dict;
    ZSTD_CDict* compression_dict;
    ZSTD_CCtx * compr_context;
    ZSTD_DCtx * decompr_context;
    void * dict_buffer;
    size_t dict_offset;
    size_t dict_len;
} sst_cmpr_inf;

/**
 * @brief Structure containing information for compression thread.
 * @struct compress_tr_inf
 * @param buffer Byte buffer for compression.
 * @param sizes List of sizes of compressed data.
 * @param curr_size Current size of compressed data.
 * @param sst Pointer to the SST compression information.
 */
typedef struct compress_tr_inf{
    byte_buffer * buffer;
    list * sizes;
    int curr_size;
    sst_cmpr_inf * sst;
}compress_tr_inf;

/**
 * @brief Initializes a compression thread information structure.
 * @param inf Pointer to the compression thread information structure.
 * @param buffer Byte buffer for compression.
 * @param sst Pointer to the SST compression information.
 */
void init_compress_tr_inf(compress_tr_inf * inf, byte_buffer * buffer, sst_cmpr_inf * sst);

/**
 * @brief Adds a key-value pair to the dictionary buffer.
 * @param inf Pointer to the compression thread information structure.
 * @param key Key of the key-value pair.
 * @param value Value of the key-value pair.
 */
void into_dict_buffer(compress_tr_inf * inf, f_str key, f_str value);

/**
 * @brief Samples data for dictionary creation.
 * @param sst Pointer to the SST compression information.
 * @param sst_size Size of the SST file.
 * @param compression_level Compression level.
 * @param key_val_sample Buffer to store key-value samples.
 * @param entry_lens Array to store lengths of entries.
 * @param num_entries Number of entries.
 * @return 0 on success, error code on failure.
 */
int sample_for_dict(sst_cmpr_inf * sst, int sst_size, int compression_level, void * key_val_sample, size_t * entry_lens, size_t num_entries);

/**
 * @brief Prepares for dictionary compression.
 * @param inf Pointer to the compression thread information structure.
 * @param blocks List of blocks.
 * @param input_buffer Byte buffer for the input data.
 */
void prep_dict_compression(compress_tr_inf * inf, list * blocks, byte_buffer * input_buffer);

/**
 * @brief Fills the compression and decompression dictionaries.
 * @param inf Pointer to the compression thread information structure.
 * @param sst_size Size of the SST file.
 * @param blocks List of blocks.
 * @param input_buffer Byte buffer for the input data.
 * @return 0 on success, error code on failure.
 */
int fill_dicts(compress_tr_inf * inf, int sst_size,  list * blocks, byte_buffer * input_buffer);

/**
 * @brief Compresses data using dictionary compression.
 * @param sst Pointer to the SST compression information.
 * @param sst_stream Void pointer to the SST stream (must be a byte_buffer).
 * @param dest Void pointer to the destination (must be a byte_buffer).
 * @return 0 on success, error code on failure.
 */
int compress_dict(sst_cmpr_inf * sst, byte_buffer * sst_stream, byte_buffer * dest, int size);

/**
 * @brief Decompresses data using dictionary decompression.
 * @param sst Pointer to the SST compression information.
 * @param sst_stream Void pointer to the SST stream (must be a byte_buffer).
 * @param dest Void pointer to the destination (must be a byte_buffer).
 * @return 0 on success, error code on failure.
 */
int decompress_dict(sst_cmpr_inf  * sst, byte_buffer * sst_stream, byte_buffer * dest,  int size);

/**
 * @brief Decompresses data using regular decompression.
 * @param sst Pointer to the SST compression information.
 * @param sst_stream Void pointer to the SST stream (must be a byte_buffer).
 * @param dest Void pointer to the destination (must be a byte_buffer).
 * @return 0 on success, error code on failure.
 */
int regular_decompress(sst_cmpr_inf * sst, byte_buffer * sst_stream, byte_buffer * dest, int size);

/**
 * @brief Compresses data using regular compression.
 * @param sst Pointer to the SST compression information.
 * @param sst_stream Void pointer to the SST stream (must be a byte_buffer).
 * @param dest Void pointer to the destination (must be a byte_buffer).
 * @return 0 on success, error code on failure.
 */
int regular_compress(sst_cmpr_inf * sst, byte_buffer * sst_stream, byte_buffer * dest,  int size);

/**
 * @brief Initializes SST compression information.
 * @param sst Pointer to the SST compression information.
 * @param dict_buffer Buffer for the dictionary.
 */
void init_sst_compr_inf(sst_cmpr_inf * sst, void * dict_buffer);

/**
 * @brief Sets the dictionary buffer for SST compression information.
 * @param sst Pointer to the SST compression information.
 * @param dict_buffer Buffer for the dictionary.
 */;
void set_dict_buffer(sst_cmpr_inf * sst, void * dict_buffer);

/**
 * @brief Frees SST compression information.
 * @param sst Pointer to the SST compression information to free.
 */
void free_sst_cmpr_inf(sst_cmpr_inf * sst);

/* Forward declaration of sst_f_inf structure */
typedef struct sst_file_info sst_f_inf;

/**
 * @brief Handles compression based on whether dictionary compression should be used.
 *        Also handles dictionary creation if needed.
 * @param inf Pointer to SST file information.
 * @param input_buffer Input buffer.
 * @param output_buffer Output buffer.
 * @param dict_buffer Dictionary buffer. the contents of the dictionary will be stored here.
 * @param blocks List of blocks.
 * @return 0 on success, error code on failure.
 */
int handle_compression(sst_f_inf* inf, byte_buffer* input_buffer, byte_buffer* output_buffer, byte_buffer* dict_buffer);

/**
 * @brief Handles decompression based on whether dictionary compression was used.
 * @param inf Pointer to SST file information.
 * @param input_buffer Input buffer.
 * @param output_buffer Output buffer.
 * @return 0 on success, error code on failure.
 */
int handle_decompression(sst_f_inf* inf, byte_buffer* input_buffer, byte_buffer* output_buffer);
int load_dicts(sst_cmpr_inf * sst, int compression_level, void * buffer, FILE * file);
static inline bool check_for_lg_pg(int curr_pg_size, int len, int max_pg_size){
    return (curr_pg_size + len  > max_pg_size);
}
