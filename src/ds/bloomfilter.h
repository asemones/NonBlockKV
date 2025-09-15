#include <stdint.h>
#include "hashtbl.h"
#include "../util/io.h"
#include "byte_buffer.h"
#include "../util/alloc_util.h"
#include "slab.h"
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

/**
 * @brief Structure representing a bit array.
 * @struct bit_array
 * @param array Pointer to the array of 64-bit unsigned integers.
 * @param size Size of the bit array.
 */
typedef struct bit_array{
    uint64_t * array;
    size_t size;
}bit_array;

/**
 * @brief Structure representing a Bloom filter.
 * @struct bloom_filter
 * @param ba Pointer to the bit array.
 * @param num_hash Number of hash functions.
 */
typedef struct bloom_filter{
    bit_array* ba;
    size_t num_hash;

}bloom_filter;

/**
 * @brief MurmurHash3 32-bit hash function.
 * @param key Pointer to the key to hash.
 * @param len Length of the key.
 * @param seed Seed for the hash function.
 * @return 32-bit hash value.
 */
uint32_t MurmurHash3_x86_32(void *key, int len, uint32_t seed);

/**
 * @brief Creates a new bit array.
 * @param size Size of the bit array.
 * @return Pointer to the newly created bit array.
 */
bit_array* create_bit_array(size_t size);

/**
 * @brief Sets a bit at a given index in the bit array.
 * @param ba Pointer to the bit array.
 * @param index Index of the bit to set.
 */
void set_bit(bit_array *ba, size_t index);

/**
 * @brief Clears a bit at a given index in the bit array.
 * @param ba Pointer to the bit array.
 * @param index Index of the bit to clear.
 */
void clear_bit(bit_array *ba, size_t index);

/**
 * @brief Gets the value of a bit at a given index in the bit array.
 * @param ba Pointer to the bit array.
 * @param index Index of the bit to get.
 * @return true if the bit is set, false otherwise.
 */
bool get_bit(bit_array *ba, size_t index);

/**
 * @brief Frees the memory allocated for the bit array.
 * @param ba Pointer to the bit array to free.
 */
void free_bit(bit_array *ba);

/**
 * @brief Creates a new Bloom filter.
 * @param num_hash Number of hash functions.
 * @param num_word Number of words in the bit array.
 * @param read_file Flag indicating whether to read from a file.
 * @param file File path (if reading from a file).
 * @return Pointer to the newly created Bloom filter.
 */
bloom_filter* bloom(size_t num_hash, size_t num_word,bool read_file, char * file);

/**
 * @brief Maps a URL to the Bloom filter.
 * @param url URL to map.
 * @param filter Pointer to the Bloom filter.
 */
void map_bit(const char * url, bloom_filter *filter);

/**
 * @brief Checks if a URL is possibly in the Bloom filter.
 * @param url URL to check.
 * @param filter Pointer to the Bloom filter.
 * @return true if the URL is possibly in the filter, false otherwise.
 */
bool check_bit(const char * url, bloom_filter *filter);

/**
 * @brief Frees the memory allocated for the Bloom filter.
 * @param filter Pointer to the Bloom filter to free.
 */
void free_filter( void *filter);

/**
 * @brief Dumps the Bloom filter to a file.
 * @param filter Pointer to the Bloom filter.
 * @param file File path to dump the filter to.
 */
void dump_filter(bloom_filter *filter, char * file);

/**
 * @brief Creates a Bloom filter from a byte stream.
 * @param buffy_the_buffer Byte buffer containing the Bloom filter data.
 * @return Pointer to the created Bloom filter.
 */
bloom_filter * from_stream(byte_buffer * buffy_the_buffer);

/**
 * @brief Copies a Bloom filter to a byte buffer.
 * @param filter Pointer to the Bloom filter.
 * @param buffer Byte buffer to copy the filter to.
 */
void copy_filter(bloom_filter * filter, byte_buffer* buffer);

/**
 * @brief Creates a deep copy of a Bloom filter.
 * @param original Pointer to the original Bloom filter.
 * @return Pointer to the newly created deep copy.
 */
bloom_filter* deep_copy_bloom_filter(const bloom_filter* original);
void reset_filter(bloom_filter * f);
bloom_filter  part_from_stream(byte_buffer * buffy_the_buffer, slab_allocator * allocator);
#endif

