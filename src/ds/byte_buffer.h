#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include "../util/alloc_util.h"
#include <fcntl.h>
#include <unistd.h>
#include "../util/maths.h"
#ifndef BYTE_BUFFER_H
#define BYTE_BUFFER_H

/**
 * @brief Structure representing a byte buffer.
 * @struct byte_buffer
 * @param buffy Pointer to the buffer data.
 * @param utility_ptr Pointer for utility purposes (e.g., tracking offsets).
 * @param max_bytes Maximum size of the buffer in bytes.
 * @param curr_bytes Current number of bytes used in the buffer.
 * @param read_pointer Current read position in the buffer.
 */
typedef struct byte_buffer {
    char * buffy;
    size_t id; /*to register buffers with the kernel*/
    size_t max_bytes;
    size_t curr_bytes;
    size_t read_pointer;
} byte_buffer;

/**
 * @brief Creates a new byte buffer.
 * @param size Initial size of the buffer.
 * @return Pointer to the newly created byte buffer.
 */
byte_buffer * create_buffer(size_t size);

/**
 * @brief Creates a new memory-aligned byte buffer.
 * @param size Size of the buffer.
 * @param alligment Alignment requirement.
 * @return Pointer to the newly created memory-aligned byte buffer.
 */
byte_buffer * mem_aligned_buffer(size_t size, size_t alligment);

/**
 * @brief Resizes the byte buffer.
 * @param b Pointer to the byte buffer.
 * @param min_size Minimum required size.
 * @return 0 on success, error code on failure.
 */
int buffer_resize(byte_buffer * b, size_t min_size);

/**
 * @brief Writes data to the byte buffer.
 * @param buffer Pointer to the byte buffer.
 * @param data Pointer to the data to write.
 * @param size Size of the data to write.
 * @return 0 on success, error code on failure.
 */
int write_buffer(byte_buffer * buffer, void * data, size_t size);

/**
 * @brief Reads data from the byte buffer.
 * @param buffer Pointer to the byte buffer.
 * @param dest Destination buffer to read data into.
 * @param len Number of bytes to read.
 * @return 0 on success, error code on failure.
 */
int read_buffer(byte_buffer * buffer, void * dest, size_t len);

/**
 * @brief Dumps the contents of the byte buffer to a file.
 * @param buffer Pointer to the byte buffer.
 * @param file Pointer to the file to write to.
 * @return 0 on success, error code on failure.
 */
int dump_buffy(byte_buffer * buffer, FILE * file);

/**
 * @brief Retrieves a pointer to a specific index in the buffer.
 * @param b Pointer to the byte buffer.
 * @param size Offset from the beginning of the buffer.
 * @return Pointer to the specified index.
 */
char * buf_ind(byte_buffer * b, int size);

/**
 * @brief Finds the nearest occurrence of a character forward from the current read pointer.
 * @param buffer Pointer to the byte buffer.
 * @param c Character to search for.
 * @return Pointer to the found character or NULL if not found.
 */
char * go_nearest_v(byte_buffer * buffer, char c);

/**
 * @brief Finds the nearest occurrence of a character backward from the current read pointer.
 * @param buffer Pointer to the byte buffer.
 * @param c Character to search for.
 * @return Pointer to the found character or NULL if not found.
 */
char * go_nearest_backwards(byte_buffer * buffer, char c);

/**
 * @brief Gets the character at the current read pointer.
 * @param buffer Pointer to the byte buffer.
 * @return Character at the current read pointer.
 */
char * get_curr(byte_buffer * buffer);

/**
 * @brief Gets the character at the next read position.
 * @param buffer Pointer to the byte buffer.
 * @return Character at the next read position.
 */
char * get_next(byte_buffer * buffer);

/**
 * @brief Transfers data from one byte buffer to another.
 * @param read_me Pointer to the source byte buffer.
 * @param write_me Pointer to the destination byte buffer.
 * @param num Number of bytes to transfer.
 * @return 0 on success, error code on failure.
 */
int byte_buffer_transfer(byte_buffer * read_me, byte_buffer * write_me, size_t num);

/**
 * @brief Resets the byte buffer (clears content and resets pointers).
 * @param v_buffer Pointer to the byte buffer.
 */
void reset_buffer(void * v_buffer);

/**
 * @brief Frees the memory allocated for the byte buffer.
 * @param v_buffer Pointer to the byte buffer.
 */
void free_buffer(void * v_buffer);
static inline void b_seek(byte_buffer * buff, int pos){
    buff->read_pointer = pos;
}
static inline void padd_buffer(byte_buffer * buffer, int to_pad){
    buffer->curr_bytes += to_pad;
}
static inline uint32_t read_int32(byte_buffer *buffer){
    uint32_t dest = 0;
    memcpy(&dest, buffer->buffy + buffer->read_pointer, sizeof(int32_t));
    buffer->read_pointer += sizeof(int32_t);
    return dest;
}
static inline uint64_t read_int64(byte_buffer *buffer){
    uint64_t dest = 0;
    memcpy(&dest, buffer->buffy + buffer->read_pointer, sizeof(uint64_t));
    buffer->read_pointer += sizeof(uint64_t);
    return dest;
}
static inline uint8_t read_byte(byte_buffer *buffer){
    uint8_t dest = 0;
    memcpy(&dest, buffer->buffy + buffer->read_pointer, sizeof(uint8_t));
    buffer->read_pointer += sizeof(uint8_t);
    return dest;
}
static inline int write_int64(byte_buffer *buffer, const uint64_t src){
    memcpy(buffer->buffy + buffer->curr_bytes, &src, sizeof(uint64_t));
    buffer->curr_bytes += sizeof(uint64_t);
    return (int)sizeof(uint64_t);
}
static inline int write_int32(byte_buffer *buffer, const uint32_t src){
    memcpy(buffer->buffy + buffer->curr_bytes, &src, sizeof(uint32_t));
    buffer->curr_bytes += sizeof(uint32_t);
    return (int)sizeof(uint32_t);
}
static inline uint16_t read_int16(byte_buffer *buffer){
    uint16_t dest = 0;
    memcpy(&dest, buffer->buffy + buffer->read_pointer, sizeof(int16_t));
    buffer->read_pointer += sizeof(int16_t);
    return dest;
}
static inline int write_int16(byte_buffer *buffer, const uint16_t src){
    memcpy(buffer->buffy + buffer->curr_bytes, &src, sizeof(uint16_t));
    buffer->curr_bytes += sizeof(uint16_t);
    return (int)sizeof(uint16_t);
}
static inline int write_byte(byte_buffer *buffer, const uint8_t src){
    memcpy(buffer->buffy + buffer->curr_bytes, &src, sizeof(uint16_t));
    buffer->curr_bytes += sizeof(uint16_t);
    return (int)sizeof(uint16_t);
}
byte_buffer * create_empty_buffer();
#endif

