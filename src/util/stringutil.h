#include <string.h>
#include <stdlib.h>
#include "../ds/byte_buffer.h"
#include "../ds/arena.h"
#include "alloc_util.h"
#include <uuid/uuid.h>
#include "mono_counter.h"

#ifndef STRINGUTIL_H
#define STRINGUTIL_H


/** Function to find all occurrences of a target string within a source string
* @param source_string the source string to search within
* @param target_string the target string to search for
* @param num_occurrences an integer for the desired (maxiumum) number of occurrences
* @return an array of pointers to the occurrences of the target string within the source string
*/


static inline void gen_sst_fname(size_t id, size_t level, char * buffer){
    snprintf(buffer, 100, "sst_%zu_%zu", id, level);
}
static inline void grab_time_char(char *buffer) {

    struct timeval tv;
    gettimeofday(&tv, NULL);

    struct tm tmval;
    localtime_r(&tv.tv_sec, &tmval);
    strftime(buffer, 20, "%Y-%m-%d %H:%M:%S", &tmval);

    snprintf(buffer + strlen(buffer), 10, ".%06ld", (long)tv.tv_usec);
}
static inline void grab_uuid(char * buffer){
  uuid_t b;
  uuid_generate(b);
  uuid_unparse_lower(b, buffer);
}

static const char B64URL[64] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static inline void u64_to_be(uint64_t x, uint8_t out[8]) {
    for (int i = 7; i >= 0; --i) { out[i] = (uint8_t)(x & 0xFFu); x >>= 8; }
}

static inline void enc3(const uint8_t *src, char *dst) {
    uint32_t v = ((uint32_t)src[0] << 16) | ((uint32_t)src[1] << 8) | src[2];
    dst[0] = B64URL[(v >> 18) & 63];
    dst[1] = B64URL[(v >> 12) & 63];
    dst[2] = B64URL[(v >> 6)  & 63];
    dst[3] = B64URL[v & 63];
}

static inline void enc2(const uint8_t *src, char *dst) {
    uint32_t v = ((uint32_t)src[0] << 16) | ((uint32_t)src[1] << 8);
    dst[0] = B64URL[(v >> 18) & 63];
    dst[1] = B64URL[(v >> 12) & 63];
    dst[2] = B64URL[(v >> 6)  & 63];
}

static inline void b64url_u64(char out[11], uint64_t id) {
    uint8_t bytes[8];
    u64_to_be(id, bytes);

    enc3(&bytes[0], &out[0]); 
    enc3(&bytes[3], &out[4]);
    enc2(&bytes[6], &out[8]);
}
#define CTR_FN_LEN 16
static inline void gen_file_name(char * out, uint64_t id, const char * ext, uint64_t ext_len){
    b64url_u64(out, id);
    const int b64url_len= 11;
    out[b64url_len] = '.';

    const char * definite_end = &out[b64url_len] + 1;
    memcpy(definite_end, ext, ext_len );
    out[b64url_len+ 1 + ext_len]= '\0';
}





#endif