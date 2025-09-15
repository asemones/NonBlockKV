#include <string.h>
#include <stdlib.h>
#include "../ds/byte_buffer.h"
#include "../ds/arena.h"
#include "alloc_util.h"
#include <uuid/uuid.h>

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



#endif