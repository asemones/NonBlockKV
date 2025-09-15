#ifndef WAL_H
#define WAL_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h> // For bool type
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "option.h"
#include "../../ds/byte_buffer.h"
#include "../../util/io.h"
#include "../../util/io_types.h"
#include "key-value.h"
#include "../../util/error.h"
#include "../../ds/structure_pool.h" 
#include "../../util/multitask_primitives.h"
#include "../../util/stringutil.h"

#define NUM_WAL_SEGMENTS 3 // Example: Number of WAL segments
#define WAL_SEGMENT_SIZE (GLOB_OPTS.WAL_SIZE) // Use existing option for size per segment
#define MAX_WAL_SEGMENT_FN_LEN 32 // Max length for segment filenames like "WAL_SEG_0.bin"

typedef struct WAL_segment {
    char filename[MAX_WAL_SEGMENT_FN_LEN]; 
    size_t current_size;                 
    bool active;                          
    db_FILE * model;         
} WAL_segment;

/**
 * @brief Manages the set of WAL segments.
 */
typedef struct WAL_segments_manager {
    WAL_segment segments[NUM_WAL_SEGMENTS]; // Array of WAL segments
    int current_segment_idx;                // Index of the currently active segment (0 to NUM_WAL_SEGMENTS-1)
    size_t segment_capacity;                // Max size for each segment (WAL_SEGMENT_SIZE)
    int num_segments;
} WAL_segments_manager;

typedef struct WAL {
    WAL_segments_manager segments_manager;
    struct db_FILE *meta_ctx;
    byte_buffer *wal_buffer;
    size_t total_len;
    cascade_cond_var_t var;
    bool rotating;
    int flush_cadence;  
} WAL;

WAL* init_WAL(byte_buffer *b); // Signature kept as original

int write_WAL(WAL *w, f_str key, f_str value); // Signature kept as original

void kill_WAL(WAL *w);// Signature kept as original

void serialize_wal_metadata(WAL *w, byte_buffer *b);

bool deserialize_wal_metadata(WAL *w, byte_buffer *b);

int rotate_wal_segment(WAL *w);


int flush_wal_buffer(WAL *w, f_str k, f_str v);

void get_wal_fn(char* buf, int idx);
#endif // WAL_H
