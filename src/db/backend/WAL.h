#ifndef WAL_H
#define WAL_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h> 
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

#define NUM_WAL_SEGMENTS 3 
#define WAL_SEGMENT_SIZE (GLOB_OPTS.WAL_SIZE) 
#define MAX_WAL_SEGMENT_FN_LEN 32 

typedef struct WAL_segment {
    char filename[MAX_WAL_SEGMENT_FN_LEN]; 
    size_t current_size;                 
    bool active;                          
    db_FILE * model;         
} WAL_segment;


typedef struct WAL_segments_manager {
    WAL_segment segments[NUM_WAL_SEGMENTS];
    int current_segment_idx;                
    size_t segment_capacity;               
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

WAL* init_WAL(byte_buffer *b); 

int write_WAL(WAL *w, f_str key, f_str value);

void kill_WAL(WAL *w);


#endif 
