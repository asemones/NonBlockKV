#ifndef MANIFEST_H
#define MANIFEST_H

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
#include "sst_manager.h"
#include "indexer.h"

#define NUM_MD_SEG 2
#define MAX_WAL_SEGMENT_FN_LEN 32 

typedef struct manifest_segment {
    char filename[MAX_WAL_SEGMENT_FN_LEN]; 
    size_t current_size;                 
    bool active;                          
    db_FILE * model;         
} manifest_segment;


typedef struct  manifest_seg_mgr {
    manifest_segment segments[NUM_MD_SEG];
    int current_segment_idx;                
    size_t segment_capacity;               
    int num_segments;
} manifest_seg_mgr;

typedef struct manifest {
    manifest_seg_mgr segments_manager;
    struct db_FILE *meta_ctx;
    byte_buffer *manifest_buffer;
    size_t total_len;
    bool rotating;
    int flush_cadence;
    uint64_t snapshot_ptr; 
    counter_t ctr; 
} manifest;
typedef uint8_t manifest_cmd;
enum m {
    KILL_LOG,
    FILE_ADD,
    FILE_DELTE,
    MD_COMMIT,
    MD_FLUSH,
    REPLAY,
    RECOVER
};

manifest* init_manifest(byte_buffer *b, uint64_t seg_cap); 
int mainfest_flush_block(manifest * m);
void kill_manifest(manifest *w);
void delete_manifest(manifest * m);

#endif 