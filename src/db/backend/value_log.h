#ifndef VALUE_LOG_H
#define VALUE_LOG_H
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
#include "../../util/io_types.h"
#include "../../util/io.h"
#include "key-value.h"
#include "../../util/error.h"
#include "../../ds/structure_pool.h" 
#include "../../util/stringutil.h"
#include "WAL.h"
#include "../../ds/skiplist.h"
#include "../../ds/circq.h"
#include "../../util/mono_counter.h"
#define VALUE_LEN 16;
#define MEDIUM_MASK 44444
#define LARGE_MASK 55555
/*value log info
user adjustable setting for key-value threshold. the point of this log is to improve lookups with keys while
reducing write amppliifciation. To reduce write amp at a cost of space amp, we may preform ORDERED value log merges
only at the bottom layer(s) of the tree, and keep these ordered v logs segemented 


size tiers:
medium: an entire vtree approach or just a basic mapping
vtree approach:
Global datastructure? i dont think we need to store all of vtable info 
Instead, we may iteratively prepare during flush 
actually we do need some info, namely 
we can just deal with this later 
we are going the ordered route
large: write out to own file, just delete file on delete
consider some sort of sst to blob file map and keep the blobs ordered
*/
typedef struct v_log_file{
    uint64_t deleted_b;
    uint64_t total_b;
}v_log_file;
LPH_DECLARE(v_log_tbl,v_log_file );
DEFINE_CIRCULAR_QUEUE(v_log_file, v_log_gc_q);
typedef struct medium_strategy{
    uint64_t file_size;
    uint64_t seg_size;
    uint64_t delete_thresh;
    byte_buffer * curr;
    v_log_tbl files;
    v_log_gc_q gc_q;
} medium_strategy;
typedef struct value_ptr{
    uint32_t len;
    uint32_t off;
    uint64_t file_no;
}value_ptr;
typedef struct large_strategy{
    v_log_tbl files;
}large_strategy;

typedef struct value_log{
    uint64_t large_thresh;
    uint64_t medium_thresh;
    counter_t counter;
    medium_strategy med;
    large_strategy large;

}value_log;

void init_v_log(value_log * log);
value_log create_v_log(uint64_t fs);
byte_buffer * v_log_get_value(f_str large_v, bool async);

void * get_value(value_log v, f_str key, f_str inline_v);
void clean();
void shutdown_v_log(value_log * log);

#endif

