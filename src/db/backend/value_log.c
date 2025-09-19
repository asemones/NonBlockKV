
/*format for reading:
we will need a fileno, a size, an off for some types
for larger ones, we do not need an off. thus, off may be 4 bytes
fileno might need to be 8 bytes for smaller, 4 bytes for larger
len will need to be 8 bytes for larger, 4 for smaller
so max = 8 + 4 + 4;
= 16
we only have 14 bytes of memory space
so how can this be reduced?
*/
#include "value_log.h"
LPH_DEFINE(v_log_tbl, v_log_file);
#define ERROR 0
#define V_LOG_EXT "vlg"
static void create_fn(char * fn, uint64_t no){
    gen_file_name(fn, no,V_LOG_EXT, 3);
}
static void seralize_v_log_file_info(const value_log * log, byte_buffer * write_buffer){
    
}
static int read_v_log_info(){
    return 0;
}
int v_log_update(value_log * v){
    return 0;
}
value_log create_v_log(uint64_t fs){
    value_log v;
    new_value(&v.counter);
    v.med.file_size = fs;
    v.med.curr = select_buffer(fs);
    read_v_log_info();
    return v;
}
static byte_buffer * v_log_med_read(const value_ptr p, bool async,  byte_buffer * storage){
    char fn[16];
    create_fn(fn, p.file_no);
    db_FILE * f = dbio_open(fn, 'r');
    set_context_buffer(f, storage);
    int32_t err=  dbio_read(f,p.off, p.len);
    if (err < 0 ) return NULL;
    storage->curr_bytes += err;
    return storage;
}

byte_buffer * v_log_get_value(f_str large_v, bool async){
    value_ptr * ptr = large_v.entry;
    if (large_v.len & MEDIUM_MASK){
        uint64_t size = ptr->len;
        byte_buffer * b = select_buffer(size);
        return v_log_med_read(*ptr, async, b);

    }
    else {
        return NULL;
    }
}   
value_ptr format_med_inline_v(f_str * value, uint64_t file_counter, uint32_t off){
    value_ptr ptr;
    ptr.file_no = file_counter;
    ptr.len = value->len;
    ptr.off = off;
    return ptr;
}
void seed_counter(counter_t *ctr) {
    uint64_t now = (uint64_t)time(NULL) & TS_MASK;
    ctr->bits.seconds = now;
    ctr->bits.count   = 0;
}
static uint64_t curr_value(counter_t * ctr){
    return ctr->raw;
}
static value_ptr v_log_add(f_str * value, byte_buffer * file_buffer, uint64_t counter){
    uint64_t loc= file_buffer->curr_bytes;
    write_buffer(file_buffer, &value->len, sizeof(value->len));
    write_buffer(file_buffer, value->entry, value->len);
    value_ptr ptr = format_med_inline_v(value, counter, loc );
    value->len = MEDIUM_MASK;
    return ptr;
}
static void flush_v_tbl_buff(value_log * v){
    uint64_t temp = curr_value(&v->counter);
    char fn [16];
    create_fn(fn, temp);
    new_value(&v->counter);
    byte_buffer * temp_b = v->med.curr;
    v->med.curr= select_buffer(v->med.file_size);
    db_FILE * file = dbio_open(fn, 'r');
    set_context_buffer(file, temp_b);
    v_log_file f;
    f.deleted_b=  0 ;
    f.total_b = temp_b->curr_bytes;
    v_log_tbl_put(&v->med.files, temp, f ,NULL);
    v_log_update(v);
    dbio_write(file, 0, temp_b->curr_bytes);
    dbio_close(file);
    return_buffer(temp_b);
}
static value_ptr v_log_med_write(value_log * v, f_str key, f_str value){
    while(value.len+ v->med.curr->curr_bytes > v->med.file_size){
        flush_v_tbl_buff(v);
    }
    return v_log_add(&value, v->med.curr, v->counter.raw);

}
value_ptr v_log_write(value_log * v, f_str key, f_str value){
    value_ptr formatted;
    if ( value.len < v->medium_thresh){
        formatted= v_log_med_write(v, key, value);
    }
    return formatted;
}
int v_log_delete(uint64_t file_id, v_log_tbl * tbl, uint64_t val_len){
    v_log_file * file = NULL;
    if (!v_log_tbl_get(tbl, file_id, file)) return -1;
    file->deleted_b += val_len;
    v_log_tbl_put(tbl, file_id, *file, NULL);
    v_log_update(tbl);
    return 0;

}
