#include "manifest.h"
#define MANIFEST_BCKUP_EXT "mbe"



typedef struct manifest_record{
    void * data_src;
    manifest_cmd type;
    uint16_t len;
} manifest_record;
typedef struct manifest_commit_inf{
    sst_manager * sst_man;
}manifest_commit_inf;
static int m_rc_size(const manifest_record r){
    return r.len + sizeof(r.len) + sizeof(r.type) + sizeof(uint32_t); //checksum
}
static inline int m_rc_hdr_s(){
    return 7;
}
static inline manifest_record make_record(const manifest_cmd cmd, void * data, uint16_t d_len){
   manifest_record record;
   record.data_src = data;
   record.len =  d_len;
   record.type = cmd;
   return record;
}
static inline int writ_record_hdr(byte_buffer * b, const manifest_record r){
    int len  =0;
    write_byte(b, r.type);
    write_int16(b, r.len);
    len += m_rc_hdr_s();
    return len;
}

static int flush_atomic_add(manifest *w, const manifest_record r);
static uint64_t add_to_buffer_generic(byte_buffer * b, const manifest_record r);
static int rotate_manifest_segment(manifest * w);
static int commit_buffer(manifest *w);
/*load and parse manifest.
this is just a fat switch to apply all of the change*/
static bool manifest_parse_record(manifest * w, byte_buffer * b, manifest_record * out){
    uint32_t check_sum = read_int32(b);
    uint8_t type = read_byte(b);
    uint16_t len = read_int16(b);
    uint8_t hdr_incl_len = sizeof(type) + sizeof(len);
    if (len + hdr_incl_len + b->read_pointer > b->max_bytes){
        /*obviously corrupted len field*/
        return false;
    }
    /*if len field is corrupted, checksum wont match*/
    if (!verify_data(buff_ind(b->read_pointer - hdr_incl_len), len + hdr_incl_len, check_sum)){
        return false;
    }
    out->data_src = get_curr(b);
    out->type = type;
    out->len = len;
    return true;
}
static void commit_m_rc_list(list * manifest_cmt_buffer, const  manifest_commit_inf m){
    manifest_record * m_arr = manifest_cmt_buffer->arr;
    for (int i = 0; i < manifest_cmt_buffer->len; i++){
        const manifest_record r = m_arr[i];
        switch (r.type){
            case FILE_ADD:
                sst_f_inf inf;

            case FILE_DELTE:
            default:

        }
    }
    clear_list(manifest_cmt_buffer);
}
static void act_on_record(byte_buffer * stream, list * manifest_cmt_buffer, const manifest_record curr, const manifest_commit_inf m){
    switch(curr.type){
        case MD_FLUSH:
            b_seek_next_align(stream, 4096);
        case MD_COMMIT:
            commit_m_rc_list(manifest_cmt_buffer, m);
            b_seek_next_align(stream, 4096);
        default:
            insert(manifest_cmt_buffer, &curr);
    }
}
static inline int get_b_swap_size(uint64_t base){
    return base - m_rc_hdr_s();
}
static inline int do_rotation_needed(){
    return 0;
}
static int refresh_md_wrt(manifest * w, byte_buffer * ceral){
    dbio_write(w->meta_ctx, 0, ceral->curr_bytes);
    dbio_fsync(w->meta_ctx);
    return 0;
}
static int produce_snapshot(sst_manager * mana, manifest * w){
    byte_buffer * b = select_buffer(get_md_size_lwr_bnd(mana->num_levels, mana->levels)); 
    byte_buffer * md_buffer= select_buffer(4096);
    seralize_sst_md_all(b, w);
    pad_nearest_x(b, 4096); // allign to 4kb pg
    w->snapshot_ptr = b->curr_bytes;
    serialize_manifest_metadata(w, md_buffer);
    

}
static int write_md_generic(manifest *w,const manifest_record r) {
    if (!w) return FAILED_TRANSCATION;

    int ret = 0;
    uint64_t data_size = m_rc_size(r);
    manifest_seg_mgr*mgr = &w->segments_manager;
    manifest_segment *current_segment = &mgr->segments[mgr->current_segment_idx]; 

 
    bool rotation_needed = (current_segment->current_size + data_size > get_b_swap_size(mgr->segment_capacity));
    bool values_written  = false;
    if (rotation_needed) {

        ret = rotate_manifest_segment(w);
        if (ret != 0) return FAILED_TRANSCATION;
        if (w->manifest_buffer && w->manifest_buffer->curr_bytes > 0) {
            ret = flush_atomic_add(w,r);
            if (ret < 0) {
                fprintf(stderr, "Error flushing manifest buffer after rotation\n");
                return FAILED_TRANSCATION;
            }
        }
        current_segment = &mgr->segments[mgr->current_segment_idx];
        values_written = true;
    }

    bool buffer_flush_needed = (w->manifest_buffer && (w->manifest_buffer->curr_bytes + data_size > w->flush_cadence));
    if (buffer_flush_needed) {
        ret = flush_atomic_add(w,r);
        if (ret < 0) {
            fprintf(stderr, "Error flushing manifest buffer before adding new data\n");
            return FAILED_TRANSCATION;
        }
        values_written = true;
    }

    if (!w->manifest_buffer) {
         fprintf(stderr, "CRITICAL: manifest buffer is NULL before writing\n");
         exit(EXIT_FAILURE);
    }
    if (values_written){
        return 0;
    }
    int written_to_buffer = 0;
    ret =  add_to_buffer_generic(w->manifest_buffer,r);
    if (ret < 0) {
        fprintf(stderr, "Error writing key to manifest buffer\n");
        return FAILED_TRANSCATION;
    }
    written_to_buffer += ret;

    w->total_len++;
    current_segment->current_size += written_to_buffer;
    return 0;
}

int man_f_add(manifest* w,  sst_f_inf * val){
    return write_md_generic(w, make_record(FILE_ADD, val,  sst_md_serialized_len(val)));
}
int man_f_del(manifest* w, sst_f_inf * val){
    return write_md_generic(w, make_record(FILE_DELTE, val,  sst_md_str(val)));
}
/*this will NEVER preform an atomic flush write. as such, we can safely call a buffer flush
without two disk fsyncs*/
int man_f_commit(manifest* w){
    write_md_generic(w, make_record(MD_COMMIT, NULL,0));
    return commit_buffer(w);
}
static uint64_t add_to_buffer_f_add(byte_buffer * b, sst_f_inf * in){
    seralize_sst_md_all(b, in);
    return 0;
}
static uint64_t add_to_buffer_f_delete(byte_buffer * b, sst_f_inf * in){
    write_sst_strs(b, in);
    return 0;
}
/*this is a strange format i did here out of lazyniess.
Earlier, the amount of data to be persisted is calcuilated and stored
in the f_str key len because i am lazy and didnt want to write a bunch of duplicate code.
is it a good idea? NO. but thats why this comment exists :/ */
static uint64_t add_to_buffer_generic(byte_buffer * b, const manifest_record r){
    uint64_t checksum_spot = reserve_checksum(b);
    writ_record_hdr(b,r );
    switch(r.type){
        case FILE_ADD:
            add_to_buffer_f_add(b,  (sst_f_inf*)r.data_src);
            break;
       
        case FILE_DELTE:
            add_to_buffer_f_delete(b, (sst_f_inf*)r.data_src);
            break;
            
        case MD_COMMIT:
            break;// on commit, do not write a damn thing. this allows us to prevent strange edge case by blocking 
            //size 1 results from being written. since smallest key size is 2/4 bytes, we may set the limit to one less byte than the flush_size.
            // this prevents "double flush" circumstances when the md buffer gets flushed to disk but the md commit cmd doesnt fit
        default:
            write_buffer(b, r.data_src, r.len);
            break;
    }
    do_checksum(b, checksum_spot);
    return  m_rc_size(r);
}
/*the choice: metadata stored in manifest, or their respective files
pro for files: easy api for commits -> cannot be resolved easily
con for files; much slower startup times-> could be resolved by using async spam
requires some changes to the sst file format

*/
static db_FILE * clone_ctx(db_FILE * ctx){
    db_FILE * ctx_cp = get_ctx();
    size_t size = sizeofdb_FILE();
    task_t * task = aco_get_arg();
    memcpy(ctx_cp, ctx, size);
    ctx_cp->callback_arg = task;
    return ctx_cp;
}
static void serialize_manifest_metadata(manifest *w, byte_buffer *b) {
    reset_buffer(b);
    uint64_t spot = reserve_checksum(b);
    write_buffer(b, (char*)&w->total_len, sizeof(w->total_len));
    write_buffer(b, (char*)&w->segments_manager.current_segment_idx, sizeof(w->segments_manager.current_segment_idx));
    write_buffer(b, (char*)&w->segments_manager.segments[w->segments_manager.current_segment_idx].current_size, sizeof(size_t));
    write_int64(b, w->snapshot_ptr);
    do_checksum(b, spot);
}
static bool deserialize_manifest_metadata(manifest *w, byte_buffer *b) {
    read_buffer(b, (char*)&w->total_len, sizeof(w->total_len));
    read_buffer(b, (char*)&w->segments_manager.current_segment_idx, sizeof(w->segments_manager.current_segment_idx));
    read_buffer(b, (char*)&w->segments_manager.segments[w->segments_manager.current_segment_idx].current_size, sizeof(size_t));
    w->snapshot_ptr = read_int64(b);
    if (w->segments_manager.current_segment_idx < 0 || w->segments_manager.current_segment_idx >= w->segments_manager.num_segments) {
        fprintf(stderr, "Warning: Invalid current_segment_idx (%d) read from manifest metadata. Resetting.\n", w->segments_manager.current_segment_idx);
        w->segments_manager.current_segment_idx = 0;
        w->segments_manager.segments[0].current_size = 0;
        w->total_len = 0;
        return false;
    }
    return true;
}

static int rotate_manifest_segment(manifest *w) {
    manifest_seg_mgr *mgr = &w->segments_manager;
    int current_idx = mgr->current_segment_idx;
    w->rotating = true;
    int next_idx = (current_idx + 1) % mgr->num_segments;
    
    mgr->segments[current_idx].active = false;
    mgr->segments[next_idx].active = true;
    mgr->segments[next_idx].current_size = 0;

    mgr->current_segment_idx = next_idx;
    return 0;
}
static int read_segement_header(byte_buffer * stream, char * time) {
    char * start = go_nearest_v(stream, ':');
    if (start == NULL) return -1;
    start ++; /*skip a space*/
    int size;
    read_buffer(stream, &size, sizeof(size));
    assert(size > 0 && size < 128 );
    read_buffer(stream, time, size);
    return size;
}
static int commit_buffer(manifest *w){
    manifest_seg_mgr*mgr = &w->segments_manager;
    manifest_segment *current_segment = &mgr->segments[mgr->current_segment_idx];
    db_FILE *file_ctx = clone_ctx(current_segment->model);


    byte_buffer *buffer_to_flush = w->manifest_buffer;
    w->manifest_buffer = select_buffer(w->flush_cadence);

    set_context_buffer(file_ctx, buffer_to_flush);
    size_t flush_size = buffer_to_flush->curr_bytes;
    size_t write_offset = current_segment->current_size - flush_size;
    int submission_result = dbio_write(file_ctx, write_offset, flush_size);
    dbio_fsync(file_ctx);


    return_ctx(file_ctx);
    return_buffer(buffer_to_flush);
    return submission_result;

}
static int flush_atomic_add(manifest *w, const manifest_record r) {
    if (!w || !w->manifest_buffer || w->manifest_buffer->curr_bytes == 0) {
        return 0;
    }

    manifest_seg_mgr*mgr = &w->segments_manager;
    manifest_segment *current_segment = &mgr->segments[mgr->current_segment_idx];
    byte_buffer *buffer_to_flush = w->manifest_buffer;
    size_t flush_size = buffer_to_flush->curr_bytes;
    size_t write_offset = current_segment->current_size - flush_size;

    db_FILE *file_ctx = clone_ctx(current_segment->model);
    if (!file_ctx) {
        fprintf(stderr, "CRITICAL: No db_FILE available in pool for segment %d. Aborting.\n", mgr->current_segment_idx);
        exit(EXIT_FAILURE);
    }
    /*write type referring to a flush so the parser knows to skip to the next 4k aligned pg*/

    current_segment->current_size += add_to_buffer_generic(w->manifest_buffer, make_record(MD_FLUSH, NULL, 0));
    w->manifest_buffer = select_buffer(w->flush_cadence);
    if (current_segment->current_size + w->manifest_buffer->max_bytes > w->segments_manager.segment_capacity){
        char buf [128];
        grab_time_char(buf);
        current_segment->current_size += write_buffer(w->manifest_buffer, "TIME: ", strlen("TIME: "));
        int len = strlen(buf);
        current_segment->current_size +=write_buffer(w->manifest_buffer, &len, sizeof(len));
        current_segment->current_size +=write_buffer(w->manifest_buffer, buf, len);
    }
    if (!w->manifest_buffer) {
        fprintf(stderr, "CRITICAL: Failed to select new manifest buffer during flush. Aborting.\n");
        return_ctx(file_ctx);
        exit(EXIT_FAILURE);
    }
    current_segment->current_size += add_to_buffer_generic(w->manifest_buffer, r);

    
    set_context_buffer(file_ctx, buffer_to_flush);

    int submission_result = dbio_write(file_ctx, write_offset, flush_size);


    return_ctx(file_ctx);
    return_buffer(buffer_to_flush);

    if (submission_result < 0) {
        perror("dbio_write submission failed in flush_wal_buffer");
       
        return submission_result;
    }
    return flush_size;
}
manifest* init_manifest(byte_buffer *b, uint64_t seg_cap) {
    manifest *w = malloc(sizeof(manifest));
    if (!w) {
        perror("CRITICAL: Failed to allocate manifest struct");
        exit(EXIT_FAILURE);
    }
    memset(w, 0, sizeof(manifest));
    manifest_seg_mgr *mgr = &w->segments_manager;
    mgr->num_segments = NUM_MD_SEG;
    mgr->segment_capacity =seg_cap;
    mgr->current_segment_idx = 0;
    w->flush_cadence = 4096;
    for (int i = 0; i < mgr->num_segments; ++i) {
        manifest_segment *seg = &mgr->segments[i];
        sprintf(seg->filename, "MAN_SEG_%d.bin", i);
        seg->current_size = 0;
        seg->active = (i == 0);

        
        db_FILE *file_ctx_for_clone = dbio_open(seg->filename, 'r');
        if (!file_ctx_for_clone || file_ctx_for_clone->desc.fd < 0) {
            dbio_close(file_ctx_for_clone);
            file_ctx_for_clone = dbio_open(seg->filename, 'w');
        }

        // Ensure the open succeeded
        if (!file_ctx_for_clone || file_ctx_for_clone->desc.fd < 0) {
            perror("CRITICAL: Failed to open/create initial manifest segment file");
            exit(EXIT_FAILURE);
        }
        seg->model = file_ctx_for_clone;
    }

    w->manifest_buffer = select_buffer(GLOB_OPTS.WAL_BUFFERING_SIZE);
    
    if (!w->manifest_buffer) {
        perror("CRITICAL: Failed to allocate initial manifest buffer");
        exit(EXIT_FAILURE);
    }
    
    bool loaded_metadata = false;
    w->meta_ctx = dbio_open(GLOB_OPTS.WAL_M_F_N, 'r');

    if (w->meta_ctx && w->meta_ctx->desc.fd >= 0) {
        byte_buffer *meta_read_buf = b ? b : select_buffer(4096);
        if (meta_read_buf) {
            reset_buffer(meta_read_buf);
            set_context_buffer(w->meta_ctx, meta_read_buf);
            int bytes_read = dbio_read(w->meta_ctx, 0, meta_read_buf->max_bytes);

            if (bytes_read > 0) {
                meta_read_buf->curr_bytes = bytes_read;
                if (deserialize_manifest_metadata(w, meta_read_buf)) {
                    loaded_metadata = true;
                    for(int i=0; i<mgr->num_segments; ++i) {
                        mgr->segments[i].active = (i == mgr->current_segment_idx);
                    }
                }
            } else if (bytes_read < 0) {
                perror("Failed to read manifest metadata file, continuing...");
            }
            if (!b) return_buffer(meta_read_buf);
        } else {
            perror("Failed to allocate buffer for reading manifest metadata, continuing...");
        }
        dbio_close(w->meta_ctx);
    }

    w->meta_ctx = dbio_open(GLOB_OPTS.WAL_M_F_N, 'w');
    if (!w->meta_ctx || w->meta_ctx->desc.fd < 0) {
        perror("CRITICAL: Failed to open/create manifest metadata file for writing");
        exit(EXIT_FAILURE);
    }

    if (!loaded_metadata) {
        mgr->segments[0].current_size = 0;
        w->total_len = 0;
        struct db_FILE *trunc_ctx = dbio_open(mgr->segments[0].filename, 'w');
        if(trunc_ctx) dbio_close(trunc_ctx); else perror("Warning: Failed to truncate initial manifest segment");
    } 
    manifest_seg_mgr* mrg = &w->segments_manager;
    manifest_segment *  current_segment= &mrg->segments[w->segments_manager.current_segment_idx];
    char buf [128];
    grab_time_char(buf);
    current_segment->current_size += write_buffer(w->manifest_buffer, "TIME: ", strlen("TIME: "));
    int len = strlen(buf);
    current_segment->current_size +=write_buffer(w->manifest_buffer, &len, sizeof(len));
    current_segment->current_size +=write_buffer(w->manifest_buffer, buf, len);
    
    return w;
}
/*this kill function flushes the buffers, which means everything gets safely persisted to disk
as such, since all commands should be submmited beforehand, no special dump is required*/
void kill_manifest(manifest *w) {
    if (!w) return;
    if (man_f_commit(w) < 0) {
        fprintf(stderr, "Warning: Error during final manifest buffer flush in kill_manifest.\n");
    }
    w->meta_ctx->callback_arg = aco_get_arg();
    byte_buffer *meta_write_buf = select_buffer(4096);
    if (meta_write_buf) {
        serialize_manifest_metadata(w, meta_write_buf);
        if (meta_write_buf->curr_bytes > 0) {
            set_context_buffer(w->meta_ctx, meta_write_buf);
            if (dbio_write(w->meta_ctx, 0, meta_write_buf->curr_bytes) < 0) {
                 perror("Warning: Failed to write final manifest metadata");
            } else {
                 dbio_fsync(w->meta_ctx);
            }
        }
    } 
    else {
        perror("Warning: Failed to allocate buffer for final manifest metadata write");
    }
    for (int  i =0; i < NUM_MD_SEG; i++){
        w->segments_manager.segments[i].model->callback_arg = aco_get_arg();
        dbio_close(w->segments_manager.segments[i].model);
    }
    if (w->meta_ctx) {
        dbio_close(w->meta_ctx);
        w->meta_ctx = NULL;
    }

    if (w->manifest_buffer) {
        return_buffer(w->manifest_buffer);
        w->manifest_buffer = NULL;
    }  
    free(w);
}
