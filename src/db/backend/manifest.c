#include "manifest.h"







int man_f_delete(){

}

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
    write_buffer(b, (char*)&w->total_len, sizeof(w->total_len));
    write_buffer(b, (char*)&w->segments_manager.current_segment_idx, sizeof(w->segments_manager.current_segment_idx));
    write_buffer(b, (char*)&w->segments_manager.segments[w->segments_manager.current_segment_idx].current_size, sizeof(size_t));
}

static bool deserialize_manifest_metadata(manifest *w, byte_buffer *b) {
    read_buffer(b, (char*)&w->total_len, sizeof(w->total_len));
    read_buffer(b, (char*)&w->segments_manager.current_segment_idx, sizeof(w->segments_manager.current_segment_idx));
    read_buffer(b, (char*)&w->segments_manager.segments[w->segments_manager.current_segment_idx].current_size, sizeof(size_t));

    if (w->segments_manager.current_segment_idx < 0 || w->segments_manager.current_segment_idx >= w->segments_manager.num_segments) {
        fprintf(stderr, "Warning: Invalid current_segment_idx (%d) read from manifest metadata. Resetting.\n", w->segments_manager.current_segment_idx);
        w->segments_manager.current_segment_idx = 0;
        w->segments_manager.segments[0].current_size = 0;
        w->total_len = 0;
        return false;
    }
    return true;
}

static int rotate_wal_segment(manifest *w) {
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
static int flush_manifest_buffer(manifest *w, f_str k) {
    if (!w || !w->manifest_buffer || w->manifest_buffer->curr_bytes == 0) {
        return 0;
    }

    manifest_seg_mgr*mgr = &w->segments_manager;
    manifest_segment *current_segment = &mgr->segments[mgr->current_segment_idx];
    byte_buffer *buffer_to_flush = w->manifest_buffer;
    size_t flush_size = buffer_to_flush->curr_bytes;
    size_t write_offset = current_segment->current_size;

    db_FILE *file_ctx = clone_ctx(current_segment->model);
    if (!file_ctx) {
        fprintf(stderr, "CRITICAL: No db_FILE available in pool for segment %d. Aborting.\n", mgr->current_segment_idx);
        exit(EXIT_FAILURE);
    }
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
    if (k.entry){
        current_segment->current_size += write_disk_format(w->manifest_buffer, k);
    }
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
static void get_wal_fn(char * buf, int idx){
    sprintf(buf, "WAL_SEG_%d.bin", idx);
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
        sprintf(seg->filename, "WAL_SEG_%d.bin", i);
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
static int write_md_generic(manifest *w, f_str key) {
    if (!w) return FAILED_TRANSCATION;

    int ret = 0;
    uint64_t data_size = key.len;
    manifest_seg_mgr*mgr = &w->segments_manager;
    manifest_segment *current_segment = &mgr->segments[mgr->current_segment_idx]; 

 
    bool rotation_needed = (current_segment->current_size + data_size > mgr->segment_capacity);
    bool values_written  = false;
    if (rotation_needed) {

        ret = rotate_wal_segment(w);
        if (ret != 0) return FAILED_TRANSCATION;
        if (w->manifest_buffer && w->manifest_buffer->curr_bytes > 0) {
            ret = flush_manifest_buffer(w, key);
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
        ret = flush_manifest_buffer(w, key);
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
    ret = write_disk_format(w->manifest_buffer, key);
    if (ret < 0) {
        fprintf(stderr, "Error writing key to manifest buffer\n");
        return FAILED_TRANSCATION;
    }
    written_to_buffer += ret;

    if (ret < 0) {
        fprintf(stderr, "Error writing value to manifest buffer\n");
        return FAILED_TRANSCATION;
    }
    written_to_buffer += ret;

    w->total_len++;
    current_segment->current_size += written_to_buffer;
    return 0;
}

void kill_manifest(manifest *w) {
    if (!w) return;
    f_str empty;
    empty.entry = NULL;
    if (flush_manifest_buffer(w, empty) < 0) {
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
        w->segments_manager.segments->model->callback_arg = aco_get_arg();
        dbio_close(w->segments_manager.segments->model);
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
