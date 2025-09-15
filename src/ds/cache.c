#include "cache.h"
#define OVER_FLOW_EXTRA 100
#define NUM_COMPRESSION_BUF 20
cache_entry empty;
static cache_entry create_cache_entry(size_t page_size, size_t num_keys, arena *a) {
    cache_entry entry;
    entry.buf = NULL;
    entry.ar = NULL;
    entry.arr_len = 0;
    entry.ref_count  = 0;
    entry.loc = NULL;
    return entry;
}
cache create_cache(size_t capacity, size_t page_size) {
    cache c;
    c.capacity     = capacity;
    c.page_size    = page_size;
    c.filled_pages = 0;
    c.mem          = calloc_arena(capacity * 2);
    c.max_pages = capacity / (page_size + sizeof(byte_buffer) + sizeof(cache_entry)
                  + OVER_FLOW_EXTRA + (sizeof(f_str)) * 2);
    c.frames     = (cache_entry*)malloc(sizeof(cache_entry) * c.max_pages);
    c.ref_bits   = (uint8_t*)calloc(c.max_pages, sizeof(uint8_t));
    c.clock_hand = 0;

    for (size_t i = 0; i < c.max_pages; i++) {
        c.frames[i]   = create_cache_entry(page_size, page_size / 16, c.mem);
        c.frames[i].idx = i;
        c.ref_bits[i] = 0;
    }
    c.compression_buffers = create_pool(NUM_COMPRESSION_BUF);
    for (int i = 0; i < NUM_COMPRESSION_BUF; i++){
        insert_struct(c.compression_buffers, create_buffer(page_size));
    }
    return c;
}
/*implement a 256 vector verision or inline asm. we really dont want eviction as a bottleneck*/
size_t clock_evict(cache *c) {
    while (1) {
        size_t idx = c->clock_hand;
        cache_entry *  f = &c->frames[idx];

        if (c->ref_bits[idx] == 0 && f->ref_count == 0) {
            reset_buffer(f->buf);
            f->ar = NULL;
            f->arr_len =0;

            c->clock_hand = (c->clock_hand + 1) % c->max_pages;
            return idx;
        }

        c->ref_bits[idx] = 0;
        c->clock_hand = (c->clock_hand + 1) % c->max_pages;
    }
}
static size_t get_free_frame(cache *c) {
    if (c->filled_pages < c->max_pages) {
        return c->filled_pages++;
    }
    return clock_evict(c);
}
void handle_checksum_error(cache * c, db_FILE * sst_file, byte_buffer * buffer){
    fprintf(stderr, "invalid data\n");
    dbio_close(sst_file);
}

cache_entry retrieve_entry_no_prefetch(cache *c, block_index *index, const char *file_name, sst_f_inf *sst) {
    if (index->page) {
        cache_entry * pg = index->page;
        pg->ref_count ++;
        c->ref_bits[pg->idx] = 1;
        return *pg;
    }
    db_FILE * sst_file =  dbio_open(file_name, 'r');

    if (!sst_file) {
        return empty;
    }
    size_t idx = get_free_frame(c);
    /*yes, this can include negative indexing! however, this is fine because
    it will never go out of bounds AND index points in contingous memory*/
    block_index * read_loc = get_pg_header(index);

    size_t relative_index = index->offset - read_loc->offset;
    /*len becomes aligned*/
    pin_page(&c->frames[idx]);
    /*save our frame*/
    byte_buffer *buffer = request_struct(man->four_kb);
    set_context_buffer(sst_file, buffer);

    size_t bytes_read = dbio_read(sst_file, read_loc->offset, index->len);
    uint64_t actually_read = min(bytes_read, index->len);
    bytes_read = actually_read;
    if (bytes_read == 0) return empty;
    byte_buffer * decompressed = NULL;
    /*if compress, we need to swap buffers*/
    if (use_compression(sst)){
        decompressed = request_struct(c->compression_buffers);
        buffer->curr_bytes = index->len + relative_index;
        b_seek(buffer, relative_index);
        handle_decompression(sst, buffer, decompressed);
        c->frames[idx].buf = decompressed;
    }

    c->ref_bits[idx] = 1;
    /*checksum fail*/
    if (!verify_data((uint8_t*)buf_ind(buffer, relative_index), actually_read, index->checksum)) {
        handle_checksum_error(c, sst_file, buffer);
        c->frames[idx].ref_count --;
        if (decompressed) return_struct(c->compression_buffers, decompressed, &reset_buffer);
        return_struct(man->four_kb, buffer, NULL);
        return empty;
    }
    /*if compress, we need to swap buffers, so return the buffer it was read into */
    if (use_compression(sst)){
        c->frames[idx].buf = decompressed;
        c->frames[idx].loc = c->compression_buffers;
        return_struct(man->four_kb, buffer, NULL);
    }
    else{
        /*otherwise, no compression buffer exists so continue and set buffer to regular buf*/
        c->frames[idx].buf = buffer;
    }
    dbio_close(sst_file);
    index->page = &c->frames[idx];
    c->frames[idx].ar = (uint16_t*)buf_ind(c->frames[idx].buf, index->footer_start);
    c->frames[idx].arr_len = index->num_keys;
    return c->frames[idx];
}

void pin_page(cache_entry *c) {
   c->ref_count ++;
}
void unpin_page(cache_entry *c) {
    c->ref_count --;
    if (c->ref_count <= 0 && c->loc == NULL ){
        return_struct(man->four_kb, c->buf, &reset_buffer);
        c = NULL; // Assuming 'buffer' is the pointer corresponding to 'c->idx' and no reset needed
    }
    else if (c->ref_count <= 0){
        return_struct(c->loc, c->buf, &reset_buffer);
        c->loc = NULL;
        c = NULL;
    }
}

void free_cache(cache *c) {
    if (!c) return;
    free(c->frames);
    free(c->ref_bits);
    free_arena(c->mem);
}