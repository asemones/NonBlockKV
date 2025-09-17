#include "indexer.h"
#define NUM_HASH_SST 10
#define MAX_KEY_SIZE 100
#define TIME_STAMP_SIZE 32
#define MAX_F_N_SIZE 64
#define MAX_LEVELS 7
#define BASE_LEVEL_SIZE 6400000
#define MIN_COMPACT_RATIO 50
#define MEM_SIZE 100000
sst_f_inf create_sst_empty(){
    sst_f_inf file;
    file.block_indexs = List(0, sizeof(block_index), true);
    file.mem_store = calloc_arena(MEM_SIZE);
    file.length = 0;
    file.block_start = -1;
    file.filter = bloom(NUM_HASH_SST,2000, false, NULL);
    file.marked = false;
    file.in_cm_job = false;
    file.use_dict_compression = false; 
    init_sst_compr_inf(&file.compr_info, NULL);
    gettimeofday(&file.time, NULL);
    file.compressed_len = 0;
    file.file_name = arena_alloc(file.mem_store, MAX_F_N_SIZE);
    return file;

}

sst_f_inf create_sst_filter(){
    sst_f_inf file;
    file.block_indexs = calloc(sizeof(list), 1);
    file.length = 0;
    file.block_start = -1;
    file.marked = false;
    file.in_cm_job = false;
    file.use_dict_compression = false; 
    init_sst_compr_inf(&file.compr_info, NULL);
    file.compressed_len = 0;
    gettimeofday(&file.time, NULL);
    return file;

}
bool use_compression(sst_f_inf * f){
    return (f->compressed_len > 0 );
}
inline int num_blocks_in_pg(block_index * blocks){
    int start= 0;
    while(blocks[start].type != PHYSICAL_PAGE_HEADER){
        start --;
    }
    int start_num = start;
    while(blocks[start].type != PHYSICAL_PAGE_END){
        start ++;
    }
    return (start - start_num + 1);
}
/*
int sst_deep_copy(sst_f_inf * master, sst_f_inf * copy){
    if (master == NULL) return 0;
    memcpy(copy, master, sizeof(sst_f_inf));
    copy->mem_store = calloc_arena(MEM_SIZE);
    if (copy->mem_store == NULL){
        return STRUCT_NOT_MADE;
    }
    copy->block_indexs = List(0, sizeof(block_index),false);
    for (int i = 0; i < master->block_indexs->len; i++){
        block_index * master_b = at(copy->block_indexs, i);
        if (master_b == NULL) continue;
        block_index ind;
        ind.min_key = arena_alloc(copy->mem_store, 40);
        ind.checksum =master_b->checksum;
        ind.len = master_b->len;
        ind.num_keys = master_b->num_keys;
        ind.offset = master_b->offset;
        ind.type = master_b->type;
        memcpy(ind.min_key, master_b->min_key, 40);
        insert(copy->block_indexs, &ind);
    }
    copy->marked = master->marked;
    copy->in_cm_job = master->in_cm_job;
    copy->use_dict_compression = master->use_dict_compression;
    copy->compressed_len = master->compressed_len;
    
    // Initialize compression info
    init_sst_compr_inf(&copy->compr_info, NULL);
    
    return 0;
}
*/
block_index * create_block_index(size_t est_num_keys){
    block_index * index = (block_index*)wrapper_alloc((sizeof(block_index)), NULL,NULL);
    if (index== NULL) return NULL;
    index->len = 0;
    index->num_keys =0;
    index->type = PHYSICAL_PAGE_HEADER;
    index->page = NULL;
    return index;
}
block_index create_ind_stack(size_t  est_num_keys){
    block_index index;
    index.len = 0;
    index.num_keys =0;
    index.type = PHYSICAL_PAGE_HEADER;
    index.page = NULL;
    return index;
}
uint64_t block_ind_size(){
    return sizeof(block_index) + 40;
}
block_index * block_from_stream(byte_buffer * stream, block_index * index){
    read_buffer(stream, &index->offset, sizeof(size_t));
    read_buffer(stream, &index->len, sizeof(size_t));
    read_buffer(stream, &index->num_keys, sizeof(index->num_keys));
    read_buffer(stream, &index->checksum, sizeof(index->checksum));
    read_buffer(stream, &index->type, sizeof(index->type));
    read_buffer(stream, &index->footer_start, sizeof(index->footer_start));
    return index;
}
f_str block_key_decode(const char * block_start, uint16_t * offsets, uint16_t ind){
    f_str one;
    one.entry = (void*)block_start + offsets[ind] + sizeof(uint16_t);
    one.len = *(uint16_t*)block_start + offsets[ind];
    return one;
}
f_str decode_val_from_k(f_str decoded_key){
    f_str ret;
    char * addr_of_vu_start = decoded_key.entry;
    char * real_start=  addr_of_vu_start + decoded_key.len;
    ret.len =  *((uint16_t*)real_start);
    ret.entry = real_start + sizeof(ret.len);
    return ret;
}
/*look into simd in future^tm*/
void * fetch_all_large(const char * start, uint16_t num, uint16_t * offsets){
    return NULL;
}
merge_data decode_next_pair_iter(const char * block_start, uint16_t * offsets, uint16_t ind_to_get){
    f_str key= block_key_decode(block_start, offsets, ind_to_get);
    f_str value = decode_val_from_k(key);
    merge_data ret;
    ret.key = format_for_in_mem(key);
    ret.value = format_for_in_mem(value);
    ret.index = ind_to_get;
    return ret;
}
int block_to_stream(byte_buffer * targ, block_index index){
    int result = write_buffer(targ, (char*)&index.offset, sizeof(size_t));
    result = write_buffer(targ, (char*)&index.len, sizeof(size_t));
    result = write_buffer(targ, &index.num_keys, sizeof(index.num_keys));
    result = write_buffer(targ, &index.checksum, sizeof(index.checksum));
    result = write_buffer(targ, &index.type, sizeof(index.type));
    result  = write_buffer(targ, &index.footer_start, sizeof(index.footer_start));
    return result;
}
sst_partition_ind * part_stream(byte_buffer * stream, sst_partition_ind * ind){
    read_buffer(stream, &ind->off, sizeof(ind->off));
    read_buffer(stream, &ind->len, sizeof(ind->len));
    read_buffer(stream, &ind->num_blocks, sizeof(ind->num_blocks));
    read_fstr(stream, &ind->min_fence);
    return ind;   
}
uint64_t future_part_bytes(sst_partition_ind * ind){
    return sizeof(ind->off) + sizeof(ind->len) +  sizeof(ind->num_blocks) + 40;
}
void part_to_stream(byte_buffer * stream, sst_partition_ind * ind){
    write_buffer(stream, &ind->off, sizeof(ind->off));
    write_buffer(stream, &ind->len, sizeof(ind->len));
    write_buffer(stream, &ind->num_blocks, sizeof(ind->num_blocks));
    write_fstr(stream, ind->min_fence);
}
/*why in the world is this used*/
int read_index_block(sst_f_inf * file, byte_buffer * stream){
    FILE * f = fopen(file->file_name, "rb");
    if (f == NULL) {
        return SST_F_INVALID;
    }
    int ret = fseek (f, 0, SEEK_END);
    if (ret  < 0 ) return FAILED_SEEK;
    size_t eof = ftell(f);
    ret = fseek(f, file->block_start, SEEK_SET);
    if (ret < 0 ) return FAILED_SEEK;
    int read = fread(stream->buffy, 1, eof - file->block_start, f);
    if (read != eof - file->block_start){
        return FAILED_READ;
    }
    stream->curr_bytes = read;
    stream->read_pointer = 0;
    for (int i =0; i < file->block_indexs->len; i++){
        block_index * index = (block_index*)at(file->block_indexs, i);
        block_index * ret = block_from_stream(stream, index);
        if (ret == NULL){
            return STRUCT_NOT_MADE;
        }
    }
    /*why the fuck is this here???? spent two hours looking for it*/
    file->filter = from_stream(stream);
    if (file->filter == NULL){
        return STRUCT_NOT_MADE;
    }
    fclose(f);
    return 0;
}
void dump_block_min_keys(byte_buffer* stream,  block_index * arr , int num){
    for (int i = 0; i < num; i++){
        write_fstr(stream, arr[i].min_key);
    }   
}
void read_block_min_keys(byte_buffer* stream,  block_index * arr, int num){
    for (int i  = 0; i < num; i++){
        read_and_allocate(stream, &arr[i].min_key);
    }
}
int all_stream_index(size_t num_table, byte_buffer* stream, block_index * indexes){
    block_index * test;
    for (int i = 0; i < num_table; i ++){
        test = block_from_stream(stream, &indexes[i]);
        if (test == NULL){
            return STRUCT_NOT_MADE;
        }
    }
    read_block_min_keys(stream, indexes, num_table);
    return 0;
}
int all_index_stream(size_t num_table, byte_buffer* stream, list * indexes){
    int ret =0;
    block_index * arr = indexes->arr;
    for (int i = 0; i < num_table; i ++){
        ret = block_to_stream(stream, arr[i]);
        if (ret != 0 ){
            return STRUCT_NOT_MADE;
        }
    }
    dump_block_min_keys(stream, arr, num_table);
    return 0;
}
int all_index_stream_part(size_t num_table, byte_buffer* stream, list * indexes, sst_partition_ind * parts){
    int ret =0;
    block_index * arr = indexes->arr;
    uint64_t part_counter = 0;
    uint64_t curr_part = 0;
    for (int i = 0; i < num_table; i ++){
        if (part_counter == parts[curr_part].num_blocks) {
            dump_block_min_keys(stream, &arr[i], part_counter);
            seralize_filter(stream, parts[curr_part].filter);
            part_counter = 0;
            curr_part++;
        }
        ret = block_to_stream(stream, arr[i]);
        if (ret != 0 ){
            return STRUCT_NOT_MADE;
        }
        else{
            part_counter ++;
        }
    }
    return 0;
}
void free_block_index(void * block){
    block_index * index = (block_index*)block;
    if (index == NULL) return;
    free(index);
}
void reuse_block_index(void * b){
    block_index * index = (block_index*)b;
    if (index == NULL) return;
    // free_filter(index->filter); // Assuming free_filter handles NULL
    // index->filter = NULL; // Already removed
    index->len = 0;
    index->num_keys = 0;
    index->type = PHYSICAL_PAGE_HEADER;
    index->page = NULL;
}
int cmp_sst_f_inf(void* one, void * two){
    sst_f_inf * info1 = one;
    sst_f_inf * info2 = two;
    
    return f_cmp(info1->min, info2->max);
}
int sst_equals(sst_f_inf * one, sst_f_inf * two){
    return one->block_indexs == two->block_indexs;
}

int find_sst_file_eq_iter(list  *sst_files, size_t num_files, const char * file_n) {
   

    for (int i = 0; i < sst_files->len; i++){
        sst_f_inf * sst = internal_at_unlocked(sst_files,i);
        if (strcmp(sst->file_name, file_n) == 0) return i;
    }
    return -1;
}
void generate_unique_sst_filename(char *buffer, size_t buffer_size, int level) {
    static int seeded = 0;
    if (!seeded) {
        struct timeval tv_seed;
        gettimeofday(&tv_seed, NULL);
        srand((unsigned int)(tv_seed.tv_sec ^ tv_seed.tv_usec ^ getpid()));
        seeded = 1;
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);

    uint32_t r1 = (uint32_t)rand();
    uint32_t r2 = (uint32_t)rand();
    uint64_t random_val = ((uint64_t)r1 << 32) | r2;

    snprintf(buffer, buffer_size, "sst_%d_%ld_%ld_%016lx",
             level, tv.tv_sec, tv.tv_usec, random_val);
}
void build_index(sst_f_inf * sst, block_index * index, byte_buffer * b, size_t num_entries, size_t block_offsets){

    index->offset = block_offsets;
    u_int16_t key_len;
    if (b){
        memcpy(&key_len, &b->buffy[block_offsets + 2], sizeof(u_int16_t));
        read_and_allocate_char(&b->buffy[block_offsets + 4], &sst->min, key_len);
        if (is_avx_supported()){
            index->checksum =  crc32_avx2((uint8_t*)buf_ind(b, block_offsets), index->len);
        }
        else{
            index->checksum = crc32((uint8_t*)buf_ind(b, block_offsets), index->len);
        }
    } 
    index->num_keys = num_entries;
    insert(sst->block_indexs, index);  
}
void free_sst_inf(void * ss){
    sst_f_inf * sst = (sst_f_inf*)ss;
    free_list(sst->block_indexs, &reuse_block_index);
    free_arena(sst->mem_store);
    sst->block_indexs = NULL;
    sst->mem_store = NULL;
    free_bit(sst->filter->ba);
    free_sst_cmpr_inf(&sst->compr_info);
    sst->filter = NULL;
    sst = NULL;

    //free(sst);
}
void grab_min_b_key(block_index * index, byte_buffer *b, int loc){
    u_int16_t key_len;
    memcpy(&key_len, &b->buffy[loc + 2], sizeof(u_int16_t));
    index->min_key = f_set(&b->buffy[loc+4], key_len);
}
int block_b_search(const uint16_t *offsets, int num_entries, const f_str target, const char * block_start){
    int lo = 0, hi = num_entries - 1;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        f_str k = block_key_decode(block_start, (uint16_t *)offsets, mid);
        int cmp = disk_f_cmp(k, target);
        if (cmp == 0) return mid;
        else if (cmp <  0) lo = mid + 1;
        else  hi = mid - 1;
    }
    return -1;
}
int prefix_b_search(const uint16_t *offsets, int num_entries, f_str prefix, const char *block_start){
    int lo = 0, hi = num_entries - 1, mid;
    int found = -1;

    // find *any* match, but bias hi to find lower bound
    while (lo <= hi) {
        mid = (lo + hi) >> 1;
        f_str k = block_key_decode(block_start, (uint16_t *)offsets, mid);
        int  cmp = disk_f_cmp(k, prefix);
        if (cmp <  0) lo = mid + 1;
        else if (cmp >  0) hi = mid - 1;
        else {              
            found = mid;
            hi = mid - 1; // keep looking left for the first
        }
    }
    return found;
}

void write_all_sst_parts(sst_f_inf * sst, byte_buffer * stream, list * blocks){
    sst_partition_ind * parts  = sst->sst_partitions->arr;
    uint64_t bytes = sst->sst_partitions->len * future_part_bytes(parts);
    uint64_t multipler = block_ind_size();
    for (int i  =0; i <  sst->sst_partitions->len; i++){
        parts[i].off = bytes + (multipler * i);
        part_to_stream(stream, &parts[i]);
    }
}
void read_sst_parts(sst_f_inf * to_read, byte_buffer * b,  uint64_t total_size){
    db_FILE * file = dbio_open(to_read->file_name, 'r');
    set_context_buffer(file, b);
    dbio_read(file, to_read->part_start, total_size);// (part_size - block_start);
    dbio_close(file);
    sst_partition_ind ind;
    for (int i = 0; i < to_read->part_start; i++){
        part_stream(b, &ind);
        insert(to_read->sst_partitions, &ind);
    }
}
void dump_sst_meta_part(sst_f_inf * to_write, byte_buffer * b, uint64_t part_start_4k_alligned, list * blocks){
    write_all_sst_parts(to_write, b, blocks);
    to_write->part_start = part_start_4k_alligned;
    to_write->block_start = part_start_4k_alligned + b->curr_bytes;
    all_index_stream_part(blocks->len,b, blocks, to_write->sst_partitions->arr);
}