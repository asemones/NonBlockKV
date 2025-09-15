#include "compression.h"
#include "indexer.h"
#define ALIGN_UP(x, a) (((x) + ((a)-1)) & ~((a)-1))

struct block_index;
/*lazy*/
static __thread byte_buffer * shared_temp_buffer = NULL;
static const size_t BUFFER_SIZE = 64 * 1024; /*nobody should be using pages bigger*/

 byte_buffer * get_temp_copy_buffer(){
   shared_temp_buffer = create_buffer(BUFFER_SIZE);
   return shared_temp_buffer;
}
void init_compress_tr_inf(compress_tr_inf * inf, byte_buffer * buffer, sst_cmpr_inf* sst){
    inf->curr_size = 0;
    inf->sizes = List(0, sizeof(size_t), false);
    inf->buffer = buffer;
    inf->sst = sst;
}
void init_sst_compr_inf(sst_cmpr_inf * sst, void * dict_buffer){
    sst->compr_context = NULL;
    sst->compression_dict = NULL;
    sst->decompress_dict = NULL;
    sst->decompr_context = NULL;
    sst->dict_len =0;
    sst->dict_offset =0;
    sst->dict_buffer = NULL;
    if (dict_buffer != NULL) sst->dict_buffer = dict_buffer;
}
void set_dict_buffer(sst_cmpr_inf * sst, void * dict_buffer){
    sst->dict_buffer = dict_buffer;
}
void into_dict_buffer( compress_tr_inf * inf, f_str  key, f_str  value){
    write_buffer(inf->buffer, key.entry,key.len);
    write_buffer(inf->buffer, value.entry, value.len);
    size_t real = key.len + value.len;
    insert(inf->sizes, &real);
}
int compress_blocks(sst_f_inf * sst, byte_buffer * raw, byte_buffer * compressed){
    int logical_page_tracker = 0;
    byte_buffer * temp = get_temp_copy_buffer();
    for (int i = 0 ;i < sst->block_indexs->len; i++){
        block_index * block = at(sst->block_indexs,i);
        raw->read_pointer = block->offset;
        int new_offset = compressed->curr_bytes;
        int res = -1;
        if (sst->use_dict_compression){
            res = compress_dict(&sst->compr_info, raw, temp, block->len);
        }
        if (res < 0){
            res = regular_compress(&sst->compr_info, raw, temp, block->len);
        }
        int padding = 0;
        /*we need to allign all compressed blocks to fit into a physical page size*/
        if (check_for_lg_pg(logical_page_tracker, res, GLOB_OPTS.BLOCK_INDEX_SIZE)){  
            int next_offset = compressed->curr_bytes + res;
            new_offset = ALIGN_UP(next_offset, GLOB_OPTS.BLOCK_INDEX_SIZE);
            padding = new_offset - next_offset;
            logical_page_tracker = 0;
            block_index * prev =  at(sst->block_indexs,i- 1);
            if (prev){
                prev->type = PHYSICAL_PAGE_END;
            }
        }
        else{
            block->type = MIDDLE;
        }
        /*add the new compressed len*/
        logical_page_tracker += res;
        write_buffer(compressed, temp->buffy, temp->curr_bytes);
        padd_buffer(compressed, padding);
        block->offset = new_offset;
        block->len = res;
        temp->curr_bytes = 0;
    }
    return 0;
}
void prep_dict_compression(compress_tr_inf * inf, list * blocks, byte_buffer * input_buffer){
    f_str key;
    f_str value;
    char key_buffer[2048];
    char value_buffer[2048];
    key.entry = key_buffer;
    int real_c = 0;
    value.entry = value_buffer;
    input_buffer->read_pointer = 0;
    for (int i = 0; i < blocks->len; i++){
        block_index * b = at(blocks, i);
        input_buffer->read_pointer+= 2; /*skip over the listing with num_keys*/
        for (int j = 0; j < b->num_keys; j++){
            read_disk_format(input_buffer, &key);
            read_disk_format(input_buffer, &value);
            into_dict_buffer(inf, key, value);
            real_c++;
        }
    }
    fprintf(stdout, "%d", real_c);

/*zdict manager is needed to handle loading and dumping dictionaries. you PROBABLY cannot just do one fat file unless you do something funky. nor can you
leave it at the end due to the shitty api.
rememeber that compression intergration is UNTESTED and may be painful*/}
int load_dicts(sst_cmpr_inf * sst, int compression_level, void * buffer, FILE * file){
    fseek(file,sst->dict_offset, SEEK_SET);
    read_wrapper(buffer, 1,sst->dict_len, file);
    if (sst->compression_dict == NULL){
        sst->compression_dict = ZSTD_createCDict(sst->dict_buffer, sst->dict_len, compression_level);
    }
    if (sst->decompress_dict == NULL){
        sst->decompress_dict= ZSTD_createDDict(sst->dict_buffer, sst->dict_len);
    }
    return 0;
}
int sample_for_dict(sst_cmpr_inf * sst, int sst_size, int compression_level, void * key_val_sample, size_t * entry_lens, size_t num_entries){
    if (GLOB_OPTS.dict_size_ratio == 0) return 1;
    int dict_size =  sst_size / GLOB_OPTS.dict_size_ratio;
    if (sst->dict_buffer == NULL){
        sst->dict_buffer = calloc(dict_size, 1);
        if (sst->dict_buffer == NULL) return -1;
    }
    size_t res = ZDICT_trainFromBuffer(sst->dict_buffer, dict_size, key_val_sample, entry_lens, num_entries);
    if (ZSTD_isError(res)){
        fprintf(stdout, "%s\n", ZSTD_getErrorName(res));
        return -1;
    }
    if (sst->compression_dict == NULL){
        sst->compression_dict = ZSTD_createCDict(sst->dict_buffer, dict_size, compression_level);
    }
    if (sst->decompress_dict == NULL){
        sst->decompress_dict= ZSTD_createDDict(sst->dict_buffer, dict_size);
    }
    sst->dict_len = dict_size;
    return 0;
}
int fill_dicts(compress_tr_inf * inf, int sst_size,  list * blocks, byte_buffer * input_buffer){
    prep_dict_compression( inf , blocks, input_buffer);
    return sample_for_dict(inf->sst,sst_size, GLOB_OPTS.compress_level,inf->buffer->buffy, inf->sizes->arr, inf->sizes->len);
}
int compress_dict(sst_cmpr_inf * sst,  byte_buffer * sst_stream, byte_buffer * dest,  int size ){
    char * place_to_write = buf_ind(dest, dest->curr_bytes);
    char * place_to_read = get_curr(sst_stream);
    int bytes_left=  dest->max_bytes - dest->curr_bytes;
    if (sst->compr_context == NULL){
         sst->compr_context = ZSTD_createCCtx();
    }
    size_t bytes = ZSTD_compress_usingCDict(sst->compr_context,place_to_write, bytes_left, place_to_read, size, sst->compression_dict);
    if (ZSTD_isError(bytes)){
        fprintf(stdout, "%s\n", ZSTD_getErrorName(bytes));
        return -1;
    }
    dest->curr_bytes+= bytes;
    return bytes;
}
int decompress_dict(sst_cmpr_inf * sst,  byte_buffer * sst_stream, byte_buffer * dest,  int size){

    if (sst->decompr_context == NULL){
        sst->decompr_context = ZSTD_createDCtx();
    }
    void * loc = get_curr(sst_stream);
    size_t bytes = ZSTD_decompress_usingDDict(sst->decompr_context, dest->buffy, dest->max_bytes, loc, size, sst->decompress_dict);
    if (ZSTD_isError(bytes)) {
        printf("%s", ZSTD_getErrorName(bytes));
        ZSTD_getErrorName(bytes);
        return -1;
    }
    dest->curr_bytes+= bytes;
    return bytes;
}
int regular_decompress(sst_cmpr_inf * sst,  byte_buffer * sst_stream, byte_buffer * dest,  int size){
    /*add the chunking if needed, liekly wont need*/
    if (sst->decompr_context == NULL){
        sst->decompr_context = ZSTD_createDCtx();
    }
    void * loc = get_curr(sst_stream);
    size_t bytes= ZSTD_decompressDCtx(sst->decompr_context, dest->buffy, dest->max_bytes, loc, size);
    if (ZSTD_isError(bytes)) {
        printf("%s", ZSTD_getErrorName(bytes));
        ZSTD_getErrorName(bytes);
        return -1;
    }
    dest->curr_bytes+= bytes;
    return bytes;
}
/*add a size parameter*/
int regular_compress(sst_cmpr_inf * sst, byte_buffer * sst_stream, byte_buffer * dest,  int size){
    char * place_to_write = buf_ind(dest, dest->curr_bytes);
    char * place_to_read = get_curr(sst_stream);
    int bytes_left=  dest->max_bytes - dest->curr_bytes;
    if (sst->compr_context == NULL){
        sst->compr_context = ZSTD_createCCtx();
    }
    size_t bytes= ZSTD_compressCCtx(sst->compr_context, place_to_write, bytes_left, place_to_read, size, GLOB_OPTS.compress_level);
    if (ZSTD_isError(bytes)) {
        printf("%s", ZSTD_getErrorName(bytes));
        return -1;
    }
    dest->curr_bytes+= bytes;
    return bytes;
}
int handle_compression(sst_f_inf* inf, byte_buffer* input_buffer, byte_buffer* output_buffer, byte_buffer* dict_buffer) {
    list * blocks = inf->block_indexs;
    if (inf == NULL || input_buffer == NULL || output_buffer == NULL) {
        return -1;
    }
    if (inf->use_dict_compression) {
        if (inf->compr_info.compression_dict == NULL &&
            dict_buffer != NULL && blocks != NULL) {
            /*use dict buffer param to convert our input buffer of a sst stream into a friendly format for dict compression*/
            compress_tr_inf tr_inf;
            init_compress_tr_inf(&tr_inf, dict_buffer, &inf->compr_info);
            inf->compr_info.dict_buffer = output_buffer->buffy; /*we use the output buffer so we dont need an ADDITIONAL buffer. Dict buffer is to store the dict friendly format of sst tables*/
            int sst_size = inf->length;
            int result = fill_dicts(&tr_inf, sst_size, blocks, input_buffer);
            if (result != 0){
                return result;
            }
            free_list(tr_inf.sizes, NULL);

            reset_buffer(dict_buffer);
            /*save the dictionary buffer to inf so we may save to disk once we know where the dict offset should be*/
            write_buffer(dict_buffer, inf->compr_info.dict_buffer, inf->compr_info.dict_len);
            reset_buffer(output_buffer);
            inf->compr_info.dict_buffer = dict_buffer->buffy;
        }
    }
    if (inf->compr_info.compression_dict == NULL || inf->compr_info.dict_len ==0){
        inf->use_dict_compression = false;
    }
    compress_blocks(inf, input_buffer, output_buffer);
    
    return output_buffer->curr_bytes;
}
int handle_decompression(sst_f_inf* inf, byte_buffer* input_buffer, byte_buffer* output_buffer) {
    if (inf == NULL || input_buffer == NULL || output_buffer == NULL) {
        return -1;
    }

    if (inf->use_dict_compression && inf->compr_info.decompress_dict != NULL) {
        return decompress_dict(&inf->compr_info, input_buffer, output_buffer, input_buffer->curr_bytes);
    } 
    else {
        return regular_decompress(&inf->compr_info, input_buffer, output_buffer, input_buffer->curr_bytes);
    }
}

void free_sst_cmpr_inf(sst_cmpr_inf * sst){
    if (sst->compr_context != NULL) ZSTD_freeCCtx(sst->compr_context);
    if (sst->decompr_context != NULL) ZSTD_freeDCtx(sst->decompr_context);
    if (sst->decompress_dict != NULL) ZSTD_freeDDict(sst->decompress_dict);
    if (sst->compression_dict != NULL) ZSTD_freeCDict(sst->compression_dict);
    // dict_buffer is now managed by the struct pool, so we don't free it here
    memset(sst, 0, sizeof(*sst));
}


