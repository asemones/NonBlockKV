#include "metadata.h"
#define BUFFER_SIZE 563840
#define DICT_BUFFER_SIZE 563840
#define NUM_HASH 7
#define NUM_WORD 2000
#define MIN_KEY_SIZE 40 /*DANGEROUS*/
#define MAX_LEVELS 7
#define BASE_LEVEL_SIZE 6400000
#define MIN_COMPACT_RATIO 50
#define INDEX_MEMORY_COUNT 1024*500
#define SNAP_HIST_LEN 50


 int read_sst_list(list * sst_files, byte_buffer * tempBuffer, size_t num_sst){
    byte_buffer * dict_buffer = create_buffer(1 * 1024 * 1024);//very lazy 1 MB since dictionaries should not be larger than that
    byte_buffer * small_buff = create_buffer(30 * 1024);
    for (int i = 0; i < num_sst; i++){
         size_t block_ind_len = 0;
         sst_f_inf sst_s = create_sst_empty();
         free_filter(sst_s.filter); /*we assign this later in read_index_block*/
         sst_f_inf * sst = &sst_s;
         read_buffer(tempBuffer, sst->file_name, strlen(&tempBuffer->buffy[tempBuffer->read_pointer])+1);
         read_buffer(tempBuffer, &sst->length, sizeof(size_t));
         read_buffer(tempBuffer, &sst->compressed_len, sizeof(size_t));
         read_buffer(tempBuffer, &sst->use_dict_compression, sizeof(bool));
         read_buffer(tempBuffer, &sst->compr_info.dict_offset, sizeof(sst->compr_info.dict_offset));
         read_buffer(tempBuffer, &sst->compr_info.dict_len, sizeof(sst->compr_info.dict_len));
         read_fstr(tempBuffer, &sst->max);
         read_fstr(tempBuffer, &sst->min);
         read_buffer(tempBuffer, &sst->time,sizeof(sst->time) );
         read_buffer(tempBuffer, &sst->block_start, sizeof(size_t));
         read_buffer(tempBuffer, &block_ind_len, sizeof(size_t));
         sst->block_indexs = thread_safe_list(block_ind_len,sizeof(block_index),false);
         if (sst->block_indexs == NULL){
             return STRUCT_NOT_MADE;
         }
         sst->block_indexs->len = block_ind_len;
         sst->mem_store= calloc_arena(GLOB_OPTS.BLOCK_INDEX_SIZE);
         if(sst->mem_store == NULL){
             return STRUCT_NOT_MADE;
         }
         int error_code = read_index_block(sst, small_buff);
         if (error_code !=0) return error_code;
         if (sst->use_dict_compression){
            FILE * f = fopen(sst->file_name, "rb");
            load_dicts(&sst->compr_info, GLOB_OPTS.compress_level, dict_buffer->buffy,f);
            fclose(f);
        }
        insert(sst_files,&sst_s);
         
    }
    free_buffer(small_buff);
    free_buffer(dict_buffer);
    return 0;
}
int save_snap(meta_data * meta){
    meta->current_tx_copy = create_snap();
    snapshot * save_loc = meta->current_tx_copy;
    for (int i =0; i < LEVELS; i++){
        if (meta->sst_files[i]== NULL) continue;
        save_loc->sst_files[i] = thread_safe_list(0, sizeof(sst_f_inf),false);
        for (int j = 0; j < meta->sst_files[i]->len; j++){
            sst_f_inf f;
            sst_f_inf * sst_to_copy=  at(meta->sst_files[i],j);
            int ret =sst_deep_copy(sst_to_copy, &f);
            if (ret !=OK) return ret;
            insert(save_loc->sst_files[i], &f);
        }
    }
    return OK;
}

void fresh_meta_load(meta_data * meta){
    meta->num_sst_file = 0;
    meta->file_ptr = 0;
    meta->db_length = 0;
    meta->shutdown_status = -1;
   //meta->basic_index = Dict();
    meta->base_level_size = BASE_LEVEL_SIZE;
    meta->min_c_ratio = MIN_COMPACT_RATIO;
    /*make lists for all*/
    for (int i = 0; i < MAX_LEVELS; i++){
        meta->sst_files[i] = thread_safe_list(512,sizeof(sst_f_inf),false); 
    }
    for (size_t i = 0; i < MAX_LEVELS; i++)
    {
        meta->levels_with_data[i] = -1;
    }
    
   
   //meta->dict_buffer = (char*)calloc(1,BUFFER_SIZE);
}
meta_data * load_meta_data(char * file, char * bloom_file){
    meta_data * meta = (meta_data*)wrapper_alloc((sizeof(meta_data)), NULL,NULL);
    if (meta == NULL) return NULL;
    meta->err_code = OK;
    byte_buffer * tempBuffer = create_buffer(DICT_BUFFER_SIZE);
    if (tempBuffer == NULL) return NULL;
    int bytes;
    if ((bytes = read_file(tempBuffer->buffy,file, "rb",1,0)) <= 0) {
        fresh_meta_load(meta);
        free_buffer(tempBuffer);
        save_snap(meta);
        return meta;
    }
    int size=  sizeof(size_t);
    read_buffer(tempBuffer, &meta->shutdown_status, sizeof(meta->shutdown_status));
    read_buffer(tempBuffer, &meta->num_sst_file, size);
    read_buffer(tempBuffer, &meta->file_ptr, size);
    read_buffer(tempBuffer, &meta->db_length, size);
    read_buffer(tempBuffer, &meta->base_level_size, size);
    read_buffer(tempBuffer, &meta->min_c_ratio, size);
    read_buffer(tempBuffer, meta->levels_with_data, sizeof(int)*MAX_LEVELS);
    int lengths[MAX_LEVELS];
    read_buffer(tempBuffer, lengths, sizeof(int)*MAX_LEVELS);
    for (int i = 0; i < MAX_LEVELS; i++){
         if (lengths[i] <=0){
            meta->sst_files[i] = thread_safe_list(16,sizeof(sst_f_inf),false);
            if (meta->sst_files[i] == NULL){
                return NULL;
            }
            continue;
        }
        meta->sst_files[i] = thread_safe_list(16,sizeof(sst_f_inf),false);
        int ret = read_sst_list(meta->sst_files[i], tempBuffer,  lengths[i]);
        if (ret !=OK) {
            meta->err_code = ret;
        }
    }
    size_t readSize= 0;
    read_buffer(tempBuffer, &readSize, size);
    free_buffer(tempBuffer);
    tempBuffer = NULL;
    //save_snap(meta);

    return meta;
}
 void dump_sst_list(list * sst_files, FILE * f){
     for (int i = 0; i < sst_files->len; i++){
         sst_f_inf * sst = at(sst_files,i);
         fwrite(sst->file_name, 1,strlen(sst->file_name)+1,f);
         fwrite(&sst->length, 1,sizeof(size_t),f);
         fwrite(&sst->compressed_len, 1,sizeof(size_t),f);
         fwrite(&sst->use_dict_compression, 1,sizeof(bool),f);
         fwrite(&sst->compr_info.dict_offset, 1,sizeof(sst->compr_info.dict_offset), f);
         fwrite(&sst->compr_info.dict_len,1,sizeof(sst->compr_info.dict_len),f);
         fwrite_fstr(sst->max, f);
         fwrite_fstr(sst->min, f);
         fwrite(&sst->time, 1, sizeof(sst->time), f);
         if (sst->block_indexs == NULL) continue;
         fwrite(&sst->block_start, 1, sizeof(size_t), f);
         size_t temp = sst->block_indexs->len;
         fwrite(&temp, 1, sizeof(size_t), f);
         // Don't free the SST files here, they will be freed in free_md
         // free_sst_inf(sst);
         // sst = NULL;
     }
 }
 void write_md_d(char * file, meta_data * meta, byte_buffer * temp_buffer){
       FILE * f = fopen(file, "wb");
       meta->shutdown_status = 0;
       fwrite(&meta->shutdown_status, sizeof(meta->shutdown_status), 1, f);
       fwrite(&meta->num_sst_file, sizeof(size_t),1, f);
       fwrite(&meta->file_ptr, sizeof(size_t),1, f);
       fwrite(&meta->db_length, sizeof(size_t),1, f);
       fwrite(&meta->base_level_size, sizeof(size_t),1, f);
       fwrite(&meta->min_c_ratio, sizeof(size_t),1, f);
       int j = 0;
       /* first loop is to get the info of which sst lists are actually written*/
       for (int i = 0; i < MAX_LEVELS; i++){
            if (meta->sst_files[i] == NULL || meta->sst_files[i]->len <=0) continue;
            meta->levels_with_data[j] = i;
            j++;
       }
       for (; j < MAX_LEVELS; j++)
       {
            meta->levels_with_data[j] = -1;
       }
       
       fwrite(meta->levels_with_data, sizeof(int), MAX_LEVELS, f);
       /*the actual writing occurs here*/
        for (int i = 0; i < MAX_LEVELS; i++){
            fwrite(&meta->sst_files[i]->len, sizeof(int), 1, f);
        }
       
       for (int i = 0; i < MAX_LEVELS; i++){
            if (meta->sst_files[i] == NULL || meta->sst_files[i]->len <=0) continue;
            dump_sst_list(meta->sst_files[i], f);
       }
       fwrite(&temp_buffer->curr_bytes, sizeof(size_t),1, f);
       fclose(f);
}
 void free_md(meta_data * meta){
    
    for (int i = 0 ; i < MAX_LEVELS; i++){
        if (meta->sst_files[i] == NULL) continue;
        free_list(meta->sst_files[i], &free_sst_inf);
        
    }
    //if (meta->current_tx_copy != NULL) free(meta->current_tx_copy);
    free(meta);
    meta = NULL;
}
void destroy_meta_data(char * file,char * bloom_file, meta_data * meta){
    byte_buffer* temp_buffer = create_buffer(DICT_BUFFER_SIZE);
    if (file!= NULL){
       write_md_d(file, meta, temp_buffer);
    }
    
    free_md(meta);
    free_buffer(temp_buffer);
}
void update_md(){
    
}
