#include "compactor.h"
#define NUM_THREADP 1
#define NUM_COMPACT 10
#define MAX_COMPARE 5
#define NUM_META_DATA 3
#define NUM_THREAD 10
#define MAX_SST_LENGTH 100000000
#define VICTIM_INDEX 1
#define MB 1048576
#define KB 1024
#define GB 1073741824
#define PREV 1
#define END_OF_BLOCK 0x04
#define NUM_PAGE_BUFFERS 15
#define NUM_BIG_BUFFERS 5


/*sets the cm with the most re*/
int sst_deep_copy(sst_f_inf * master, sst_f_inf * copy){
    return 0;
}
void set_cm_sst_files(meta_data * meta, compact_manager * cm){
    for (int i =0; i < MAX_LEVELS; i++){
        if (meta->sst_files[i] == NULL) continue;
        cm->sst_files[i]= thread_safe_list(0, sizeof (sst_f_inf), false);
        for (int j = 0; j < meta->sst_files[i]->len; j++){
            sst_f_inf f;
            sst_f_inf * sst_to_copy=  at(meta->sst_files[i],j);
            int ret =sst_deep_copy(sst_to_copy, &f);
            if (ret !=OK) return;
            insert(cm->sst_files[i], &f);
        }
    }
}

int compare_jobs(const void * job1, const void * job2){
    const compact_job_internal * job_1 = job1;
    const compact_job_internal * job_2 = job2;

    return job_1->id > job_2->id;
}
compact_manager * init_cm(meta_data * meta, shard_controller * c){
    compact_manager * manager = (compact_manager*)wrapper_alloc((sizeof(compact_manager)), NULL,NULL);
    if (manager == NULL) return NULL;
    manager->sst_files = meta->sst_files;
    //set_cm_sst_files(meta, manager);
    manager->base_level_size = meta->base_level_size;
    manager->min_compact_ratio = meta->min_c_ratio; 
    manager->dict_buffer_pool = create_pool(NUM_PAGE_BUFFERS);
    /*come back here*/
    int  size = 0;
    for (int i = 0; i < NUM_PAGE_BUFFERS; i++){
        insert_struct(manager->dict_buffer_pool, create_buffer(size));
    }
    manager->c = c;
    manager->job_queue = Frontier(sizeof(compact_job_internal), false, &compare_jobs);

    manager->check_meta_cond = false;
    manager->exit = false;
    manager->lvl0_job_running= false;
   
    return manager;
}

int compare_time_stamp(struct timeval t1, struct timeval t2){
    if (t1.tv_sec < t2.tv_sec) return -1;    
    else if (t1.tv_sec > t2.tv_sec) return 1;

    if (t1.tv_usec < t2.tv_usec) return -1;
    else if (t1.tv_usec > t2.tv_usec) return 1;
    return 0;
}

int entry_len(merge_data entry){
    return entry.key.len + sizeof(entry.key.len) + entry.value.len + sizeof(entry.value.len);

}
merge_data discard_same_keys(frontier *pq, sst_iter *its, shard_controller * c , merge_data initial) {
    merge_data best_entry = initial;
    merge_data current = initial;

    while (pq->queue->len > 0) {
        merge_data *next_in_line = peek(pq);
        if (next_in_line == NULL || compare_merge_data(&current, next_in_line) != 0) {
            break;
        }

        dequeue(pq, &current);

        
        merge_data next_entry = next_key_block(&its[(int)current.index], c);
        if (next_entry.key.entry != NULL && strncmp(next_entry.key.entry, TOMB_STONE, next_entry.key.len)!=0) {
            next_entry.index = current.index;
            enqueue(pq, &next_entry);
        }

        
        if (strncmp(current.key.entry, TOMB_STONE, current.key.len) != 0 && compare_time_stamp(its[(int)current.index].file->time, its[(int)best_entry.index].file->time) > 0) {
            best_entry = current;
        }
    }

    return best_entry;
}
void complete_block(sst_f_inf * curr_sst, block_index * current_block, int block_b_count){
    current_block->len =  block_b_count;
    if (current_block->len <3 ) return;
    build_index(curr_sst,current_block, NULL, current_block->num_keys,current_block->offset);
    memset(current_block, 0, sizeof(*current_block));
    *current_block = create_ind_stack(10 *(GLOB_OPTS.BLOCK_INDEX_SIZE/(4*1024)));
}
void reset_block_counters(int *block_b_count, int *sst_b_count){
    *block_b_count = 2;
    *sst_b_count +=2;
}
static int write_blocks_to_file(byte_buffer *dest_buffer, byte_buffer * compression_buffer, byte_buffer * dict_buffer, sst_f_inf* curr_sst, list * entrys, db_FILE *curr_file, 
uint64_t l_size) {
    int keys = 0;
    int skipped = 0;


    if (compression_buffer == NULL || compression_buffer->max_bytes < l_size ){
        curr_sst->use_dict_compression = false;
    }
    else{
        curr_sst->use_dict_compression = true;
    }
    bool compress = (GLOB_OPTS.compress_level > -5) && curr_sst->use_dict_compression;

    byte_buffer *initial_write_buffer = compress ? compression_buffer : dest_buffer;
    reset_buffer(initial_write_buffer);

    for (int i = 0; i < curr_sst->block_indexs->len; i++) {
        block_index *ind = at(curr_sst->block_indexs, i);
        int loc = initial_write_buffer->curr_bytes; 

        write_buffer(initial_write_buffer, (char*)&ind->num_keys, 2);
        keys = dump_list_ele(entrys, &write_db_entry, initial_write_buffer, ind->num_keys, keys);
        grab_min_b_key(ind, initial_write_buffer, loc);

        if (is_avx_supported()){
            ind->checksum =  crc32_avx2((uint8_t*)buf_ind(initial_write_buffer, loc), ind->len);
        }
        else{
            ind->checksum = crc32((uint8_t*)buf_ind(initial_write_buffer, loc), ind->len);
        }
        ind->offset = loc;

        if (!compress) {
            int bytes_to_skip = GLOB_OPTS.BLOCK_INDEX_SIZE - (initial_write_buffer->curr_bytes - loc);
            skipped += bytes_to_skip;
            initial_write_buffer->curr_bytes += bytes_to_skip;
        }
    }
    curr_sst->length = initial_write_buffer->curr_bytes;
    if (compress) {
        reset_buffer(dest_buffer); // Prepare dest buffer for compressed output
        // Assumes handle_compression compresses from compression_buffer to dest_buffer
        // AND updates ind->offset for each block within curr_sst->block_indexs based on compressed layout
        handle_compression(curr_sst, compression_buffer,dest_buffer, dict_buffer);
        curr_sst->compressed_len = dest_buffer->curr_bytes;
        curr_sst->block_start = curr_sst->compressed_len; // Metadata starts after compressed data
    } 
    else {
        // If not compressing, dest_buffer already holds the final data and offsets are correct.
        curr_sst->compressed_len = 0;
        curr_sst->block_start = curr_sst->length; // Metadata starts after uncompressed data
    }
    curr_sst->block_start = dbio_write(curr_file, 0, dest_buffer->curr_bytes);
    //dbio_fsync(curr_file); No point in an fsync; we fsync on METADATA write
    return skipped;
}
void process_non_cached(byte_buffer *dest_buffer, sst_f_inf *curr_sst, db_FILE * curr_file ,compact_job_internal*job, int sst_b_count, list * block_indexes){
    reset_buffer(dest_buffer);
    block_index * b_0= at(block_indexes,0);
    f_cpy(&curr_sst->min, &b_0->min_key);
    dump_sst_meta_part(curr_sst, dest_buffer, curr_sst->block_start, block_indexes);
    if (curr_sst->compr_info.dict_len > 0){
        write_buffer(dest_buffer, curr_sst->compr_info.dict_buffer, curr_sst->compr_info.dict_len);
    }
    dbio_write(curr_file, curr_sst->block_start, dest_buffer->curr_bytes);
    dbio_fsync(curr_file);
    dbio_close(curr_file);

    gettimeofday(&curr_sst->time, NULL);
    insert(job->new_sst_files, curr_sst);
    memset(curr_sst, 0, sizeof(*curr_sst));
}
void process_sst(byte_buffer *dest_buffer, sst_f_inf *curr_sst, db_FILE * curr_file ,compact_job_internal*job, int sst_b_count) {
    reset_buffer(dest_buffer);
    block_index * b_0= at(curr_sst->block_indexs,0);
    f_cpy(&curr_sst->min, &b_0->min_key);
    all_index_stream(curr_sst->block_indexs->len, dest_buffer,curr_sst->block_indexs);
    copy_filter(curr_sst->filter, dest_buffer);
    if (curr_sst->compr_info.dict_len > 0){
        write_buffer(dest_buffer, curr_sst->compr_info.dict_buffer, curr_sst->compr_info.dict_len);
    }
     /*write bloom filters/file metadata*/
    /*then write our dictionary data if applicable*/
    dbio_write(curr_file, curr_sst->block_start, dest_buffer->curr_bytes);
    dbio_fsync(curr_file);
    dbio_close(curr_file);

    gettimeofday(&curr_sst->time, NULL);
    insert(job->new_sst_files, curr_sst);
    memset(curr_sst, 0, sizeof(*curr_sst));
}
block_index init_block(arena * mem_store, size_t* off_track){
    block_index current_block=  create_ind_stack(10 *(GLOB_OPTS.BLOCK_INDEX_SIZE/(4*1024)));
    current_block.offset = *off_track;
    *off_track += GLOB_OPTS.BLOCK_INDEX_SIZE;
    return current_block;
}


/*min key copying screwed up*/
int merge_tables(byte_buffer *dest_buffer, byte_buffer * compression_buffer, compact_job_internal * job, byte_buffer * dict_buffer, shard_controller * c) {
    sst_iter  * its = pad_allocate(sizeof(sst_iter)* job->to_merge->len);
    uint64_t dest_l_s = get_opt_file_s(job->end_level);
    frontier *pq = Frontier(sizeof(merge_data), 0, &compare_merge_data);
    if (pq == NULL) return STRUCT_NOT_MADE;
    list * entrys = List(0, sizeof(merge_data), false);
    if (entrys== NULL) {
        free_front(pq);
        return STRUCT_NOT_MADE;
    }
    for (int i = 0; i < job->to_merge->len; i++) {
        sst_f_inf * sst = at(job->to_merge,i);
        init_sst_iter(&its[i], sst);
        cache_entry ce = retrieve_entry_sharded(*c, its[i].cursor.index, sst->file_name, sst);
        if (ce.buf == NULL){
            free(its);
            return INVALID_DATA;
        }
        its[i].cursor.ce = ce;
        merge_data first_entry = next_key_block(&its[i], c);
        first_entry.index = i;
        enqueue(pq, &first_entry);
    }
    // block counter determines when to write a block. sst_b_count tracks ssts
    int block_b_count = 2;
    int sst_b_count = 2;
   
    size_t sst_offset_tracker = 0;
    sst_f_inf * curr_sst=  pad_allocate(sizeof(sst_f_inf));
    *curr_sst = create_sst_empty();
    generate_unique_sst_filename(curr_sst->file_name, MAX_F_N_SIZE, job->end_level);
    curr_sst->block_start = dest_l_s ;
    

    db_FILE * curr_file = dbio_open(curr_sst->file_name, 'w');
    set_context_buffer(curr_file, dest_buffer);
    
    block_index * current_block = pad_allocate(sizeof(block_index));
    *current_block = init_block(curr_sst->mem_store, &sst_offset_tracker);
    //sst_offset_tracker = 0;
    
    f_str last_key;
  
   /*i strongly reccomend not touching this loop, edge case galore thats easy to fuck up*/
    while (pq->queue->len > 0) {
        merge_data current;
        dequeue(pq, &current);

      
        merge_data next_entry = next_key_block(&its[(int)current.index], c);
        if (next_entry.key.entry != NULL) {
            next_entry.index = current.index;
            enqueue(pq, &next_entry);
        }
        merge_data best_entry = discard_same_keys(pq, its, c, current);
        if (entry_len(best_entry) > 4096){
            printf("here\n");
        }
        if (block_b_count + entry_len(best_entry) >= GLOB_OPTS.BLOCK_INDEX_SIZE){
            complete_block(curr_sst, current_block, block_b_count);
            reset_block_counters(&block_b_count, &sst_b_count);
            *current_block = init_block(curr_sst->mem_store, &sst_offset_tracker);
            merge_data * this_block_max = get_last(entrys);
            f_cpy(&curr_sst->max, &this_block_max->key);
        }
        if (sst_offset_tracker + entry_len(best_entry) >= dest_l_s  || (block_b_count == 0 && sst_offset_tracker == dest_l_s )){
            
            complete_block(curr_sst, current_block, block_b_count);
            reset_block_counters(&block_b_count, &sst_b_count);
            
            write_blocks_to_file(dest_buffer,compression_buffer,dict_buffer, curr_sst, entrys, curr_file, dest_l_s);
           
            merge_data *  max = get_last(entrys);
            if (max == NULL){
                f_cpy(&curr_sst->max, &last_key);
            }
            else{
                f_cpy(&curr_sst->max, &max->key);
            }
            process_sst(dest_buffer, curr_sst, curr_file, job, sst_b_count);
            *curr_sst = create_sst_empty();
            curr_sst->block_start=  dest_l_s ;
           
            block_b_count = 2;
            entrys->len = 0;
            
            sst_b_count = 2;
            sst_offset_tracker = 0;

            generate_unique_sst_filename(curr_sst->file_name, MAX_F_N_SIZE, job->end_level);
            curr_file = dbio_open(curr_sst->file_name, 'w');
            set_context_buffer(curr_file, dest_buffer);
            if (curr_file == NULL) return FAILED_OPEN;
            *current_block = init_block(curr_sst->mem_store, &sst_offset_tracker);
        }
        if (best_entry.key.entry!=NULL) {
            insert(entrys, &best_entry);
            current_block->num_keys ++;
            map_bit(best_entry.key.entry, curr_sst->filter);
            int best_entry_bytes  = entry_len(best_entry);
            block_b_count +=  best_entry_bytes;
            sst_b_count += best_entry_bytes;
        }
    }
    if (block_b_count > 0){
        complete_block(curr_sst, current_block, block_b_count);
        reset_block_counters(&block_b_count, &sst_b_count);
    }
    if (entrys->len > 0) {
        write_blocks_to_file(dest_buffer,compression_buffer,dict_buffer, curr_sst, entrys, curr_file, dest_l_s);
        merge_data * max = get_last(entrys);
        f_cpy(&curr_sst->max, &max->key);
        process_sst(dest_buffer, curr_sst, curr_file, job, sst_b_count);
    }
    free_front(pq);
    free_list(entrys, NULL);
    dbio_close(curr_file);
    return 0; //INVALID_DATA is 0 so default is 1 
}

int calculate_overlap(const f_str min1,  const f_str max1, const f_str min2, const f_str max2) {  

    if (f_cmp(min1, max2) >= 0) 
        return -1;
    if (f_cmp(min2, max1) >= 0) 
        return -1;
    return 0;
}

int get_level_size(int level){
    uint32_t size=  (GLOB_OPTS.LEVEL_0_SIZE * GLOB_OPTS.SST_TABLE_SIZE_SCALAR) * level;
    return size;
}
/*removes the sst when target is actually the same pointer*/
int remove_same_sst(sst_f_inf * target, list * target_lvl, int level){

    char * key_to_check = target->file_name;
    int ind;
    /*level 0 has no gurantee of ordering*/
    
    ind = find_sst_file_eq_iter(target_lvl, target_lvl->len, key_to_check);

    
    if (ind < 0) {
        return ind;
    }
    sst_f_inf * candiate = internal_at_unlocked(target_lvl, ind);
    if (!sst_equals(candiate, target)){
        return -1;
    } /*we compare the block index lists because that is a pointer. since we are dealing with structs, we cannot just compare the structs, as they may differ */
    fprintf(stdout, "removed %s in a trival move\n", candiate->file_name);
    memset(candiate, 0, sizeof(*candiate));
    remove_at_unlocked(target_lvl,  ind);

    return 0;
}

int remove_sst_from_list(sst_f_inf * target, list * target_lvl, int level){
    char * key_to_check = target->file_name;
    if (key_to_check == NULL) return -1;
    int ind;
    /*level 0 has no gurantee of ordering*/
    
  
    ind = find_sst_file_eq_iter(target_lvl, target_lvl->len, key_to_check);
  
    if (ind < 0) {
   
        return ind;
    }
    sst_f_inf * candiate = internal_at_unlocked(target_lvl, ind);
    if (!sst_equals(candiate, target)){
      
        return -1;
    } /*we compare the block index lists because that is a pointer. since we are dealing with structs, we cannot just compare the structs, as they may differ */
    if (!candiate->marked) remove(candiate->file_name);
    free_sst_inf(candiate);
    remove_at_unlocked(target_lvl,  ind);
  
    return 0;
}

void compact_one_table(compact_manager * cm, compact_job_internal job,  sst_f_inf * victim){
    if (victim == NULL) return;
   
    list * ssts_to_merge = job.to_merge;
    
    /*trival move*/
    if (ssts_to_merge->len <= 1 && job.start_level != 0) {
        char buf[64];
        int res  = remove_same_sst(victim, cm->sst_files[job.start_level], job.start_level);
        if (res >= 0){
            res = 1;
        }
        else {
            fprintf(stdout,"failed to remove %s\n", victim->file_name);
            res = -1;
        }
        fprintf(stdout, "%d removed from 1\n", res);
        generate_unique_sst_filename(buf, 64, job.end_level);
        rename(victim->file_name, buf);
        memset(victim->file_name, 0, strlen(victim->file_name) + 1);
        memcpy(victim->file_name, buf, strlen(buf)+ 1);
        merge_lists(cm->sst_files[job.end_level], job.to_merge, &cmp_sst_f_inf);
        return;

    }
    uint64_t dest_l_s = get_opt_file_s(job.end_level);
    byte_buffer* dict_buffer = request_struct(cm->dict_buffer_pool);
    byte_buffer * dest_buffer = select_buffer(dest_l_s );
    byte_buffer * compression_buffer=  select_buffer(dest_l_s );
    int result = merge_tables (dest_buffer,compression_buffer, &job, dict_buffer, cm->c);
    if (result == INVALID_DATA){
        return_struct(cm->dict_buffer_pool, dict_buffer, &reset_buffer);
        return_buffer(dest_buffer);
        return_buffer(compression_buffer);

        if(job.start_level == 0){
            cm->lvl0_job_running = false;
        }
      
        return;
    }
    integrate_new_tables(cm, &job, ssts_to_merge);
    free_list(ssts_to_merge, NULL);

 
    return_struct(cm->dict_buffer_pool, dict_buffer, &reset_buffer);
    return_buffer(dest_buffer);
    return_buffer(compression_buffer);

    if(job.start_level == 0){
        cm->lvl0_job_running = false;
    }
    return;
}
void integrate_new_tables(compact_manager *cm, compact_job_internal *job, list *old_ssts) {

    list *start_level = cm->sst_files[job->start_level];
    list *search_level = cm->sst_files[job->search_level];
    list * end_level= cm->sst_files[job->end_level];

    const int victim_index= 0;
    /*need a better system for random file names*/
    /*remove all ssts from the search level data structure. edgecase where start != search IS covered at the first call*/
    remove_sst_from_list(at(old_ssts,victim_index), start_level, job->start_level);
    for (int i = 0; i < old_ssts->len ;i++){
        remove_sst_from_list(at(old_ssts, i), search_level, job->search_level);
    }
    /*case where multiple level 0 ssts were selected.*/
    for (int i = 0; i < old_ssts->len ; i++){
        remove_sst_from_list(at(old_ssts, i), cm->sst_files[0],0);

    }
    //job->new_sst_files are in order, so we can just do a 2 way merge with the end level*/
    merge_lists(end_level, job->new_sst_files ,&cmp_sst_f_inf);

    free_list(job->new_sst_files, NULL);
    
    return;
}
void shutdown_cm(compact_manager * cm){
    cm->exit = true;
    cm->check_meta_cond = true;
}
void free_cm(compact_manager * manager){
    free_pool(manager->dict_buffer_pool, &free_buffer);
    free_front(manager->job_queue);
    free(manager);
    manager = NULL;
}