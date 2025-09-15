#include "snapshot.h"



snapshot * create_snap(){
    snapshot * s = malloc(sizeof(snapshot));
    s->reference_counter = 0;
    s->timestamp = malloc(TIME_STAMP_SIZE);
    s->cache_ref = NULL;
    return s;
}
int init_snapshot(snapshot * master, snapshot * s, shard_controller*c){
    pthread_mutex_init(&s->counter_lock, NULL);
    for (int i =0; i < LEVELS; i++){
        s->sst_files[i]= thread_safe_list(0, sizeof (sst_f_inf), false);
        if (master == NULL|| master->sst_files[i]== NULL) continue;
        for (int j = 0; j < master->sst_files[i]->len; j++){
            sst_f_inf f;
            sst_f_inf * sst_to_copy=  at(master->sst_files[i],j);
            int ret =sst_deep_copy(sst_to_copy, &f);
            if (ret !=OK) return ret;
            insert(s->sst_files[i], &f);
        }
    }
    grab_time_char(s->timestamp);
    s->cache_ref = c;
    return OK;
}
int destroy_snapshot(snapshot * s){
    if (s->reference_counter > 0){
        return PROTECTED_RESOURCE;
    }
    pthread_mutex_destroy(&s->counter_lock);
    for (int i = 0; i < LEVELS; i++ ){
        for (int j= 0;  i < s->sst_files[i]->len; j++){
            sst_f_inf * f = at(s->sst_files[i],j);
            if (f->marked){
                remove(f->file_name);
            }
        }
        free_list(s->sst_files[i], &free_sst_inf);
    }
    free(s->timestamp);
    free(s);
    s->cache_ref =  NULL;
    return OK;
}

void ref_snapshot(snapshot * s){
    pthread_mutex_lock(&s->counter_lock);
    s->reference_counter ++;
    pthread_mutex_unlock(&s->counter_lock);
}
void deref_snapshot(snapshot * s){
    pthread_mutex_lock(&s->counter_lock);
    s->reference_counter --;
    pthread_mutex_unlock(&s->counter_lock);
}
void deref_with_free(snapshot * s){
    pthread_mutex_lock(&s->counter_lock);
    s->reference_counter --;
    pthread_mutex_unlock(&s->counter_lock);
    if (s->reference_counter == 0) destroy_snapshot(s);
}