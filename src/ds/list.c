
#include "list.h"


list* List(int Size, int dataTypeSize, bool alloc){
    list * my_list  = (list*)wrapper_alloc((sizeof(list)), NULL,NULL);
    if (my_list == NULL) return NULL;
    my_list->cap = 0;
    my_list->len =0;
    if (Size < 16){
        my_list->arr = wrapper_alloc((16* dataTypeSize), NULL,NULL);
        my_list->cap= 16;
    }
    else {
        my_list->arr = wrapper_alloc((Size* dataTypeSize), NULL,NULL);
        my_list->cap= Size;
    }
    if (my_list->arr == NULL){
        free(my_list);
        return NULL;
    }
    my_list->isAlloc=alloc;
    my_list->dtS = dataTypeSize;
    my_list->thread_safe = false;
    return my_list;
}
list * thread_safe_list(int Size, int dataTypeSize, bool alloc){
    list * my_list  = (list*)wrapper_alloc((sizeof(list)), NULL,NULL);
    if (my_list == NULL) return NULL;
    my_list->cap = 0;
    my_list->len =0;
    if (Size < 16){
        my_list->arr = wrapper_alloc((16* dataTypeSize), NULL,NULL);
        my_list->cap= 16;
    }
    else {
        my_list->arr = wrapper_alloc((Size* dataTypeSize), NULL,NULL);
        my_list->cap= Size;
    }
    if (my_list->arr == NULL){
        free(my_list);
        return NULL;
    }
    my_list->isAlloc=alloc;
    my_list->dtS = dataTypeSize;
    my_list->thread_safe = true;

    return my_list;
}
void expand(list * my_list){
    if (my_list == NULL){
        return;
    }
    my_list->cap *=2;
    void * arr  =  realloc(my_list->arr, my_list->dtS * (my_list->cap));
    if (arr == NULL){
        return;
    }
    my_list->arr= arr;
    return;
}
void insert(list * my_list, void * element){
    if (my_list == NULL || element == NULL) {
        return;
    }
    //if (my_list->thread_safe) w_lock(&my_list->lock);
    if (my_list->len >= my_list->cap){
        expand(my_list);
    }
    //void pointers need to be typecasted, use (char*) so we start with value 1
    memmove((char *)my_list->arr + (my_list->len * my_list->dtS), element, my_list->dtS);
    my_list->len++;
    return;
}
void* internal_at_unlocked(list * my_list, int index){
     if (my_list == NULL || index < 0 || index >= my_list->len) {
        return NULL;
    }
    void * ret = (char*)my_list->arr + (index * my_list->dtS);
    return ret;
}
void inset_at_unlocked(list * my_list, void * element, int index){
    if (my_list == NULL || element == NULL || index < 0 || index > my_list->cap) {
        return;
    }
    if (my_list->len >= my_list->cap){
        expand(my_list);
    }
    void * temp;
    if ((temp= internal_at_unlocked(my_list, index)) !=  NULL){
        my_list->len--;
    }
    //if (my_list->thread_safe) w_lock(&my_list->lock);
    memmove((char *)my_list->arr + index * my_list->dtS, element, my_list->dtS);
    my_list->len++;
   // unlock(&my_list->lock);
}
void inset_at(list * my_list, void * element, int index){
    if (my_list == NULL || element == NULL || index < 0 || index > my_list->cap) {
        return;
    }
    if (my_list->len >= my_list->cap){
        expand(my_list);
    }
    void * temp;
    if ((temp= internal_at_unlocked(my_list, index)) !=  NULL){
        my_list->len--;
    }
    //if (my_list->thread_safe) w_lock(&my_list->lock);
    memmove((char *)my_list->arr + index * my_list->dtS, element, my_list->dtS);
    my_list->len++;
   // unlock(&my_list->lock);
}
  /*DO NOT TOUCH THIS UNLESS YOU WANT TO SPEND HOURS DEBUGGING DUMB STUFF*/
void free_list(list * my_list, void (*free_func)(void*)){
   if (my_list == NULL){
        return;
   }
   //if(my_list->thread_safe) w_lock(&my_list->lock);
   for (int i = 0; i < my_list->cap ;i++ ){
        char **temp = (char**)at(my_list, i);
        if (temp == NULL || my_list->isAlloc==false ) continue;
        if (free_func == NULL)free(*temp);
        else (*free_func)(temp);
        temp = NULL;
   }
   //if(my_list->thread_safe) unlock(&my_list->lock);
   if (my_list->arr == NULL) return;
   free(my_list->arr);
   my_list->arr = NULL;
   free(my_list);
}
void remove_at(list *my_list, int index) {
    
    if (my_list == NULL || index < 0 || index >= my_list->len) {
        return; 
    }
    memset((char *)my_list->arr + index * my_list->dtS, 0,my_list->dtS);
    for (int i = index; i < my_list->len - 1; i++) {
        memmove((char *)my_list->arr + i * my_list->dtS, (char *)my_list->arr + (i + 1) * my_list->dtS, my_list->dtS);
    }
    my_list->len--;
}
void remove_at_unlocked(list *my_list, int index) {
    
    if (my_list == NULL || index < 0 || index >= my_list->len) {
        return; 
    }
    memset((char *)my_list->arr + index * my_list->dtS, 0,my_list->dtS);
    for (int i = index; i < my_list->len - 1; i++) {
        memmove((char *)my_list->arr + i * my_list->dtS, (char *)my_list->arr + (i + 1) * my_list->dtS, my_list->dtS);
    }
    my_list->len--;
}
void remove_no_shift(list *my_list, int index, void (*free_func)(void*)){
    void * element = at(my_list, index);
    if (element == NULL) return;
    if (free_func != NULL) (*free_func)(element);
    else if (my_list->isAlloc) free(element);
    element = NULL;
}
void * at(list *my_list, int index){
    if (my_list == NULL || index < 0 || index >= my_list->len) {
        return NULL;
    }
    //if (my_list->thread_safe) r_lock(&my_list->lock);
    void * ret = (char*)my_list->arr + (index * my_list->dtS);
    //if (my_list->thread_safe) unlock(&my_list->lock);
   
    return ret;
}
void at_copy(list *my_list, void * storage, int index){
    if (my_list == NULL || index < 0 || index >= my_list->len) {
        return;
    }

    //if (my_list->thread_safe) r_lock(&my_list->lock);
    void * ret = (char*)my_list->arr + (index * my_list->dtS);
    
    memcpy(storage, ret, my_list->dtS);
    //if (my_list->thread_safe) unlock(&my_list->lock);
  
    return;
}
void swap(void * a, void * b, size_t len)
{
    char temp[len];
    
    memcpy(temp, a, len);
    
    memcpy(a, b, len);
    
    memcpy(b, temp, len);
}
int compare_values_by_key(void * one,  void * two){
    return strcmp(((kv *)one)->key, ((kv *)two)->key);
}
int compare_string_values(void* one, void*two){
    return strcmp((char*)one,(char*)two);
}
/*TODO: MERGE SORT WHEN THE NEED ARISES (LISTS > 50 ELEMENTS)*/   
void sort_list(list *my_list, bool ascending, compare func){
    //insertion is faster for smaller lists
    if (my_list == NULL || my_list->len <= 1) {
        return;
    }
    if (my_list->len < 50){
        if (ascending){
            for (int i = 1; i < my_list->len; i++){
                int j = i;
                while( j > 0 && func(at(my_list, j), at(my_list,j-1)) < 0){
                    swap(at(my_list, j), at(my_list,j-1), my_list->dtS);
                    j--;
                }
            }
        }
        else{
            for (int i = 1; i < my_list->len; i++){
                int j = i;
                while( j > 0 && func(at(my_list, j), at(my_list,j-1)) > 0){
                    swap(at(my_list, j), at(my_list,j-1), my_list->dtS);
                    j--;
                }
            }
        }
    }
    return;
}
bool check_in_list(list * my_list, void * element, bool(*func)(void*, void*)){
    if (my_list == NULL || element == NULL) return false;
    for (int i = 0; i < my_list->len; i++){
        if (func(at(my_list, i), element) == 0) return true;
    }
    return false;
}
bool compare_size_t (void * one, void * two){
    return *(size_t*)one == *(size_t*)two;
}
void shift_list(list * my_list, int start_index){
    for (int i = start_index; i < my_list->len - 1; i++) {
        memmove((char *)my_list->arr + i * my_list->dtS, (char *)my_list->arr + (i + 1) * my_list->dtS, my_list->dtS);
    }
}
void * get_last(list * my_list){
    return at(my_list, my_list->len -1);
}
int dump_list_ele(list * my_list, int(*item_write_func)( byte_buffer*, void*), byte_buffer * stream, int num, int start){
    int loop_len = min(start+num, my_list->len);
    for (int i = start; i < loop_len; i++){
        (*item_write_func)(stream, at(my_list, i));
    }
    
    return loop_len;
}
int merge_lists(list * merge_into, list * merge_from, compare func){

    int i=0;
    int j=0;
    list * temp = List(0, merge_into->dtS, merge_from->isAlloc);

    int old_len =  merge_into->len;;
    int total = merge_into->len + merge_from->len;
    while(i < merge_into->len && j < merge_from->len){
        void * merge_fr_ele = at(merge_from, j);
        void * merge_int_ele =  internal_at_unlocked(merge_into, i);
        if (func(merge_fr_ele, merge_int_ele)  < 0 ){
            insert(temp,merge_fr_ele);
            j++;
        }
        else{
            insert(temp,merge_int_ele);
            i++;
        }   
    }
    if (i >= merge_into->len){
        for (; j < merge_from->len; j++){
            insert(temp, at(merge_from, j));
        }   
    }
    else{
        for (; i < merge_into->len; i++){
            insert(temp, internal_at_unlocked(merge_into, i));
        }
    }
    for(int k = 0; k < temp->len; k++){
        inset_at_unlocked(merge_into, at(temp, k), k);
    }
    fprintf(stdout, "merging %d with %d for %d total, actually got: %d\n", merge_from->len, old_len, total, merge_into->len);

    free_list(temp, NULL);
    return 0;

}

