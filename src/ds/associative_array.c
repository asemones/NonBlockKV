#include "associative_array.h"
k_v_arr* create_k_v_arr (size_t size){
    k_v_arr * ar = malloc(sizeof(k_v_arr));
    if (ar == NULL) return NULL;
    ar->keys =  malloc(size * sizeof(f_str));
    ar->values =  malloc(size * sizeof(f_str));
    if (ar->keys == NULL || ar->values == NULL) return NULL;
    ar->cap = size;
    ar->len = 0;
    return ar;
}
void into_array(void * ar, void * key, void * value){
    k_v_arr * arr = (k_v_arr*)ar;
    arr->values[arr->len] = *((f_str*)value);
    arr->keys[arr->len] = *((f_str*)key);
    arr->len ++;
}

