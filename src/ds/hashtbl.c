#include "hashtbl.h"

uint64_t fnv1a_64(const void *data, size_t len) {
    const uint8_t *bytes = (const uint8_t *)data;
    uint64_t hash = FNV_OFFSET_64;
    
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint64_t)bytes[i];
        hash *= FNV_PRIME_64;
    }
    
    return hash;
}
kv KV(void* k, void* v){
    kv myKv;
    myKv.value = v;
    myKv.key = k;
    return myKv;
}
int compare_kv(kv *k1, kv *k2){
    if (k1 == NULL || k2 == NULL || k1->key == NULL || k2->key == NULL){
        return -1;
    }
    if (strcmp((char*)k1->key, (char*)k2->key) == 0 && strcmp((char*)k1->value, (char*)k2->value) == 0){
        return 0;
    }
    return -1;
}
int compare_kv_v(const void * kv1, const void * kv2){
    const kv * k1 = kv1;
    const kv * k2 = kv2;
    if (k1 == NULL || k2 == NULL){
        return -1;
    }
    
    return strcmp(k1->key, k2->key);
}
kv* dynamic_kv(void* k, void* v){
    kv * myKv = (kv *)wrapper_alloc((sizeof(kv)), NULL,NULL);
    if (myKv == NULL) return NULL;
    myKv->value = v;
    myKv->key = k;
    return myKv;
}