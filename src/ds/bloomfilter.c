#include "bloomfilter.h"


uint32_t MurmurHash3_x86_32(void *key, int len, uint32_t seed) {
    uint8_t *data = ( uint8_t*)key;
    int nblocks = len / 4;
    uint32_t h1 = seed;
    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;

    uint32_t *blocks = (uint32_t *)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }
    uint8_t *tail = (uint8_t*)(data + nblocks * 4);
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
                k1 *= c1; k1 = (k1 << 15) | (k1 >> 17); k1 *= c2; h1 ^= k1;
    }
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    return h1;
}

bit_array* create_bit_array(size_t size){
    bit_array *bit  = (bit_array*) wrapper_alloc((sizeof(bit_array)), NULL,NULL);
    if (bit == NULL) return NULL;
    bit->size = size;
    bit->array = (uint64_t*)calloc(size, sizeof(uint64_t));
    if (bit->array == NULL) return NULL;
    return bit;
}
void set_bit(bit_array *ba, size_t index){
    ba->array[index/64] |= (uint64_t)(1) << (index%64);
}
void clear_bit(bit_array *ba, size_t index){
    ba->array[index/64] &= ~( (uint64_t)(1) << (index%64));
}
bool get_bit(bit_array *ba, size_t index){
    return ( (ba->array[index/64] & ((uint64_t)(1) << (index%64) )) != 0 );
}
void free_bit(bit_array *ba){
    if (ba == NULL) return;
    if (ba->array !=NULL) free(ba->array);
    ba->array = NULL;
    free(ba);
    ba = NULL;
}
bloom_filter* bloom(size_t num_hash, size_t num_word , bool read_files, char * file){
    bloom_filter * b =(bloom_filter *)wrapper_alloc((sizeof(*b)), NULL,NULL);
    if (b== NULL) return NULL;
    if (read_files && file!=NULL){
        char temp [513000];
        int l= read_file(temp,file,"rb",8,2);
        if (l <=0){
            b->ba = create_bit_array(num_word);
            if (b->ba == NULL) return NULL;
            b->num_hash = num_hash;
            return b;
        }
        size_t tsize;
        memcpy(&tsize, temp, sizeof(size_t));
        memcpy(&b->num_hash, temp + sizeof(size_t), sizeof(size_t));
        b->ba = create_bit_array(tsize);
        if (b->ba == NULL) return NULL;
        b->ba->size = tsize;
        char temp2 [(b->ba->size)*sizeof(size_t)+24];
        l =  read_file(temp2,file,"rb",8,b->ba->size*sizeof(size_t));
        if (l <= 0 ){
            return NULL;
        }
        memcpy(b->ba->array, temp2 + 16, tsize*8);

    }
    else{
        b->ba = create_bit_array(num_word);
        if (b->ba == NULL) return NULL;
        b->num_hash = num_hash;
        return b;
    }
    return b;
}
void map_bit(const char * url, bloom_filter *filter){
    if (url== NULL || strncmp(url, "", 1) == 0){
        return;
    }
    for (int i = 0;  i< filter->num_hash; i++){
        uint32_t hash_one = MurmurHash3_x86_32((void*)url, strlen(url),51 + i) % (filter->ba->size * 64);
        set_bit(filter->ba, hash_one);
    }      
}
bool check_bit(const char * url, bloom_filter *filter){
    if (url== NULL || strncmp(url, "", 1) == 0){
        return false;
    }
    for (int i = 0;  i< filter->num_hash; i++){
        uint32_t hash_one = MurmurHash3_x86_32((char*)url, strlen(url),51 + i) % (filter->ba->size * 64);
        if (!get_bit(filter->ba, hash_one)) return false;
    
        
    }
    return true;
}
void dump_filter(bloom_filter *filter, char * file){
    if (filter == NULL) return;
    if (file == NULL){
        free_filter(filter);
        return;
    }
    char bytes[513000];
    size_t offset = 0;
    memcpy(bytes + offset, &filter->ba->size, sizeof(size_t));
    offset += 8;
    memcpy(bytes + offset, &filter->num_hash, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(bytes + offset, filter->ba->array, filter->ba->size*sizeof(uint64_t));
    size_t totalSize = sizeof(size_t) * 2 + (filter->ba->size*sizeof(uint64_t));
    write_file(bytes, file, "wb",1,totalSize);
    free_filter(filter);
}
void copy_filter(bloom_filter * filter, byte_buffer* buffer){
    write_buffer(buffer, (char*)&filter->ba->size, sizeof(size_t));
    write_buffer(buffer, (char*)&filter->num_hash, sizeof(size_t));
    write_buffer(buffer, (char*)filter->ba->array, filter->ba->size*sizeof(uint64_t));
    return;
}
void free_f_list(void * filter){
    bloom_filter * b = (bloom_filter*)filter;
    if (b == NULL) return;
    free_bit(b->ba);
    b->ba = NULL;
}
void free_filter( void*filter){
    bloom_filter * b = (bloom_filter*)filter;
    if (b == NULL) return;
    if (b->ba != NULL)free_bit(b->ba);
   
    b->ba = NULL;
    free(b);
    b = NULL;
}
void reset_filter(bloom_filter * f){
    memset(f->ba->array, 0, f->ba->size);
}
bloom_filter  part_from_stream(byte_buffer * buffy_the_buffer, slab_allocator * allocator){
    bloom_filter b;
    size_t tsize;
    read_buffer(buffy_the_buffer, &tsize, sizeof(size_t));
    
    read_buffer(buffy_the_buffer, &b.num_hash, sizeof(size_t));
    b.ba = slalloc(allocator, sizeof(b.ba));
    b.ba->array = slalloc(allocator, sizeof(uint64_t) *tsize);
    b.ba->size = tsize;
    read_buffer(buffy_the_buffer, b.ba->array, tsize*sizeof(uint64_t));
    return b;
}
bloom_filter * from_stream(byte_buffer * buffy_the_buffer){
    bloom_filter * b = (bloom_filter*)wrapper_alloc((sizeof(bloom_filter)), NULL,NULL);
    size_t tsize;
    read_buffer(buffy_the_buffer, &tsize, sizeof(size_t));
    if (tsize > ~tsize ){
        return NULL;
    }
    read_buffer(buffy_the_buffer, &b->num_hash, sizeof(size_t));
    b->ba = create_bit_array(tsize);
    b->ba->size = tsize;
    read_buffer(buffy_the_buffer, b->ba->array, tsize*sizeof(uint64_t));
    return b;
}
bloom_filter* deep_copy_bloom_filter(const bloom_filter* original) {
    bloom_filter* copy = malloc(sizeof(bloom_filter));
    if (copy == NULL) {
        return NULL;
    }
    copy->num_hash = original->num_hash;
    copy->ba = create_bit_array(copy->ba->size);
    if (copy->ba == NULL) {
        free(copy);
        return NULL;
    }
    memcpy(copy->ba->array, original->ba->array, original->ba->size * sizeof(uint64_t));

    return copy;
}