
#include "byte_buffer.h"
byte_buffer * create_buffer(size_t size){
    byte_buffer * buffer = (byte_buffer*)wrapper_alloc((sizeof(byte_buffer)), NULL,NULL);
    if (buffer== NULL) return NULL;
    buffer->buffy = (char*)calloc(size, 1);
    if (buffer->buffy == NULL) return NULL;
    buffer->max_bytes = size;
    buffer->curr_bytes = 0;
    buffer->read_pointer = 0;
    return buffer;
}
byte_buffer * mem_aligned_buffer(size_t size, size_t alligment){
    if (size < alligment) return NULL;
    byte_buffer * buffer = (byte_buffer*)wrapper_alloc((sizeof(byte_buffer)), NULL,NULL);
    buffer->buffy = (char*)calloc(size, 1);
    
    if (!is_pwr_2(size) || size % alligment !=0){
        return NULL;
    }
    if (!(buffer->buffy = aligned_alloc(alligment, size))){
        free(buffer->buffy);
        free(buffer);
        return NULL;
    }
    buffer->max_bytes = size;
    buffer->curr_bytes = 0;
    buffer->read_pointer = 0;
    return buffer;
    
}
byte_buffer * create_empty_buffer(){
    byte_buffer * buffer = malloc(sizeof(byte_buffer));
    memset(buffer, 0, sizeof(byte_buffer));
    return buffer;
}
uint64_t reserve_checksum(byte_buffer * b){
    uint64_t ret = b->curr_bytes;
    padd_buffer(b, sizeof(uint32_t));
    return ret;
}

static inline void writeback_checksum(byte_buffer * b, uint32_t checksum, uint64_t checksum_loc){
    memcpy(&b->buffy[checksum_loc], &checksum, sizeof(checksum));
}
void do_checksum(byte_buffer * b, uint64_t checksum_spot) {
    uint64_t payload_start_offset = checksum_spot + sizeof(uint32_t);
    uint64_t payload_len = b->curr_bytes - payload_start_offset;
    if (payload_len == 0) {
        writeback_checksum(b, 0, checksum_spot);
        return;
    }
    const char* payload_ptr = buf_ind(b, payload_start_offset);
    uint32_t checksum = crc32(payload_ptr, payload_len);
    writeback_checksum(b, checksum, checksum_spot);
}

static inline bool is_pow2(size_t x) { return x && ((x & (x - 1)) == 0); }

static inline size_t align_up(size_t v, size_t a) {
    size_t r = v % a;
    return r ? (v + (a - r)) : v;            
}

static inline size_t align_up_pow2(size_t v, size_t a) {
    return (v + (a - 1)) & ~(a - 1);
}
void pad_nearest_x(struct byte_buffer *b, uint32_t x) {
    size_t a = x;
    size_t next = is_pow2(a) ? align_up_pow2(b->curr_bytes, a)
                              : align_up(b->curr_bytes, a);
    padd_buffer(b, next - b->curr_bytes);
}

void b_seek_next_align(struct byte_buffer *b, uint32_t align) {
    size_t a = align;
    size_t next = is_pow2(a) ? align_up_pow2(b->read_pointer, a)
                              : align_up(b->read_pointer, a);
    b->read_pointer = next;
}


/*resizes buffer to a size of atleast min-size bytes*/
int buffer_resize(byte_buffer * b, size_t min_size){
    fprintf(stdout, "Buffer resize from %zu to %zu bytes. This may be a memory leak.\n"
    ,b->curr_bytes, min_size);
    int new_size = max(min_size, 2 * b->max_bytes);
    void * temp = realloc(b->buffy,new_size);
    if (!temp) return -1;
    b->buffy = temp;
    b->max_bytes = new_size;
    return b->max_bytes;
}
int write_buffer(byte_buffer * buffer, void* data, size_t size){
    memcpy(&buffer->buffy[buffer->curr_bytes], data, size);
    buffer->curr_bytes += size;
    return 0;
}

int read_buffer(byte_buffer * buffer, void* dest, size_t len){
    memcpy(dest, &buffer->buffy[buffer->read_pointer], len);
    buffer->read_pointer += len;
    return len;
}
int dump_buffy(byte_buffer * buffer, FILE * file){
    int size = fwrite(buffer->buffy, 1, buffer->curr_bytes, file);
    fflush(file);
    if (size == buffer->curr_bytes) return 0;
    
    return -1;
}
char * buf_ind(byte_buffer * b, int size){
    if (size  >= b->max_bytes){
        return NULL;
    }
    return &b->buffy[size];
}
char * go_nearest_v(byte_buffer * buffer, char c) {
    char *result = memchr(&buffer->buffy[buffer->read_pointer], c, buffer->curr_bytes - buffer->read_pointer);
    buffer->read_pointer = (result!= NULL )?  result - buffer->buffy:  buffer->curr_bytes;
    return result;
}

char * go_nearest_backwards(byte_buffer * buffer, char c){
    for (size_t i = buffer->read_pointer; i > 0; i--) {
        if (buffer->buffy[i - 1] == c) {
            buffer->read_pointer = i - 1;
            return &buffer->buffy[i - 1];
        }
    }
    buffer->read_pointer = 0;
    return NULL;
}
char * get_curr(byte_buffer * buffer){
    if (buffer->read_pointer >= buffer->curr_bytes) return NULL;
    char * temp = &buffer->buffy[buffer->read_pointer];
    return temp;
}
char * get_next(byte_buffer * buffer){
    buffer->read_pointer++;
    if (buffer->read_pointer >= buffer->curr_bytes) return NULL;
    char * temp = &buffer->buffy[buffer->read_pointer];
    return temp;
}
int byte_buffer_transfer(byte_buffer * read_me, byte_buffer* write_me, size_t num){
    int l=read_buffer(read_me, &write_me->buffy[write_me->curr_bytes],num);
    write_me->curr_bytes += num;
    return l;
}
void reset_buffer(void * v_buffer){
    byte_buffer * buffer = (byte_buffer*)v_buffer;
    buffer->curr_bytes = 0;
    buffer->read_pointer = 0;
}
void free_buffer(void* v_buffer){
    byte_buffer * buffer = (byte_buffer*)v_buffer;
    if (v_buffer == NULL) return;
    free(buffer->buffy);
    free(buffer);
    buffer=  NULL;
}