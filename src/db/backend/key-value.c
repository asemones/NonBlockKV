#include "key-value.h"
#include <sys/types.h> // Include for u_int16_t
#define ERR -1000
#define INT 0
#define FLOAT 1 
#define STR 2
#define LARGE_VAL_MASK 32768 
#define F_SSO_LEN 12
#define F_PRFX_LEN 4
int write_disk_format(byte_buffer * b, f_str u){
    write_int16(b, (uint16_t)u.len);
    if (u.len <= F_SSO_LEN) {
        write_buffer(b, u.sso, u.len);
        return 0;
    }
    write_buffer(b, u.prfx, F_PRFX_LEN);
    write_buffer(b, u.mem, u.len - F_PRFX_LEN);
    return sizeof(uint16_t) + u.len;
}
void read_disk_format(byte_buffer * b, f_str  *u){
    uint16_t fixed_len = read_int16(b);
    u->len = fixed_len;
    read_buffer(b,(char*)u->entry, u->len);
}
f_str api_create(void * data, uint64_t len){
    f_str u;
    u.len = len;
    u.entry = data;
    return u;
}
void cleanse(f_str * external_input){
  return;
}
void * try_deference_value(f_str value){
    if (value.len >= LARGE_VAL_MASK){
        return NULL;
    }
    return value.entry;
}
f_str make_fstr(char * cpy, uint32_t len){
    f_str str;
    str.len  = len;
    memcpy(str.prfx, cpy, F_PRFX_LEN);
    str.mem = NULL;
    if (len > F_SSO_LEN){
        str.mem = cpy + F_PRFX_LEN;
    } else {
        if (len > F_PRFX_LEN){
            memcpy(str.sso + F_PRFX_LEN, cpy + F_PRFX_LEN, len - F_PRFX_LEN);
        }
    }
    return str;
}
f_str format_for_in_mem(const f_str targ){
    f_str next;
    if (targ.len <= F_SSO_LEN){
        memcpy(next.sso, targ.entry, targ.len);
    }
    else{
        memcpy(next.prfx, targ.entry, F_PRFX_LEN);
        next.mem = targ.entry;
    }
    next.len = targ.len;
    return next;
}   
int f_cmp(f_str one, f_str two) {
    int r = memcmp(one.prfx, two.prfx, F_PRFX_LEN);
    if (r){
        return r;
    }

    const char* p1 = (one.len <= F_SSO_LEN) ? one.sso + F_PRFX_LEN : one.mem;
    const char* p2 = (two.len <= F_SSO_LEN) ? two.sso + F_PRFX_LEN : two.mem;

    size_t b1 = (one.len > F_PRFX_LEN) ? (one.len - F_PRFX_LEN) : 0;
    size_t b2 = (two.len > F_PRFX_LEN) ? (two.len - F_PRFX_LEN) : 0;
    size_t min_len = min(b1, b2);

    int comp = (min_len > 0) ? memcmp(p1, p2, min_len) : 0;
    if (comp){
        return comp;
    }

    if (one.len != two.len){
        if (one.len > two.len){
            return 1;
        } else {
            return -1;
        }
    }
    return 0;
}
int disk_f_cmp(f_str disk_formatted, f_str one){
    size_t cmp_s = min(one.len, disk_formatted.len);
    if (one.len <= F_SSO_LEN){
        return memcmp(one.sso, disk_formatted.entry, cmp_s);
    }
    int r = memcmp(one.prfx, disk_formatted.entry, min(cmp_s, F_PRFX_LEN));
    if (r){
        return r;
    }
    r = memcmp(one.entry + F_PRFX_LEN, disk_formatted.entry, cmp_s -F_PRFX_LEN);
    if (one.len != disk_formatted.len){
        if (one.len > disk_formatted.len){
            return 1;
        } else {
            return -1;
        }
    }
    return 0;
}
int write_fstr(byte_buffer *b, const f_str one) {
    uint32_t len = one.len;
    write_int32(b, len);

    if (len == 0) return 0;

    if (len <= F_SSO_LEN) {
        write_buffer(b, (void*)one.sso, len);
        return 0;
    }
    write_buffer(b, (void*)one.prfx, F_PRFX_LEN);
    write_buffer(b, one.mem, len - F_PRFX_LEN);
    return 0;
}


int read_fstr(byte_buffer *b, f_str *one){
    uint32_t len = read_int32(b);
    one->len = len;

    if (len <= F_SSO_LEN) {
        read_buffer(b, one->sso, len);
        return 0;
    }
    read_buffer(b, one->prfx, F_PRFX_LEN);
    read_buffer(b, one->mem, len - F_PRFX_LEN);
    return 0;
}

int read_and_allocate(byte_buffer * b, f_str * one){
    uint32_t len = read_int32(b);
    one->len = len;

    if (len <= F_SSO_LEN) {
        read_buffer(b, one->sso, len);
        return 0;
    }
    one->mem = malloc(len - F_PRFX_LEN);
    read_buffer(b, one->prfx, F_PRFX_LEN);
    read_buffer(b, one->mem, len - F_PRFX_LEN);
    return 0;
}
int read_and_allocate_char(const char * str, f_str * one, uint32_t len){
    one->len = len;

    if (len <= F_SSO_LEN) {
        memcpy(one->sso, str, len);
        return 0;
    }
    one->mem = malloc(len - F_PRFX_LEN);
    memcpy(one->prfx, str, F_PRFX_LEN);
    memcpy(one->mem, str,len - F_PRFX_LEN);
    return 0;
}

void fwrite_fstr(f_str one, FILE * f){
    fwrite(&one.len, sizeof(one.len), 1, f);
    fwrite(one.prfx, F_PRFX_LEN, 1, f);
    if (one.len <= F_SSO_LEN){
        if (one.len > F_PRFX_LEN){
            fwrite(one.sso + F_PRFX_LEN, one.len - F_PRFX_LEN, 1, f);
        }
    } 
    else {
        fwrite(one.mem, one.len - F_PRFX_LEN, 1, f);
    }
}

f_str f_str_alloc(char * later_mem){
    f_str str;
    str.mem =  later_mem;
    str.len  = 0;
    return str;
}
uint64_t get_mem_tbl_size(const f_str key, const f_str value){
    return key.len + value.len + get_kv_overhead_short();
}
f_str f_set(const char * str, uint16_t len){
    f_str one;
    one.len = len;
    if (len <= F_SSO_LEN){
        memcpy(one.sso,str, len );
        return one;
    }
    memcpy(one.prfx, str, F_PRFX_LEN);
    memcpy(one.mem, str, len - F_PRFX_LEN);
    return one;
}
f_str f_str_empty(){
    f_str str;
    str.len =  0;
    str.mem =  NULL;
    return str;
}
void f_cpy(f_str *dest, const f_str *src){
    if (src->len <= F_SSO_LEN){
        memcpy(dest->sso, src->sso, src->len);
    } 
    else {
        memcpy(dest->prfx, src->prfx, F_PRFX_LEN);
        memcpy(dest->mem, src->mem, src->len - F_PRFX_LEN);
    }
    dest->len = src->len;
}
uint64_t sst_md_serialized_len(const sst_f_inf *s) {
    uint64_t n = 0;

    // 1) file_name, NUL-terminated
    n += (uint64_t)strlen(s->file_name) + 1;

    // 2-3) sizes
    n += sizeof(size_t);      // length
    n += sizeof(size_t);      // compressed_len

    // 4) bool (or 1 byte if you change the on-disk type)
    n += sizeof(bool);

    // 5-6) dict info (use the actual field types)
    n += sizeof(s->compr_info.dict_offset);
    n += sizeof(s->compr_info.dict_len);

    // 7-8) max/min f_str (must match what read_fstr() consumes)
    n += f_str_len_mem_disk(s->max);
    n += f_str_len_mem_disk(s->min);

    // 9) timeval (or two int64_t if you change the on-disk format)
    n += sizeof(struct timeval);

    // 10-11) block_start and block_ind_len
    n += sizeof(size_t);      // block_start
    n += sizeof(size_t);      // block_ind_len

    return n;
}

