#include <stdlib.h>
#include <stdio.h>
#include<string.h>
#include <stdint.h>
#include "../../ds/byte_buffer.h"
#include "option.h"
#include <stdint.h>

#define INLINE_CAP 8
#define V_LOG_TOMBSTONE UINT16_MAX
#define F_SSO_LEN 12
#define F_PRFX_LEN 4
/**
 * @brief Basic unit for storing database entries
 * @struct db_unit
 * @param len Length of the entry in bytes
 * @param entry Pointer to the actual data
 */

typedef struct f_str{
    
    union 
    {
        struct{
            uint32_t len;
            char prfx [F_PRFX_LEN];
            void * mem;
        };
        struct{
            uint32_t __padding;
            char sso[F_SSO_LEN];
        };
        struct {
            uint64_t _padding;
            void * entry;
        };
    };
  

}f_str;


#define F_SSO_BIT  0x80000000u
#define F_LENMASK  0x7FFFFFFFu
static inline int f_is_sso(const f_str *s) { return (s->len & F_SSO_BIT) != 0; }
static inline uint32_t f_len(const f_str *s){ return s->len & F_LENMASK; }

#pragma once
/* for comparison functions*/
/*NEW STORAGE FORMAT: 
[num entries ... key len A, value len A,....... Key A Value A ]
*/
/**
 * @brief Enumeration of data sources in the LSM tree
 * @enum source
 */
enum source {
    memtable,  /**< Data from memory table */
    lvl_0,     /**< Data from level 0 SST files */
    lvl_1_7    /**< Data from levels 1-7 SST files */
};
/**
 * @brief Structure for merging data from different sources
 * @struct merge_data
 * @param key Pointer to the key db_unit
 * @param value Pointer to the value db_unit
 * @param index Index of the entry
 * @param src Source of the data (memtable, level 0, or levels 1-7)
 */
typedef struct merge_data {
    f_str key;
    f_str value;
    uint16_t index;
    enum source src;
}merge_data;
/**
 * @brief Writes a db_unit to a byte buffer
 * @param b Pointer to the byte buffer
 * @param u The db_unit to write
 * @return Number of bytes written or error code
 */
int write_disk_format(byte_buffer * b, f_str u);

/**
 * @brief Reads a db_unit from a byte buffer
 * @param b Pointer to the byte buffer
 * @param u Pointer to the db_unit to populate
 */
void read_disk_format(byte_buffer * b, f_str * u);
void * try_deference_value(f_str value);

static inline int get_kv_overhead_short(){
    uint16_t size_of_len = sizeof(uint16_t) * 2;
    uint16_t size_of_end_ptr =  sizeof(uint16_t);
    return size_of_end_ptr + size_of_len;
}
uint64_t get_mem_tbl_size(f_str key, f_str value);
f_str api_create(void * data, uint64_t len);
void cleanse(f_str * external_input);
f_str make_fstr(char * cpy, uint32_t len);
int f_cmp(f_str one, f_str two);
int write_fstr(byte_buffer * b,const f_str one);
int read_fstr(byte_buffer * b, f_str * one);
f_str f_str_alloc(char * later_mem);
void f_cpy(f_str *dest, const f_str *src);
int read_and_allocate_char(const char * str, f_str * one, uint32_t len);
void fwrite_fstr(f_str one, FILE * f);
f_str f_set(const char * str, uint16_t len);
int read_and_allocate(byte_buffer * b, f_str * one);
f_str format_for_in_mem(const f_str targ);
int disk_f_cmp(f_str disk_formatted, f_str one);