#include <stdint.h>
#include <time.h>
#include "maths.h"
/*a safe unique counter capable of 65k reads per second without needing to persist anything*/
#define TS_BITS     48
#define CNT_BITS    12
#define CNT_MAX     (1U<<CNT_BITS)
#define TS_MASK     ((1ULL<<TS_BITS) - 1)
typedef union {
    struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        uint64_t count   : 16;  
        uint64_t seconds : 48;  
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        uint64_t seconds : 48;
        uint64_t count   : 16;
#endif
    } bits;
    uint64_t raw;              
} counter_t;
uint64_t new_value(counter_t *ctr);
uint64_t get_mono_ctr_v(const counter_t ctr);