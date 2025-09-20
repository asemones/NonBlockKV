#include "mono_counter.h"

static uint64_t fast_path(counter_t *ctr){
    ctr->bits.count ++;
    return ctr->raw;
}
uint64_t new_value(counter_t *ctr) {
    uint16_t count = ctr->bits.count;
    if (count < UINT16_MAX){
        return fast_path(ctr);
    }
    uint64_t now = now_sec() & TS_MASK;
    uint64_t sec=  ctr->bits.seconds;
    if (now <= sec) now = (now + 1) & TS_MASK;
    ctr->bits.seconds = now;
    ctr->bits.count = 0;
    return ctr->raw;
}
uint64_t get_mono_ctr_v(const counter_t ctr){
    return ctr.raw;
}