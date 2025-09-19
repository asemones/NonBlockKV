#include "mono_counter.h"

uint64_t new_value(counter_t *ctr) {
     uint64_t now = now_sec() & TS_MASK;
    if (__builtin_expect(now > ctr->bits.seconds, 0)) {
        ctr->bits.seconds = now;
        ctr->bits.count = 0;
    } 
    else if (__builtin_expect(ctr->bits.count == CNT_MAX, 0)) {
        uint64_t next = (ctr->bits.seconds + 1) & TS_MASK;
        ctr->bits.seconds = (now > next) ? now : next;
        ctr->bits.count = 0;
    } 
    else {
        ctr->bits.count++;
    }
    return ctr->raw;
}
uint64_t get_mono_ctr_v(const counter_t ctr){
    return ctr.raw;
}