#include "maths.h"
uint64_t tsc_hz = 0;  
void calibrate_tsc(void){
    struct timespec ts1, ts2;
    uint64_t c1, c2;

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts1);
    c1 = getticks();

    struct timespec req = { .tv_sec = 0, .tv_nsec = 50 * 1000 * 1000 };
    clock_nanosleep(CLOCK_MONOTONIC, 0, &req, NULL);

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts2);
    c2 = getticks();

    uint64_t ns   = (ts2.tv_sec - ts1.tv_sec) * 1000000000ULL + (ts2.tv_nsec - ts1.tv_nsec);
    tsc_hz = (c2 - c1) * 1000000000ULL / ns; 
    
}
int has_been_us(uint64_t start_ns, uint64_t delta,  uint64_t * out_curr_time){
    uint64_t curr_ns = get_ns();
    uint64_t delta_ns  = delta * NS_TO_US;
    if ( curr_ns - start_ns > delta_ns){
        *out_curr_time = curr_ns;
        return true;
    }
    return false;
}
int has_been_ms(uint64_t start_ns, uint64_t delta, uint64_t * out_curr_time){
    uint64_t curr_ns = get_ns();
    uint64_t delta_ns  = delta *ONE_MILLION;
    if ( curr_ns - start_ns > delta_ns){
        *out_curr_time = curr_ns;
        return true;
    }
    return false;
}
int has_been_s(uint64_t start_ns, uint64_t delta,  uint64_t * out_curr_time){
    uint64_t curr_ns = get_ns();
    uint64_t delta_ns  = delta * ONE_BILLION;
    if ( curr_ns - start_ns > delta_ns){
        *out_curr_time = curr_ns;
        return true;
    }   
    return false;
}