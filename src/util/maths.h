
#ifndef MATHS_H
#define MATHS_H
#define _POSIX_C_SOURCE 200809L
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>

#define ONE_MILLION 1000000
#define ONE_BILLION 1000 * ONE_MILLION
#define NS_TO_US 1000;
#define US_TO_MS 1000;
#define NS_TO_S ONE_BILLION
extern uint64_t tsc_hz;     

static inline uint64_t getticks(void){
    uint64_t lo, hi;

    // RDTSC copies contents of 64-bit TSC into EDX:EAX
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (uint64_t)hi << 32 | lo;
}
void calibrate_tsc(void);

static inline int max(int a, int b){return a > b ? a : b;}
static inline int min(int a, int b){return a < b ? a : b;}
static inline int ceil_int_div(int dividend, int divsor){
    int floor = dividend/divsor;
    if (dividend % divsor) floor ++;
    return floor;
}
static inline bool is_pwr_2(uint64_t n){ return n > 0 && (n & (n - 1)) == 0;}
static inline uint64_t round_up_pow2(uint64_t x) {
    if (x == 0) return 0;
    x--;                            
    x |= x >> 1;  x |= x >> 2;       
    x |= x >> 4;  x |= x >> 8;
    x |= x >> 16; x |= x >> 32;
    return x + 1;                   
}
static inline uint64_t tsc_to_ns(uint64_t tsc){
     return (uint64_t) (((__uint128_t)tsc* tsc_hz) >> 32);
}
static inline uint64_t xorshift32(void) {
    static uint32_t state = 2463534242U;  
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
}
static inline uint64_t  fast_coin(void) {
    static uint64_t bits = 0;
    static uint8_t bits_left = 0;

    if (bits_left == 0) {
        bits = xorshift32();
        bits_left = 64;
    }

    int bit = bits & 1;
    bits >>= 1;           
    bits_left--;
    return bit;
}

static inline uint64_t get_ns(void){
    #if defined(__x86_64__)
    uint64_t cycles = getticks();
    return tsc_to_ns(cycles);
    #endif

    #ifndef __x86_64__
    struct timespec ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    return ts1.tv_nsec + ts1.tv_sec * NS_TO_S
    #endif
}
int has_been_us(uint64_t start_ns, uint64_t delta, uint64_t * out_curr_time);
int has_been_ms(uint64_t start_ns, uint64_t delta, uint64_t * out_curr_time);
int has_been_s(uint64_t start_ns, uint64_t delta, uint64_t * out_curr_time);

#endif