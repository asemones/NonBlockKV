#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include "../util/alloc_util.h"
#ifndef HASHTBLE_H
#define HASHTBLE_H
typedef struct pair{
    void * key;
    void * value;
}kv;

// HASH TABLE 
// HASH FUNC: fn1va

#define KEY_AMOUNT 10607
#define FNV_PRIME_64 1099511628211ULL
#define FNV_OFFSET_64 14695981039346656037ULL
uint64_t fnv1a_64(const void *data, size_t len);

static inline uint64_t lph_splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static inline size_t lph_next_pow2(size_t x) {
    if (x < 8) return 8;
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return x + 1;
}
#define LPH_DECLARE(NAME, VAL_T)                                          \
typedef struct NAME {                                                     \
    size_t cap;                                                           \
    size_t size;                                                          \
    size_t tombs;                                                         \
    uint64_t *keys;                                                       \
    VAL_T *vals;                                                          \
    uint8_t *state;                                                       \
} NAME;                                                                   \
                                                                          \
int     NAME##_init(NAME *m, size_t initial_capacity);                    \
void    NAME##_destroy(NAME *m);                                          \
void    NAME##_clear(NAME *m);                                            \
int     NAME##_reserve(NAME *m, size_t want);                             \
int     NAME##_put(NAME *m, uint64_t key, VAL_T value, VAL_T *old_val);   \
int     NAME##_get(const NAME *m, uint64_t key, VAL_T *out);              \
int     NAME##_erase(NAME *m, uint64_t key, VAL_T *old_val);              \
size_t  NAME##_size(const NAME *m);                                       \
size_t  NAME##_capacity(const NAME *m);

#define LPH_DEFINE(NAME, VAL_T)                                           \
static inline int NAME##_need_grow(const NAME *m) {                        \
    return ((m->size + m->tombs) * 10) >= (m->cap * 7);                   \
}                                                                         \
                                                                          \
static int NAME##_rebuild(NAME *m, size_t new_cap) {                      \
    new_cap = lph_next_pow2(new_cap);                                     \
    NAME tmp = {0};                                                       \
    tmp.cap   = new_cap;                                                  \
    tmp.keys  = (uint64_t*)calloc(new_cap, sizeof(uint64_t));             \
    tmp.vals  = (VAL_T*)malloc(new_cap * sizeof(VAL_T));                  \
    tmp.state = (uint8_t*)calloc(new_cap, sizeof(uint8_t));               \
    if (!tmp.keys || !tmp.vals || !tmp.state) {                           \
        free(tmp.keys);                                                   \
        free(tmp.vals);                                                   \
        free(tmp.state);                                                  \
        return 0;                                                         \
    }                                                                     \
    size_t mask = new_cap - 1;                                            \
    for (size_t i = 0; i < m->cap; ++i) {                                 \
        if (m->state[i] == 1) {                                           \
            uint64_t k = m->keys[i];                                      \
            VAL_T v = m->vals[i];                                         \
            size_t h = (size_t)lph_splitmix64(k) & mask;                  \
            while (tmp.state[h] == 1) {                                   \
                h = (h + 1) & mask;                                       \
            }                                                             \
            tmp.state[h] = 1;                                             \
            tmp.keys[h]  = k;                                             \
            tmp.vals[h]  = v;                                             \
            tmp.size++;                                                   \
        }                                                                 \
    }                                                                     \
    free(m->keys);                                                        \
    free(m->vals);                                                        \
    free(m->state);                                                       \
    *m = tmp;                                                             \
    m->tombs = 0;                                                         \
    return 1;                                                             \
}                                                                         \
                                                                          \
int NAME##_init(NAME *m, size_t initial_capacity) {                       \
    if (!m) return 0;                                                     \
    memset(m, 0, sizeof(*m));                                             \
    size_t cap = lph_next_pow2(initial_capacity ? initial_capacity : 64); \
    m->cap   = cap;                                                       \
    m->keys  = (uint64_t*)calloc(cap, sizeof(uint64_t));                  \
    m->vals  = (VAL_T*)malloc(cap * sizeof(VAL_T));                       \
    m->state = (uint8_t*)calloc(cap, sizeof(uint8_t));                    \
    if (!m->keys || !m->vals || !m->state) {                              \
        free(m->keys);                                                    \
        free(m->vals);                                                    \
        free(m->state);                                                   \
        memset(m, 0, sizeof(*m));                                         \
        return 0;                                                         \
    }                                                                     \
    return 1;                                                             \
}                                                                         \
                                                                          \
void NAME##_destroy(NAME *m) {                                            \
    if (!m) return;                                                       \
    free(m->keys);                                                        \
    free(m->vals);                                                        \
    free(m->state);                                                       \
    memset(m, 0, sizeof(*m));                                             \
}                                                                         \
                                                                          \
void NAME##_clear(NAME *m) {                                              \
    if (!m || !m->state) return;                                          \
    memset(m->state, 0, m->cap * sizeof(uint8_t));                        \
    m->size  = 0;                                                         \
    m->tombs = 0;                                                         \
}                                                                         \
                                                                          \
int NAME##_reserve(NAME *m, size_t want) {                                \
    if (!m) return 0;                                                     \
    if (want <= m->cap) return 1;                                         \
    return NAME##_rebuild(m, want);                                       \
}                                                                         \
                                                                          \
int NAME##_put(NAME *m, uint64_t key, VAL_T value, VAL_T *old_val) {      \
    if (!m) return 0;                                                     \
    if (NAME##_need_grow(m)) {                                            \
        if (!NAME##_rebuild(m, m->cap * 2)) return 0;                     \
    }                                                                     \
    size_t mask = m->cap - 1;                                             \
    size_t h = (size_t)lph_splitmix64(key) & mask;                        \
    size_t first_tomb = (size_t)-1;                                       \
    for (;;) {                                                            \
        uint8_t st = m->state[h];                                         \
        if (st == 0) {                                                    \
            size_t idx = (first_tomb != (size_t)-1) ? first_tomb : h;     \
            if (m->state[idx] == 2) m->tombs--;                           \
            m->state[idx] = 1;                                            \
            m->keys[idx]  = key;                                          \
            m->vals[idx]  = value;                                        \
            m->size++;                                                    \
            return 1;                                                     \
        } else if (st == 2) {                                             \
            if (first_tomb == (size_t)-1) first_tomb = h;                 \
        } else {                                                          \
            if (m->keys[h] == key) {                                      \
                if (old_val) *old_val = m->vals[h];                       \
                m->vals[h] = value;                                       \
                return 2;                                                 \
            }                                                             \
        }                                                                 \
        h = (h + 1) & mask;                                               \
    }                                                                     \
}                                                                         \
                                                                          \
int NAME##_get(const NAME *m, uint64_t key, VAL_T *out) {                 \
    if (!m || m->cap == 0) return 0;                                      \
    size_t mask = m->cap - 1;                                             \
    size_t h = (size_t)lph_splitmix64(key) & mask;                        \
    for (;;) {                                                            \
        uint8_t st = m->state[h];                                         \
        if (st == 0) return 0;                                            \
        if (st == 1 && m->keys[h] == key) {                               \
            if (out) *out = m->vals[h];                                   \
            return 1;                                                     \
        }                                                                 \
        h = (h + 1) & mask;                                               \
    }                                                                     \
}                                                                         \
                                                                          \
int NAME##_erase(NAME *m, uint64_t key, VAL_T *old_val) {                 \
    if (!m) return 0;                                                     \
    size_t mask = m->cap - 1;                                             \
    size_t h = (size_t)lph_splitmix64(key) & mask;                        \
    for (;;) {                                                            \
        uint8_t st = m->state[h];                                         \
        if (st == 0) return 0;                                            \
        if (st == 1 && m->keys[h] == key) {                               \
            if (old_val) *old_val = m->vals[h];                           \
            m->state[h] = 2;                                              \
            m->size--;                                                    \
            m->tombs++;                                                   \
            if (m->tombs > (m->cap >> 2)) {                               \
                (void)NAME##_rebuild(m, m->cap);                          \
            }                                                             \
            return 1;                                                     \
        }                                                                 \
        h = (h + 1) & mask;                                               \
    }                                                                     \
}                                                                         \
                                                                          \
size_t NAME##_size(const NAME *m) {                                       \
    return m ? m->size : 0;                                               \
}                                                                         \
                                                                          \
size_t NAME##_capacity(const NAME *m) {                                   \
    return m ? m->cap : 0;                                                \
}
int compare_kv(kv *k1, kv *k2);
int compare_kv_v(const void * kv1, const void * kv2);
kv KV(void* k, void* v);
kv* dynamic_kv(void* k, void* v);
#endif
