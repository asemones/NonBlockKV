#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#define CACHE_LINE_SIZE 64 

// Padded atomic bitmap word to prevent false sharing
typedef struct {
    atomic_int_fast64_t value;
    uint8_t padding[CACHE_LINE_SIZE - sizeof(atomic_int_fast64_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) padded_atomic_word_t;

typedef struct bitmap {
    padded_atomic_word_t* map;  
    uint64_t size_bits;
} bitmap;

static inline bitmap bitmap_create(size_t num_bits) {
    size_t num_words = (num_bits + 63) / 64;
    size_t bytes_needed = num_words * sizeof(padded_atomic_word_t);
    

    padded_atomic_word_t* b = aligned_alloc(CACHE_LINE_SIZE, bytes_needed);
    bitmap map = {0};
    

    for (size_t i = 0; i < num_words; i++) {
        atomic_init(&b[i].value, 0);
    }
    
    map.map = b;
    map.size_bits = num_bits;
    return map;
}

static inline void bitmap_set(bitmap* bm, size_t bit_index) {
    if (!bm->map) return;
    size_t word_index = bit_index / 64;
    size_t bit_offset = bit_index % 64;
    
    // Atomically set bit
    uint64_t mask = 1ULL << bit_offset;
    atomic_fetch_or(&bm->map[word_index].value, mask);
}

static inline void bitmap_clear(bitmap* bm, size_t bit_index) {
    if (!bm->map) return;
    size_t word_index = bit_index / 64;
    size_t bit_offset = bit_index % 64;
    
    uint64_t mask = ~(1ULL << bit_offset);
    atomic_fetch_and(&bm->map[word_index].value, mask);
}

static inline bool bitmap_test(bitmap* bm, size_t bit_index) {
    if (!bm->map) return false;
    size_t word_index = bit_index / 64;
    size_t bit_offset = bit_index % 64;
    
    uint64_t word = atomic_load(&bm->map[word_index].value);
    return (word & (1ULL << bit_offset)) != 0;
}
static inline size_t bitmap_get_set_bits_512(const bitmap* bm, size_t* indices, size_t max_indices) {
    if (!bm->map || !indices) return 0;
    
    size_t count = 0;

    uint64_t word_snapshots[8];
    for (int word_idx = 0; word_idx < 8; word_idx++) {
        word_snapshots[word_idx] = atomic_load(&bm->map[word_idx].value);
    }
    for (int word_idx = 0; word_idx < 8 && count < max_indices; word_idx++) {
        uint64_t word = word_snapshots[word_idx];
        while (word && count < max_indices) {
            int bit_pos = __builtin_ctzl(word);
            indices[count++] = word_idx * 64 + bit_pos;
            word &= word - 1;
        }
    }
    
    return count;
}
static inline size_t bitmap_get_set_bits(const bitmap* bm, size_t* indices, size_t max_indices) {
    if (!bm->map || !indices) return 0;
    
    if (bm->size_bits == 512) {
        return bitmap_get_set_bits_512(bm, indices, max_indices);
    }
    
    size_t count = 0;
    const size_t num_words = (bm->size_bits + 63) / 64;
    
    for (size_t word_idx = 0; word_idx < num_words && count < max_indices; word_idx++) {
        uint64_t word = atomic_load(&bm->map[word_idx].value);
        
        if (word == 0) continue;
        
        while (word != 0 && count < max_indices) {
            uint64_t t = word & -word;
            int bit_pos =  __builtin_ctzl(word);
            indices[count++] = word_idx * 64 + bit_pos;
            word ^= t;
        }
    }
    
    return count;
}