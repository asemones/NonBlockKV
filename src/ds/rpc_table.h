#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>

// #define RPC_TABLE_SIZE 4096 // Replaced by dynamic size
// #define RPC_HASH_MASK (RPC_TABLE_SIZE - 1) // Replaced by dynamic mask (table->size - 1)

#define SHARD_ID_BITS 16
#define REAL_ID_BITS 48

#define SHARD_ID_MASK (((uint64_t)1 << SHARD_ID_BITS) - 1)
#define REAL_ID_MASK (((uint64_t)1 << REAL_ID_BITS) - 1)
#define SHARD_ID_SHIFT REAL_ID_BITS

#define MAX_ID_COUNTER REAL_ID_MASK

typedef enum {
    ENTRY_EMPTY,
    ENTRY_PENDING,
    ENTRY_COMPLETED,
    ENTRY_FAILED,
    ENTRY_TOMBSTONE,
    ENTRY_READ
} entry_state_t;

typedef union future_t {
    int return_code;
    void* value;
} future_t;

typedef struct rpc_table {
    uint64_t shard_id;
    uint64_t id_counter;
    size_t size; // Capacity of the table (must be power of 2)
    size_t mask; // size - 1
    uint64_t* keys;
    future_t* results;
    uint8_t* states;
} rpc_table_t;

static inline uint64_t rpc_encode_id(uint64_t shard_id, uint64_t real_id) {
    return ((shard_id & SHARD_ID_MASK) << SHARD_ID_SHIFT) | (real_id & REAL_ID_MASK);
}

static inline uint64_t rpc_get_shard_id(uint64_t encoded_id) {
    return (encoded_id >> SHARD_ID_SHIFT) & SHARD_ID_MASK;
}

static inline uint64_t rpc_get_real_id(uint64_t encoded_id) {
    return encoded_id & REAL_ID_MASK;
}

// Initializes the RPC table with a given size (must be a power of 2).
// Returns true on success, false on failure (e.g., invalid size, memory allocation failed).
static inline bool rpc_table_init(rpc_table_t* table, uint64_t shard_id, size_t initial_size) {
    if (shard_id > SHARD_ID_MASK) {
        // Consider logging an error or returning false if shard_id is invalid
    }
    // Ensure size is a power of 2
    if (initial_size == 0) {
        return false; // Invalid size
    }
    if ((initial_size & (initial_size - 1)) != 0){
        initial_size  = round_up_pow2(initial_size);
    }
    table->shard_id = shard_id & SHARD_ID_MASK;
    table->id_counter = 1;
    table->size = initial_size;
    table->mask = initial_size - 1;

    table->keys = (uint64_t*)malloc(sizeof(uint64_t) * table->size);
    table->results = (future_t*)malloc(sizeof(future_t) * table->size);
    table->states = (uint8_t*)malloc(sizeof(uint8_t) * table->size);

    if (!table->keys || !table->results || !table->states) {
        free(table->keys);
        free(table->results);
        free(table->states);
        return false; 
    }

    memset(table->keys, 0, sizeof(uint64_t) * table->size);
    memset(table->results, 0, sizeof(future_t) * table->size);
    memset(table->states, ENTRY_EMPTY, sizeof(uint8_t) * table->size);

    return true;
}

// Frees the memory allocated for the RPC table arrays.
static inline void rpc_table_destroy(rpc_table_t* table) {
    if (table) {
        free(table->keys);
        free(table->results);
        free(table->states);
        table->keys = NULL;
        table->results = NULL;
        table->states = NULL;
        table->size = 0;
        table->mask = 0;
    }
}

static inline uint32_t rpc_hash(uint64_t id, size_t mask) {
    return id % mask;
}


static inline uint64_t rpc_start(rpc_table_t* table) {
    
    if (table->id_counter >= MAX_ID_COUNTER) {
        table->id_counter = 1;
    }
    
    uint64_t real_id = table->id_counter++;

    uint64_t encoded_id = rpc_encode_id(table->shard_id, real_id);
    
    uint64_t slot = rpc_hash(encoded_id, table->mask);
  

    while (table->states[slot] != ENTRY_EMPTY &&
            table->states[slot] != ENTRY_TOMBSTONE) {
        slot = (slot + 1) & table->mask;
    }

    table->keys[slot] = encoded_id;
    table->states[slot] = ENTRY_PENDING;
    return encoded_id;

}

static inline bool rpc_complete(rpc_table_t* table, uint64_t encoded_id, future_t result) {
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return false;

        if (table->keys[slot] == encoded_id &&
            table->states[slot] == ENTRY_PENDING) {
            table->results[slot] = result;
            table->states[slot] = ENTRY_COMPLETED;
            return true;
        }

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) break;
    }

    return false;
}
static inline bool rpc_get_result(rpc_table_t* table, uint64_t encoded_id, future_t* out_result) {
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return false;

        if (table->keys[slot] == encoded_id) {
            if (table->states[slot] == ENTRY_COMPLETED || table->states[slot] == ENTRY_FAILED) {
                if (out_result) *out_result = table->results[slot];
                table->states[slot] = ENTRY_TOMBSTONE;
                return true;
            } else {
                return false;
            }
        }

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) break;
    }

    return false;
}

static inline bool rpc_no_return(rpc_table_t* table, uint64_t encoded_id){
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return false;

        if (table->keys[slot] == encoded_id &&
            table->states[slot] == ENTRY_PENDING) {
            table->states[slot] = ENTRY_TOMBSTONE;
            return true;
        }

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) break;
    }

    return false;
}
static inline void rpc_free(rpc_table_t* table, uint64_t encoded_id) {
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return;

        if (table->keys[slot] == encoded_id &&
            table->states[slot] != ENTRY_TOMBSTONE) {
            table->states[slot] = ENTRY_TOMBSTONE;
            return;
        }

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) return;
    }
}

static inline bool rpc_is_pending(rpc_table_t* table, uint64_t encoded_id) {
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return false;

        if (table->keys[slot] == encoded_id)
            return table->states[slot] == ENTRY_PENDING;

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) break;
    }

    return false;
}

static inline bool rpc_is_complete(rpc_table_t* table, uint64_t encoded_id) {
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return false;

        if (table->keys[slot] == encoded_id) {
            return table->states[slot] == ENTRY_COMPLETED || table->states[slot] == ENTRY_FAILED;
        }

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) break;
    }

    return false;
}
static inline bool rpc_fail(rpc_table_t* table, uint64_t encoded_id, future_t failure_reason) {
    uint32_t slot = rpc_hash(encoded_id, table->mask);
    uint32_t orig_slot = slot;

    while (true) {
        if (table->states[slot] == ENTRY_EMPTY) return false;

        if (table->keys[slot] == encoded_id) {
            if (table->states[slot] == ENTRY_PENDING) {
                table->results[slot] = failure_reason;
                table->states[slot] = ENTRY_FAILED;
                return true;
            } else {
                return false;
            }
        }

        slot = (slot + 1) & table->mask;
        if (slot == orig_slot) break;
    }

    return false;
}
static inline bool rpc_id_belongs_to_table(rpc_table_t* table, uint64_t encoded_id) {
    return rpc_get_shard_id(encoded_id) == table->shard_id;
}

static inline uint64_t rpc_get_max_id_counter(void) {
    return MAX_ID_COUNTER;
}

static inline bool rpc_is_valid_real_id(uint64_t real_id) {
    return real_id > 0 && real_id <= MAX_ID_COUNTER;
}