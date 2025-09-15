#include "arena.h"
#include "../db/backend/key-value.h"

#pragma once
/**
 * @brief Key-value array structure for storing pairs of keys and values
 * @struct k_v_arr
 * @param keys Array of key db_units
 * @param values Array of value db_units
 * @param cap Maximum capacity of the array
 * @param len Current number of key-value pairs in the array
 */
typedef struct k_v_arr{
    f_str* keys;
    f_str * values;
    size_t cap;
    int len;
}k_v_arr;


/**
 * @brief Creates a new key-value array
 * @param size Initial capacity of the array
 * @param a Arena to allocate memory from
 * @return Pointer to the newly created key-value array
 */
k_v_arr* create_k_v_arr (size_t size);

/**
 * @brief Inserts a key-value pair into the array
 * @param ar Pointer to the key-value array
 * @param key Pointer to the key to insert
 * @param value Pointer to the value to insert
 */
void into_array(void * ar, void * key, void * value);

