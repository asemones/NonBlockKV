#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdint.h>
#include "byte_buffer.h"
#include "../util/error.h"
#include "slab.h"
#include "../db/backend/indexer.h"
#include "../db/backend/key-value.h"
#pragma once

#define MAX_LEVEL 16
/**
 * @brief Node in a skip list data structure
 * @struct Node
 * @param key The key stored in this node
 * @param value The value associated with the key
 * @param forward Array of pointers to next nodes at different levels
 */
typedef struct sst_node {
    f_str min_key;
    sst_f_inf * inf;
    struct  sst_node* forward[];
} sst_node;
/**
 * @brief Skip list data structure for efficient key-value storage and retrieval
 * @struct sst_sl
 * @param level Current maximum level in the skip list
 * @param header Pointer to the header node
 * @param compare Function pointer to the comparison function for keys
 */
typedef struct sst_sl {
    int level;
    int use_default;
    sst_node * header;
    slab_allocator allocator;
} sst_sl;

/**
 * @brief Inserts a key-value pair into the skip list
 * @param list Pointer to the skip list
 * @param key The key to insert
 * @param value The value associated with the key
 * @return 0 on success, error code on failure
 */
int sst_insert_list(sst_sl* list, sst_f_inf* sst);

/**
 * @brief Creates a new skip list
 * @param compare Function pointer to the comparison function for keys
 * @return Pointer to the newly created skip list
 */
sst_sl* create_sst_sl(int max_nodes);


/**
 * @brief Loads a skip list from a byte buffer stream
 * @param s Pointer to the skip list to populate
 * @param stream Pointer to the byte buffer containing the skip list data
 * @return Number of entries loaded or error code
 */
sst_sl* create_sst_sl(int max_nodes);
sst_node* sst_search_list(sst_sl* list, f_str k);
sst_node* sst_search_list_prefix(sst_sl* list, f_str k);
sst_node * sst_search_between(sst_sl * list, f_str k);

/**
 * @brief Deletes a key-value pair from the skip list
 * @param list Pointer to the skip list
 * @param key The key to delete
 */

void sst_delete_element(sst_sl* list, f_str k);

/**
 * @brief Frees all memory allocated for the skip list
 * @param list Pointer to the skip list to free
 */
void freesst_sl(sst_sl* list);

