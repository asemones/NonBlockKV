#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdint.h>
#include "byte_buffer.h"
#include "../db/backend/key-value.h"
#include "../util/error.h"
#include "arena.h"

#pragma once

#define MAX_LEVEL 16
/**
 * @brief Node in a skip list data structure
 * @struct Node
 * @param key The key stored in this node
 * @param value The value associated with the key
 * @param forward Array of pointers to next nodes at different levels
 */
typedef struct Node {
    f_str key;
    f_str value;
    struct Node* forward[];
} Node;
/**
 * @brief Skip list data structure for efficient key-value storage and retrieval
 * @struct SkipList
 * @param level Current maximum level in the skip list
 * @param header Pointer to the header node
 * @param compare Function pointer to the comparison function for keys
 */
typedef struct SkipList {
    int level;
    int use_default;
    Node* header;
    arena * arena;
} SkipList;

/**
 * @brief Inserts a key-value pair into the skip list
 * @param list Pointer to the skip list
 * @param key The key to insert
 * @param value The value associated with the key
 * @return 0 on success, error code on failure
 */
int insert_list(SkipList* list, f_str key, f_str value);

/**
 * @brief Creates a new node for the skip list
 * @param level The level of the node
 * @param key The key to store in the node
 * @param value The value associated with the key
 * @return Pointer to the newly created node
 */\
Node* create_skiplist_node(int level, f_str key, f_str value, arena * allocator);

/**
 * @brief Creates a new skip list
 * @param compare Function pointer to the comparison function for keys
 * @return Pointer to the newly created skip list
 */
SkipList* create_skiplist(int max_nodes,int (*compare)(const void*, const void*));

/**
 * @brief Generates a random level for a new node
 * @return Random level between 1 and MAX_LEVEL
 */
int random_level(void);

/**
 * @brief Loads a skip list from a byte buffer stream
 * @param s Pointer to the skip list to populate
 * @param stream Pointer to the byte buffer containing the skip list data
 * @return Number of entries loaded or error code
 */
int skip_from_stream(SkipList *s, byte_buffer *stream);
int insert_list(SkipList* list, f_str key, f_str value);

/**
 * @brief Searches for a key in the skip list
 * @param list Pointer to the skip list
 * @param key The key to search for
 * @return Pointer to the node containing the key, or NULL if not found
 */
Node* search_list(SkipList* list, f_str key);

/**
 * @brief Searches for a key prefix in the skip list
 * @param list Pointer to the skip list
 * @param key The key prefix to search for
 * @return Pointer to the first node with the matching prefix, or NULL if not found
 */
Node* search_list_prefix(SkipList* list, f_str key);

/**
 * @brief Deletes a key-value pair from the skip list
 * @param list Pointer to the skip list
 * @param key The key to delete
 */

void delete_element(SkipList* list,  f_str key);

/**
 * @brief Frees all memory allocated for the skip list
 * @param list Pointer to the skip list to free
 */
void freeSkipList(SkipList* list);

/**
 * @brief Comparison function for integer keys
 * @param a Pointer to the first integer
 * @param b Pointer to the second integer
 * @return Negative if a < b, 0 if a == b, positive if a > b
 */
int compareInt(const void* a, const void* b);

/**
 * @brief Comparison function for string keys
 * @param a Pointer to the first string
 * @param b Pointer to the second string
 * @return Negative if a < b, 0 if a == b, positive if a > b
 */
static inline int compareString(const void* a, const void* b) {
    return strcmp((const char*)a, (const char*)b);
}
void reset_skip_list(SkipList* list);