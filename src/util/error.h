#include <stdio.h>
#include <stdlib.h>
#define MAX_ERROR_MESSAGE_LENGTH 4096


#pragma once


/**
 * @brief Enumeration of error codes used throughout the database system
 * @enum error_codes
 */
typedef enum error_codes{
    OK,                           /**< Operation completed successfully */
    INVALID_DATA,                 /**< Data is invalid or corrupted */
    INVALID_META_DATA,            /**< Metadata is invalid or corrupted */
    INVALID_BLOCK,                /**< Block structure is invalid */
    INVALID_WAL_FILE,             /**< Write-ahead log file is invalid */
    CRITICAL_MALLOC_FAILURE,      /**< Memory allocation failure requiring immediate shutdown */
    NON_CRITICAL_MALLOC_FAILURE,  /**< Memory allocation failure that can be handled */
    FAILED_BUFFER_WRITE,          /**< Failed to write to a buffer */
    SST_F_INVALID,                /**< SST file is invalid */
    FAILED_OPEN,                  /**< Failed to open a file */
    FAILED_SEEK,                  /**< Failed to seek in a file */
    FAILED_READ,                  /**< Failed to read from a file */
    FALED_WRITE,                  /**< Failed to write to a file */
    FAILED_ARENA,                 /**< Failed arena operation */
    STRUCT_NOT_MADE,              /**< Failed to create a structure */
    FAILED_TRANSCATION,           /**< Failed transaction */
    PROTECTED_RESOURCE,           /**< Attempted to access a protected resource */
    UNKNOWN_ERROR                 /**< Unknown or unspecified error */
}error_codes;




/*error codes ordered by recency in the call stack*/
/*Each "worker" gets an error call stsck*/
/*To finish error codes, concurrency needs to be done. Revist */
/**
 * @brief Converts an error code to its corresponding string representation
 * @param error The error code to convert
 * @return String representation of the error code
 */
static inline const char * get_error_string(error_codes error);


/**
 * @brief Creates a formatted error message from a list of error codes
 * @param error_codes Array of error codes, ordered by recency in the call stack
 * @param num_codes Number of error codes in the array
 * @param buf Buffer to store the formatted error message
 */
static inline void create_error_message(int* error_codes, int num_codes, char* buf) {
    if (num_codes <= 0 || buf == NULL) {
        return;
    }

    buf[0] = '\0';  
    int remaining_space = MAX_ERROR_MESSAGE_LENGTH - 1; 

    const char* main_error = get_error_string(error_codes[num_codes - 1]);
    int written = snprintf(buf, remaining_space, "Error: %s", main_error);
    
    if (written > 0) {
        remaining_space -= written;
        buf += written;
    }

    if (num_codes <= 1) return;

    written = snprintf(buf, remaining_space, "\nContributing factors:");
    if (written > 0) {
        remaining_space -= written;
        buf += written;
    }

    for (int i = num_codes - 2; i >= 0 && remaining_space > 0; i--) {
        const char* error_str = get_error_string(error_codes[i]);
        written = snprintf(buf, remaining_space, "\n  - %s", error_str);
        if (written > 0) {
            remaining_space -= written;
            buf += written;
        }
    }
}

static inline const char* get_error_string(error_codes error) {
    switch(error) {
        case OK:
            return "Operation successful";
        case INVALID_DATA:
            return "Invalid data";
        case INVALID_META_DATA:
            return "Invalid metadata";
        case INVALID_BLOCK:
            return "Invalid block";
        case INVALID_WAL_FILE:
            return "Invalid WAL file";
        case CRITICAL_MALLOC_FAILURE:
            return "Critical memory allocation failure";
        case NON_CRITICAL_MALLOC_FAILURE:
            return "Non-critical memory allocation failure";
        case FAILED_BUFFER_WRITE:
            return "Failed to write to buffer";
        case SST_F_INVALID:
            return "Invalid SST file";
        case FAILED_OPEN:
            return "Failed to open file";
        case FAILED_SEEK:
            return "Failed to seek in file";
        case FAILED_READ:
            return "Failed to read from file";
        case FALED_WRITE:
            return "Failed to write to file";
        case FAILED_ARENA:
            return "Failed arena operation";
        case STRUCT_NOT_MADE:
            return "Failed to create structure";
        case FAILED_TRANSCATION:
            return "Failed transaction";
        default:
            return "Unknown error";
    }
}

