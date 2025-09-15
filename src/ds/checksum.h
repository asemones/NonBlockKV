#include <stdlib.h>
#include <immintrin.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdio.h>


/*write blazing fast checksum function-MUST outpace fastest nvme io speeds*/
#pragma once

/**
 * @brief Executes the CPUID instruction to get CPU information
 * @param info Array to store the CPU information
 * @param infoType Type of information to retrieve
 */
void cpuid(int info[4], int infoType);

/**
 * @brief Checks if AVX instructions are supported by the CPU
 * @return 1 if AVX is supported, 0 otherwise
 */
int is_avx_supported(void);

/**
 * @brief Initializes the CRC32 lookup table for faster computation
 */
void init_crc32_table(void);

/**
 * @brief Computes a CRC32 checksum for the given data
 * @param data Pointer to the data
 * @param length Length of the data in bytes
 * @return The computed CRC32 checksum
 */
uint32_t crc32(const void *data, size_t length);

/**
 * @brief Computes a CRC32 checksum using AVX2 instructions for faster processing
 * @param data Pointer to the data
 * @param length Length of the data in bytes
 * @return The computed CRC32 checksum
 */
uint32_t crc32_avx2(const uint8_t *data, size_t length);

/**
 * @brief Verifies data integrity by comparing a computed checksum with an expected value
 * @param data Pointer to the data to verify
 * @param len Length of the data in bytes
 * @param checksum Expected checksum value
 * @return true if the data is valid (checksums match), false otherwise
 */
bool verify_data(const uint8_t *data, size_t len, uint32_t checksum);
