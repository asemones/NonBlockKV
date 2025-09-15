#include "checksum.h"
void cpuid(int info[4], int infoType) {
    __asm__ __volatile__ (
        "cpuid"
        : "=a"(info[0]), "=b"(info[1]), "=c"(info[2]), "=d"(info[3])
        : "a"(infoType)
    );
}


int is_avx_supported() {
    int cpuInfo[4];
    cpuid(cpuInfo, 1); 
    return (cpuInfo[2] & (1 << 28)) !=0;
}
static uint32_t crc32_table[256];

void init_crc32_table() {
    uint32_t polynomial = 0xEDB88320;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (uint32_t j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ polynomial;
            else
                crc >>= 1;
        }
        crc32_table[i] = crc;
    }
}

uint32_t crc32(const void *data, size_t length) {
    const uint8_t *bytes = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;
    
    for (size_t i = 0; i < length; i++) {
        uint8_t byte = bytes[i];
        uint8_t table_index = (crc ^ byte) & 0xFF;
        crc = (crc >> 8) ^ crc32_table[table_index];
    }

    return ~crc;
}

uint32_t crc32_avx2(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF; 
    size_t chunks = length / 32;
    size_t i;
    for (i = 0; i < chunks * 32; i += 32) {
        __m256i chunk = _mm256_loadu_si256((__m256i *)(data + i)); 

        for (int j = 0; j < 32; j++) {
            crc = _mm_crc32_u8(crc, ((uint8_t *)&chunk)[j]);
        }
    }
    for (; i < length; i++) {
        crc = _mm_crc32_u8(crc, data[i]);
    }

    return ~crc;
}
bool verify_data(const uint8_t * data, size_t len, uint32_t checksum){
    if (is_avx_supported()){
        uint32_t real_checksum = crc32_avx2(data, len);
        return real_checksum == checksum;
    }
    uint32_t real_checksum = crc32(data, len);
    return real_checksum == checksum;


}