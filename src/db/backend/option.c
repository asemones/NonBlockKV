#include "option.h"


void set_level_options_lin(level_options * opts, int num, uint64_t file_size, uint64_t bits_per_key,  uint64_t partition_size){
    for (int i= 0; i < num; i++){
        opts[i].file_size = file_size;
        opts[i].bits_per_key =  bits_per_key;
        opts[i].cached = false;
        opts[i].partition_size  =  partition_size;
    }
}