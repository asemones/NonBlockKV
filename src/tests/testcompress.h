#include "../db/backend/compression.h"
#define TEST_DBUF_SIZE 1024 * 3000
#define TEST_BB_SIZE 1024 * 2000

byte_buffer * sst_stream;
byte_buffer * dest;
sst_f_inf inf;
/*finish these tests*/
void start(){
    inf = create_sst_empty();
    sst_stream = create_buffer(TEST_BB_SIZE);
    dest = create_buffer(TEST_BB_SIZE);

}
void end(){
    remove ("WAL_0.bin");
    remove("WAL_1.bin");
    remove("WAL_M.bin");
    remove("meta.bin");
    remove ("bloom.bin");
    free_sst_inf(&inf);
    free_buffer(sst_stream);
    free_buffer(dest);
}
void write_random_data(byte_buffer * buffer, const int iters){
   for (int i = 0; i < iters; i++){
        write_buffer(buffer,"helloworld", strlen("hellowrld"));
   }
}
void test_regular_compress(){
    start(); 
    write_random_data(sst_stream, 200);
    regular_compress(&inf.compr_info, sst_stream, dest, sst_stream->curr_bytes);
    TEST_ASSERT_NOT_EQUAL_INT(dest->curr_bytes, 0);
    end();
    
}
void test_regular_decompress(){
    start(); 
    write_random_data(sst_stream, 200);
    int bytes = sst_stream->curr_bytes;
    regular_compress(&inf.compr_info, sst_stream, dest, sst_stream->curr_bytes);
    sst_stream->curr_bytes = 0;
    regular_decompress(&inf.compr_info, dest, sst_stream, dest->curr_bytes);
    TEST_ASSERT_EQUAL_INT(sst_stream->curr_bytes, bytes);
    end();
}