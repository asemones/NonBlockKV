#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include "alloc_util.h"
#include <fcntl.h>
#include "../ds/list.h" // Still needed for list functions if used directly
#include <sys/resource.h>
#include "aco.h" // Still needed for aco functions
#include "io_types.h" // Include the extracted types


#ifndef IO_H
#define IO_H
#define DEFAULT_READ_FLAGS (O_RDONLY | __O_DIRECT )
#define DEFAULT_WRT_FLAGS (O_WRONLY | O_CREAT | O_APPEND)
#define DEFAULT_PERMS (S_IRUSR  | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
#define STD_IO_CB 0
#define ASYN_CB 1
// Type definitions moved to io_types.h
int do_open(const char * fn, struct db_FILE * req, struct io_manager * manager);
int do_read(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager);
int do_write(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager);
int do_close(int fd, struct db_FILE * req, struct io_manager * manager);
int do_fsync(struct db_FILE * file, struct io_manager * manager);
void do_async_rw(struct db_FILE * req, off_t offset, size_t len, struct io_manager * manager);
static inline void dbio_close(struct db_FILE * request){
    do_close(request->desc.fd, request, man);
    return_struct(man->io_requests, request, NULL);
    request = NULL;
}
static inline void adbio_write(struct db_FILE * request, off_t off, size_t len){
    request->op = WRITE;
    request->behavior = FORK_JOIN;
    do_async_rw(request, off, len, man);
}
static inline void  adbio_read(struct db_FILE * request, off_t off, size_t len){
    request->op = READ;
    request->behavior= FORK_JOIN;
    do_async_rw(request, off, len, man);
}
static inline void  adbio_close(struct db_FILE * request, off_t off, size_t len){
    do_close(request->desc.fd, request, man);
    request->behavior= ASYNC_AWAIT;
    return_struct(man->io_requests, request, NULL);
    request = NULL;
}
static inline int adbio_open(struct db_FILE * request, off_t off, size_t len){
    return 0;
}
static inline int dbio_write(struct db_FILE * request, off_t off, size_t len){
    request->op = WRITE;
    return do_write(request, off, len,man);
}
static inline int dbio_read(struct db_FILE * request, off_t off, size_t len){
    request->op = READ;
    return do_read(request, off, len,man);
}
static inline int dbio_unfixed_read(struct db_FILE * request, off_t off, size_t len){
    request->op = UNFIXED_READ;
    return do_read(request, off, len,man);
}
static inline int dbio_unfixed_write(struct db_FILE * request, off_t off, size_t len){
    request->op = UNFIXED_WRITE;
    return do_write(request, off, len,man);
}
static inline void dbio_fsync(struct db_FILE * request){
    do_fsync(request, man);
}
static inline size_t sizeofdb_FILE(){
    return sizeof(db_FILE);
}
static inline void set_context_buffer(struct db_FILE * request, byte_buffer * buf){
    request->buf = buf;
}
static inline void return_ctx(struct db_FILE * request){
    return_struct(man->io_requests, request, NULL);
}
static inline db_FILE*  get_ctx(){
    return request_struct(man->io_requests);
}
struct db_FILE * dbio_open(const char * file_name, char  mode);


/**
 * @brief Wrapper function for fwrite
 * @param ptr Pointer to the data to be written
 * @param size Size of each element to be written
 * @param n Number of elements to be written
 * @param file Pointer to the FILE object
 * @return Number of elements successfully written
 */
static inline int write_wrapper(void * ptr, size_t size, size_t n, FILE * file ){
    return fwrite(ptr, size, n, file);
}
/**
 * @brief Wrapper function for fread
 * @param ptr Pointer to the buffer where read data will be stored
 * @param size Size of each element to be read
 * @param n Number of elements to be read
 * @param file Pointer to the FILE object
 * @return Number of elements successfully read
 */
static inline int read_wrapper(void  * ptr, size_t size, size_t n, FILE * file ){
    return fread(ptr, size, n, file);
}
/**
 * @brief Writes data to a file
 * @param buf Pointer to the buffer containing data to be written
 * @param file Path to the file
 * @param mode File opening mode
 * @param bytes Size of each element to be written
 * @param element Number of elements to be written
 * @return Number of elements successfully written or -1 on error
 */
static inline int write_file(char * buf, char * file, char * mode, size_t bytes, size_t element);

/**
 * @brief Reads data from a file
 * @param buf Pointer to the buffer where read data will be stored
 * @param file Path to the file
 * @param mode File opening mode
 * @param bytes Size of each element to be read
 * @param element Number of elements to be read (if 0, reads entire file)
 * @return Number of elements successfully read or -1 on error
 */
static inline int read_file( char * buf, char * file, char * mode, size_t bytes, size_t element);

static inline int read_file( char * buf, char * file, char * mode, size_t bytes, size_t element)
{
    FILE *File= fopen(file, mode);
    if (File == NULL){
        perror("fopen");
        return -1;
    }
    fseek (File, 0, SEEK_END);
    if (element == 0){
        element= ftell (File);
    }
    fseek (File, 0, SEEK_SET);
    int read_size = fread(buf, bytes, element ,File);
    fclose(File);
    return read_size;
}

static inline int write_file(char * buf, char * file, char * mode, size_t bytes, size_t element){
    FILE *File= fopen(file, mode);
    if (File == NULL){
        perror("fopen");
        return -1;
    }
    int written_size = fwrite(buf, bytes, element ,File);
    fclose(File);
    return written_size;
}
/**
 * @brief Gets the size of a file
 * @param file Pointer to the FILE object
 * @return Size of the file in bytes
 */
static inline long get_file_size(FILE *file) {
    long size;
    
    fseek(file, 0, SEEK_END);
    size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    return size;
}
byte_buffer * select_buffer(int size);
void init_io_manager(struct io_manager * manage, int num_4kb, int num_sst_tble, int num_memtable, int sst_tbl_s, int mem_tbl_s);
int add_open_close_requests(struct io_uring *ring, struct db_FILE * requests, int seq);
int add_read_write_requests(struct io_uring *ring, struct io_manager *manage, struct db_FILE *requests, int seq);
int process_completions(struct io_uring *ring, void(*func) (void *, enum post_io_behavior));
int chain_open_op_close(struct io_uring *ring, struct io_manager * m, struct db_FILE * req);
void init_db_FILE_ctx(const int max_concurrent_ops, db_FILE * dbs);
void return_buffer(byte_buffer * buff);
void io_prep_in(struct io_manager * io_manager, io_config config, aio_callback std_func);
bool try_submit_interface(struct io_uring *ring);
void perform_tuning(io_batch_tuner *t);
#endif