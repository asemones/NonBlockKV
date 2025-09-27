// Copyright 2018 Sen Han <00hnes@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#define _GNU_SOURCE

#include "aco.h"
#include <stdio.h>
#include <stdint.h>

// this header including should be at the last of the `include` directives list
#include "aco_assert_override.h"

void aco_runtime_test(void){
    _Static_assert(sizeof(void*) == 8, "require 'sizeof(void*) == 8'");
    _Static_assert(sizeof(__uint128_t) == 16, "require 'sizeof(__uint128_t) == 16'");
    _Static_assert(sizeof(int) >= 4, "require 'sizeof(int) >= 4'");
    assert(sizeof(int) >= 4);
    _Static_assert(sizeof(int) <= sizeof(size_t), "require 'sizeof(int) <= sizeof(size_t)'");
    assert(sizeof(int) <= sizeof(size_t));
}

// Note: dst and src must be valid and 16-byte aligned.
// sz must be 16*n + 8 where 0 <= n <= 8
#define aco_amd64_inline_short_aligned_memcpy_test_ok(dst, src, sz) \
    (   \
        (((uintptr_t)(src) & 0x0f) == 0) && (((uintptr_t)(dst) & 0x0f) == 0) \
        &&  \
        (((sz) & 0x0f) == 0x08) && (((sz) >> 4) <= 8) \
    )

#define aco_amd64_inline_short_aligned_memcpy(dst, src, sz) do { \
    __uint128_t xmm0, xmm1, xmm2, xmm3, xmm4, xmm5, xmm6, xmm7; \
                                                              \
    switch ((sz) >> 4) {                      \
        case 0:                                               \
            break;                                                \
                                                              \
        case 1:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            break;                                                \
                                                              \
        case 2:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            break;                                                \
                                                              \
        case 3:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            xmm2 = *((__uint128_t*)(src) + 2);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            *((__uint128_t*)(dst) + 2) = xmm2;                      \
            break;                                                \
                                                              \
        case 4:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            xmm2 = *((__uint128_t*)(src) + 2);                      \
            xmm3 = *((__uint128_t*)(src) + 3);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            *((__uint128_t*)(dst) + 2) = xmm2;                      \
            *((__uint128_t*)(dst) + 3) = xmm3;                      \
            break;                                                \
                                                              \
        case 5:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            xmm2 = *((__uint128_t*)(src) + 2);                      \
            xmm3 = *((__uint128_t*)(src) + 3);                      \
            xmm4 = *((__uint128_t*)(src) + 4);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            *((__uint128_t*)(dst) + 2) = xmm2;                      \
            *((__uint128_t*)(dst) + 3) = xmm3;                      \
            *((__uint128_t*)(dst) + 4) = xmm4;                      \
            break;                                                \
                                                              \
        case 6:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            xmm2 = *((__uint128_t*)(src) + 2);                      \
            xmm3 = *((__uint128_t*)(src) + 3);                      \
            xmm4 = *((__uint128_t*)(src) + 4);                      \
            xmm5 = *((__uint128_t*)(src) + 5);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            *((__uint128_t*)(dst) + 2) = xmm2;                      \
            *((__uint128_t*)(dst) + 3) = xmm3;                      \
            *((__uint128_t*)(dst) + 4) = xmm4;                      \
            *((__uint128_t*)(dst) + 5) = xmm5;                      \
            break;                                                \
                                                              \
        case 7:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            xmm2 = *((__uint128_t*)(src) + 2);                      \
            xmm3 = *((__uint128_t*)(src) + 3);                      \
            xmm4 = *((__uint128_t*)(src) + 4);                      \
            xmm5 = *((__uint128_t*)(src) + 5);                      \
            xmm6 = *((__uint128_t*)(src) + 6);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            *((__uint128_t*)(dst) + 2) = xmm2;                      \
            *((__uint128_t*)(dst) + 3) = xmm3;                      \
            *((__uint128_t*)(dst) + 4) = xmm4;                      \
            *((__uint128_t*)(dst) + 5) = xmm5;                      \
            *((__uint128_t*)(dst) + 6) = xmm6;                      \
            break;                                                \
                                                              \
        case 8:                                               \
            xmm0 = *((__uint128_t*)(src) + 0);                      \
            xmm1 = *((__uint128_t*)(src) + 1);                      \
            xmm2 = *((__uint128_t*)(src) + 2);                      \
            xmm3 = *((__uint128_t*)(src) + 3);                      \
            xmm4 = *((__uint128_t*)(src) + 4);                      \
            xmm5 = *((__uint128_t*)(src) + 5);                      \
            xmm6 = *((__uint128_t*)(src) + 6);                      \
            xmm7 = *((__uint128_t*)(src) + 7);                      \
            *((__uint128_t*)(dst) + 0) = xmm0;                      \
            *((__uint128_t*)(dst) + 1) = xmm1;                      \
            *((__uint128_t*)(dst) + 2) = xmm2;                      \
            *((__uint128_t*)(dst) + 3) = xmm3;                      \
            *((__uint128_t*)(dst) + 4) = xmm4;                      \
            *((__uint128_t*)(dst) + 5) = xmm5;                      \
            *((__uint128_t*)(dst) + 6) = xmm6;                      \
            *((__uint128_t*)(dst) + 7) = xmm7;                      \
            break;                                                \
    }                                                             \
                                                              \
    /* Unconditionally copy the final 8 bytes to handle the `16*n + 8` size. */ \
    *((uint64_t*)((uintptr_t)(dst) + (sz) - 8)) = *((uint64_t*)((uintptr_t)(src) + (sz) - 8)); \
} while (0)
#define aco_amd64_optimized_memcpy_drop_in(dst, src, sz) do {\
    if(aco_amd64_inline_short_aligned_memcpy_test_ok((dst), (src), (sz))){ \
        aco_amd64_inline_short_aligned_memcpy((dst), (src), (sz)); \
    }else{ \
        memcpy((dst), (src), (sz)); \
    } \
} while(0)

void aco_yield_to(aco_t *next){
    aco_t *self = aco_gtls_co;

    if (self->share_stack->owner != self) {
        if (self->share_stack->owner != NULL) {
            aco_t *owner = self->share_stack->owner;

            owner->save_stack.valid_sz =
                (uintptr_t)owner->share_stack->align_retptr -
                (uintptr_t)owner->reg[ACO_REG_IDX_SP];

            if (owner->save_stack.valid_sz > owner->save_stack.sz) {
                size_t new_sz = owner->save_stack.sz;
                do new_sz <<= 1; while (new_sz < owner->save_stack.valid_sz);
                owner->save_stack.ptr = realloc(owner->save_stack.ptr, new_sz);
                owner->save_stack.sz  = new_sz;
                assert(owner->save_stack.ptr);
            }

            if (owner->save_stack.valid_sz > 0) {
                aco_amd64_optimized_memcpy_drop_in(
                    owner->save_stack.ptr,
                    owner->reg[ACO_REG_IDX_SP],
                    owner->save_stack.valid_sz);
            }
            owner->share_stack->owner = NULL;
            owner->share_stack->align_validsz = 0;
        }
    }

    if (next->share_stack->owner != next) {
        assert(next->save_stack.valid_sz <=
               next->share_stack->align_limit - sizeof(void*));

        if (next->save_stack.valid_sz > 0) {
            aco_amd64_optimized_memcpy_drop_in(
                (void *)((uintptr_t)next->share_stack->align_retptr -
                         next->save_stack.valid_sz),
                next->save_stack.ptr,
                next->save_stack.valid_sz);
        }
        next->share_stack->align_validsz =
            next->save_stack.valid_sz + sizeof(void*);
        next->share_stack->owner = next;
    }

    aco_gtls_co = next;        
    acosw(self, next);           
    aco_gtls_co = self; 
}

static void aco_default_protector_last_word(void){
    aco_t* co = aco_get_co();
    fprintf(stderr,"error: aco_default_protector_last_word triggered\n");
    fprintf(stderr, "error: co:%p should call `aco_exit()` instead of direct "
        "`return` in co_fp:%p to finish its execution\n", co, (void*)co->fp);
    assert(0);
}

__thread aco_t* aco_gtls_co;
static __thread aco_cofuncp_t aco_gtls_last_word_fp = aco_default_protector_last_word;
static __thread void* aco_gtls_fpucw_mxcsr[1];

void aco_thread_init(aco_cofuncp_t last_word_co_fp){
    aco_save_fpucw_mxcsr(aco_gtls_fpucw_mxcsr);
    if(last_word_co_fp != NULL) {
        aco_gtls_last_word_fp = last_word_co_fp;
    }
}

void aco_funcp_protector(void){
    if(aco_gtls_last_word_fp != NULL){
        aco_gtls_last_word_fp();
    } else {
        aco_default_protector_last_word();
    }
    assert(0);
}

aco_share_stack_t* aco_share_stack_new(size_t sz){
    return aco_share_stack_new2(sz, 1);
}

#define aco_size_t_safe_add_assert(a,b) do { assert((a)+(b) >= (a)); } while(0)

aco_share_stack_t* aco_share_stack_new2(size_t sz, char guard_page_enabled){
    if(sz == 0){
        sz = 1024 * 1024 * 2; // 2MB default
    }
    if(sz < 512){
        sz = 512;
    }

    if(guard_page_enabled != 0){
        long pgsz_long = sysconf(_SC_PAGESIZE);
        assert(pgsz_long > 0 && (((pgsz_long - 1) & pgsz_long) == 0));
        size_t pgsz = (size_t)pgsz_long;

        if(sz <= pgsz){
            sz = pgsz * 2;
        } else {
            size_t new_sz;
            if((sz & (pgsz - 1)) != 0){
                new_sz = (sz & (~(pgsz - 1))) + (pgsz * 2);
            } else {
                new_sz = sz + pgsz;
            }
            sz = new_sz;
        }
    }

    aco_share_stack_t* p = malloc(sizeof(aco_share_stack_t));
    assertalloc_ptr(p);
    memset(p, 0, sizeof(aco_share_stack_t));

    if(guard_page_enabled != 0){
        size_t pgsz = (size_t)sysconf(_SC_PAGESIZE);
        p->real_ptr = mmap(NULL, sz, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        assertalloc_bool(p->real_ptr != MAP_FAILED);
        assert(0 == mprotect(p->real_ptr, pgsz, PROT_READ));

        p->guard_page_enabled = 1;
        p->ptr = (void*)(((uintptr_t)p->real_ptr) + pgsz);
        p->real_sz = sz;
        p->sz = sz - pgsz;
    } else {
        p->sz = sz;
        p->ptr = malloc(sz);
        assertalloc_ptr(p->ptr);
    }
    
    p->owner = NULL;
#ifdef ACO_USE_VALGRIND
    p->valgrind_stk_id = VALGRIND_STACK_REGISTER(p->ptr, (void*)((uintptr_t)p->ptr + p->sz));
#endif
    
    uintptr_t u_p = (uintptr_t)p->ptr + p->sz - (sizeof(void*) * 2);
    u_p = (u_p >> 4) << 4; // 16-byte alignment
    p->align_highptr = (void*)u_p;
    p->align_retptr  = (void*)(u_p - sizeof(void*)); 
    *((void**)(p->align_retptr)) = (void*)(aco_funcp_protector_asm);
    p->align_limit = p->sz - 16 - (sizeof(void*) * 2);
    
    return p;
}

void aco_share_stack_destroy(aco_share_stack_t* sstk){
    assert(sstk != NULL && sstk->ptr != NULL);
#ifdef ACO_USE_VALGRIND
    VALGRIND_STACK_DEREGISTER(sstk->valgrind_stk_id);
#endif
    if(sstk->guard_page_enabled){
        assert(0 == munmap(sstk->real_ptr, sstk->real_sz));
    } else {
        free(sstk->ptr);
    }
    sstk->ptr = NULL;
    sstk->real_ptr = NULL;
    free(sstk);
}

aco_t* aco_create(
        aco_t* main_co, aco_share_stack_t* share_stack, 
        size_t save_stack_sz, aco_cofuncp_t fp, void* arg
    ){
    aco_t* p = malloc(sizeof(aco_t));
    assertalloc_ptr(p);
    memset(p, 0, sizeof(aco_t));

    if(main_co != NULL){ // This is a non-main coroutine
        assertptr(share_stack);
        p->share_stack = share_stack;
        p->reg[ACO_REG_IDX_RETADDR] = (void*)fp;
        p->reg[ACO_REG_IDX_SP] = p->share_stack->align_retptr;
        #ifndef ACO_CONFIG_SHARE_FPU_MXCSR_ENV
            p->reg[ACO_REG_IDX_FPU] = aco_gtls_fpucw_mxcsr[0];
        #endif
        p->main_co = main_co;
        p->arg = arg;
        p->fp = fp;
        
        if(save_stack_sz == 0){
            save_stack_sz = 64;
        }
        p->save_stack.sz = save_stack_sz;
        p->save_stack.ptr = malloc(save_stack_sz);
        assertalloc_ptr(p->save_stack.ptr);
        p->save_stack.valid_sz = 0;
        
        return p;
    } else { // This is a main coroutine
        p->main_co = NULL;
        p->arg = arg;
        p->fp = fp;
        p->share_stack = NULL;
        p->save_stack.ptr = NULL;
        return p;
    }
}

aco_attr_no_asan
void aco_resume(aco_t* resume_co){
    assert(resume_co != NULL && resume_co->main_co != NULL && resume_co->is_end == 0);
    
    if(resume_co->share_stack->owner != resume_co){
        if(resume_co->share_stack->owner != NULL){
            aco_t* owner_co = resume_co->share_stack->owner;
            assert(owner_co->share_stack == resume_co->share_stack);
            
            owner_co->save_stack.valid_sz = 
                (uintptr_t)(owner_co->share_stack->align_retptr) - (uintptr_t)(owner_co->reg[ACO_REG_IDX_SP]);
            
            if(owner_co->save_stack.sz < owner_co->save_stack.valid_sz){
                free(owner_co->save_stack.ptr);
                size_t new_sz = owner_co->save_stack.sz;
                while(new_sz < owner_co->save_stack.valid_sz) {
                    new_sz <<= 1;
                }
                owner_co->save_stack.sz = new_sz;
                owner_co->save_stack.ptr = malloc(owner_co->save_stack.sz);
                assertalloc_ptr(owner_co->save_stack.ptr);
            }
            
            if(owner_co->save_stack.valid_sz > 0) {
                aco_amd64_optimized_memcpy_drop_in(
                    owner_co->save_stack.ptr,
                    owner_co->reg[ACO_REG_IDX_SP], 
                    owner_co->save_stack.valid_sz
                );
            }
            
            if(owner_co->save_stack.valid_sz > owner_co->save_stack.max_cpsz){
                owner_co->save_stack.max_cpsz = owner_co->save_stack.valid_sz;
            }
            
            owner_co->share_stack->owner = NULL;
            owner_co->share_stack->align_validsz = 0;
        }
        
        assert(resume_co->share_stack->owner == NULL);
        assert(resume_co->save_stack.valid_sz <= resume_co->share_stack->align_limit - sizeof(void*));
        
        if(resume_co->save_stack.valid_sz > 0) {
            aco_amd64_optimized_memcpy_drop_in(      
                (void*)((uintptr_t)(resume_co->share_stack->align_retptr) - resume_co->save_stack.valid_sz), 
                resume_co->save_stack.ptr,
                resume_co->save_stack.valid_sz
            );
        }
        
        if(resume_co->save_stack.valid_sz > resume_co->save_stack.max_cpsz){
            resume_co->save_stack.max_cpsz = resume_co->save_stack.valid_sz;
        }
        
        resume_co->share_stack->align_validsz = resume_co->save_stack.valid_sz + sizeof(void*);
        resume_co->share_stack->owner = resume_co;
    }
    
    aco_gtls_co = resume_co;
    acosw(resume_co->main_co, resume_co);
    aco_gtls_co = resume_co->main_co;
}

void aco_destroy(aco_t* co){
    assertptr(co);
    if(aco_is_main_co(co)){
        free(co);               
    } else {
        if(co->share_stack->owner == co){
            co->share_stack->owner = NULL;
            co->share_stack->align_validsz = 0;
        }
        free(co->save_stack.ptr);
        co->save_stack.ptr = NULL;
        free(co);
    }
}