#ifndef SLL_H
#define SLL_H

#include <stdlib.h>
#include "slab.h"

#define SLL_DECLARE(type,prefix)\
typedef struct prefix##_node {\
    type value;\
    struct prefix##_node *next;\
}prefix##_node;\
typedef struct {\
    prefix##_node *head;\
    slab_allocator allocator;\
}prefix##_list;\
static inline void prefix##_init(prefix##_list *l,size_t pagesz,size_t slab_count){\
    l->head=NULL;\
    l->allocator=create_allocator(pagesz,slab_count);\
}\
static inline void prefix##_destroy(prefix##_list *l){\
    prefix##_free(l);\
    free_slab_allocator(&l->allocator);\
}\
static inline prefix##_node *prefix##_new_node(prefix##_list *l,type v){\
    prefix##_node *n=slalloc(&l->allocator,sizeof *n);\
    if(n){\
        n->value=v;\
        n->next=NULL;\
    }\
    return n;\
}\
static inline void prefix##_push_front(prefix##_list *l,type v){\
    prefix##_node *n=prefix##_new_node(l,v);\
    if(!n)return;\
    n->next=l->head;\
    l->head=n;\
}\
static inline type prefix##_pop_front(prefix##_list *l){\
    prefix##_node *n=l->head;\
    if(!n)return (type){0};\
    l->head=n->next;\
    type v=n->value;\
    slfree(&l->allocator,n);\
    return v;\
}\
static inline void prefix##_remove(prefix##_list *l,prefix##_node *target){\
    if(!l->head||!target)return;\
    if(l->head==target){\
        l->head=target->next;\
    }else{\
        prefix##_node *prev=l->head;\
        while(prev->next&&prev->next!=target)prev=prev->next;\
        if(prev->next!=target)return;\
        prev->next=target->next;\
    }\
    slfree(&l->allocator,target);\
}\
static inline void prefix##_free(prefix##_list *l){\
    prefix##_node *n=l->head;\
    while(n){\
        prefix##_node *next=n->next;\
        slfree(&l->allocator,n);\
        n=next;\
    }\
    l->head=NULL;\
}

#endif
