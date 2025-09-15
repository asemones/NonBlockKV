#include "arena.h"
arena *malloc_arena(size_t size) {
    arena *a = (arena *)malloc(sizeof(arena));
    if (!a) return NULL;

    a->data = malloc(size);
    if (!a->data) {
        free(a);
        return NULL;
    }

    a->size = size;
    a->capacity = 0;
    a->next = NULL;
    a->cursor= NULL;
    a->free = false;
    return a;
}

arena *calloc_arena(size_t size) {
    arena *a = (arena *)malloc(sizeof(arena));
    if (!a) return NULL;

    a->data = calloc(1, size);
    if (!a->data) {
        free(a);
        return NULL;
    }

    a->size = size;
    a->capacity = 0;
    a->next = NULL;
    a->cursor= NULL;
    a->free = false;
    return a;
}
void reset_arena(void *va) {
    arena * a = (arena*)va;
    memset(a->data, 0, a->size);
    a->capacity = 0;
    a->next  = NULL;
    a->cursor = NULL;
}
void reset_expanded_arena(arena * a){
    arena * temp = a;
    while (temp->next != NULL){
        temp->capacity = 0;
        temp = temp->next;
    }
    a->cursor = a;
}

void *arena_alloc(arena *a, size_t size) {
    if (a->capacity + size > a->size) {
        return NULL;
    }
    void *temp = &((char *)a->data)[a->capacity];
    a->capacity += size;
    return temp;
}
void *arena_alloc_expand(arena *a, size_t size, struct_pool * src) {
    if (a->next != NULL && a->capacity + size > a->size){
            a->cursor = a->next;
    }
    else if (src == NULL && a->capacity + size > a->size){
        a->next = malloc_arena(max(size * 2, a->size));
        a->cursor = a->next;
        a->cursor->free = true;
    }
    else if (a->capacity + size > a->size) {
        if (a->cursor == NULL){
            a->next = request_struct(src);
            a->cursor = a->next;
        }
        else if (a->next == NULL){
            a->next = malloc_arena(max(size * 2, a->size));
            a->cursor = a->next;
            a->cursor->free = true;
        }
        return arena_alloc(a->cursor, size);
    }
    return arena_alloc(a, size);
}
void free_arena(void *a) {
    arena *aren = (arena*)a;
    if (aren == NULL) return;
    free(aren->data);
    free(aren);
}
void return_arena(arena * a, struct_pool * pool){
    arena * temp = a;
    while (temp != NULL){
        arena * t = temp;
        temp = temp->next;
        if (t->free){
            free_arena(t);
        }
        else{
            return_struct(pool, t, &reset_arena);
        }
    }
}
