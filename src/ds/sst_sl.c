
#include "sst_sl.h"
#include "slab.h"

static sst_node *slab_alloc_func(slab_allocator *alloc, int lvl) {
    (void)lvl;
    return (sst_node *)slalloc(alloc, sizeof(sst_node) + sizeof(sst_node *) * (size_t)MAX_LEVEL);
}

static sst_node *create_sst_sl_node(int level, sst_f_inf *src, slab_allocator *allocator) {
    sst_node *node = slab_alloc_func(allocator, level);
    if (!node) return NULL;
    memset(&node->min_key, 0, sizeof(node->min_key));
    node->inf = NULL;
    for (int i = 0; i < MAX_LEVEL; i++) node->forward[i] = NULL;
    if (src) {
        node->inf = src;
        node->min_key = src->min;
    }
    return node;
}

sst_sl* create_sst_sl(int max_nodes) {
    sst_sl* list = (sst_sl*)malloc(sizeof(sst_sl));
    if (!list) return NULL;
    list->level = 1;
    list->use_default = 1;
    size_t slab_size = sizeof(sst_node) + sizeof(sst_node*) * (size_t)MAX_LEVEL;
    list->allocator = create_allocator(slab_size, max_nodes);
    list->header = create_sst_sl_node(MAX_LEVEL, NULL, &list->allocator);
    if (!list->header) {
        free(list);
        return NULL;
    }
    return list;
}

int random_level2() {
    int level = 1;
    while (fast_coin() && level < MAX_LEVEL) level++;
    return level;
}

int sst_insert_list(sst_sl* list, sst_f_inf* sst) {
    sst_node* update[MAX_LEVEL];
    sst_node* x = list->header;
    f_str k = sst->min;
    for (int i = list->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->inf && f_cmp(x->forward[i]->min_key, k) < 0) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    int level = random_level2();
    if (level > list->level) {
        for (int i = list->level; i < level; i++) update[i] = list->header;
        list->level = level;
    }
    sst_node* n = create_sst_sl_node(level, sst, &list->allocator);
    if (!n) return STRUCT_NOT_MADE;
    for (int i = 0; i < level; i++) {
        n->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = n;
    }
    return 0;
}

sst_node* sst_search_list(sst_sl* list, f_str k) {
    sst_node* x = list->header;
    for (int i = list->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->inf && f_cmp(x->forward[i]->min_key, k) < 0) {
            x = x->forward[i];
        }
    }
    x = x->forward[0];
    if (x && x->inf && f_cmp(x->min_key, k) == 0) return x;
    return NULL;
}

sst_node* sst_search_list_prefix(sst_sl* list, f_str k) {
    sst_node* x = list->header;
    for (int i = list->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->inf && f_cmp(x->forward[i]->min_key, k) < 0) {
            x = x->forward[i];
        }
    }
    return x->forward[0];
}

sst_node * sst_search_between(sst_sl * list, f_str k){
    sst_node* x = list->header;
    for (int i = list->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->inf && f_cmp(x->forward[i]->min_key, k) <= 0) {
            x = x->forward[i];
        }
    }
    if (x && x != list->header && x->inf &&
        f_cmp(k, x->min_key) >= 0 && f_cmp(k, x->inf->max) <= 0) return x;
    sst_node* y = x ? x->forward[0] : NULL;
    if (y && y->inf &&
        f_cmp(k, y->min_key) >= 0 && f_cmp(k, y->inf->max) <= 0) return y;
    return NULL;
}

void sst_delete_element(sst_sl* list, f_str k) {
    sst_node* update[MAX_LEVEL];
    sst_node* x = list->header;
    for (int i = list->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->inf && f_cmp(x->forward[i]->min_key, k) < 0) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    x = x->forward[0];
    if (x && x->inf && f_cmp(x->min_key, k) == 0) {
        for (int i = 0; i < list->level; i++) {
            if (update[i]->forward[i] != x) break;
            update[i]->forward[i] = x->forward[i];
        }
        while (list->level > 1 && list->header->forward[list->level - 1] == NULL) list->level--;
    }
}

void freesst_sl(sst_sl* list) {
    if (!list) return;
    free_slab_allocator(&list->allocator);
    free(list);
}

