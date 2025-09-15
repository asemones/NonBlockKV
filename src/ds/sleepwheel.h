
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include "frontier.h"

#ifndef TW_TICK_NS
#define TW_TICK_NS 1000000ULL
#endif

typedef void (*tw_waker_t)(void *, void *);

typedef struct {
    uint64_t wake_me;
    void    *napper;
} sleep_entry_t;

typedef struct {
    frontier *pq;
    uint64_t  now_ns;
} timer_wheel_t;

static int cmp_entry(const void *a, const void *b) {
    const sleep_entry_t *ea = a, *eb = b;
    return (ea->wake_me > eb->wake_me) - (ea->wake_me < eb->wake_me);
}

static inline void tw_init(timer_wheel_t * tw) {
    tw->pq = Frontier(sizeof(sleep_entry_t), false, cmp_entry);
}

static inline void tw_add(timer_wheel_t *tw, uint64_t delay_ns, void *napper) {
    if (!delay_ns) delay_ns = 1;
    sleep_entry_t e = { .wake_me = tw->now_ns + delay_ns, .napper = napper };
    enqueue(tw->pq, &e);
}

static inline void tw_advance(timer_wheel_t *tw, uint64_t now_ns, tw_waker_t on_wake, void *user_ctx) {
    if (now_ns < tw->now_ns) return;
    tw->now_ns = now_ns;
    while (tw->pq->queue->len) {
        sleep_entry_t top;
        dequeue(tw->pq, &top);
        if (top.wake_me > now_ns) {
            enqueue(tw->pq, &top); 
            break; 
        }
        if (on_wake) {
            on_wake(top.napper, user_ctx);
        }
    }
}

static inline void tw_free(timer_wheel_t *tw) {
    if (!tw) return;
    free_front(tw->pq);
    free(tw);
}
