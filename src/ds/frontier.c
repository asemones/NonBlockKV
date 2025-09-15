#include "frontier.h"
frontier* Frontier(size_t dts, bool is_alloc, int (*compare)(const void*, const void*)) {
    frontier *front = (frontier*)wrapper_alloc((sizeof(frontier)), NULL, NULL);
    if (front  == NULL ) return NULL;
    front->queue = List(16, dts, true);
    if (front->queue == NULL) return NULL;
    front->queue->isAlloc = is_alloc;
    front->compare = compare;
    return front;
}

void enqueue(frontier *front, void* element) {
    if (front == NULL || element == NULL) {
        return;
    }
    insert(front->queue, element);
    int i = front->queue->len - 1;
    while (i != 0 && front->compare(at(front->queue, (i - 1) / 2), at(front->queue, i)) >= 0) {
        swap(at(front->queue, (i - 1) / 2), at(front->queue, i), front->queue->dtS);
        i = (i - 1) / 2;
    }
}

void heapify(frontier *front, int index) {
    size_t size = front->queue->len;
    int largest = index;
    int left = 2 * index + 1;
    int right = 2 * index + 2;

    if (left < size) {
        void * left_element = at(front->queue,left );
        void * curr_element = at(front->queue, largest);
        if (front->compare(left_element, curr_element) <= 0){
            largest = left;
        }
        
    }
    if (right < size){
        void * right_element = at(front->queue ,right );
        void * curr_element = at(front->queue, largest);
        if (front->compare(right_element, curr_element) <= 0){
            largest = right;
        }
    }
    if (largest != index) {
        swap(at(front->queue, index), at(front->queue, largest), front->queue->dtS);
        heapify(front, largest);
    }
}

int dequeue(frontier* front, void* out_buffer) {
    if (front->queue->len == 0) {
        return -1; // or handle the empty queue case appropriately
    }

    void* root = at(front->queue, 0);

    memcpy(out_buffer, root, front->queue->dtS);

    void* last_element = at(front->queue, front->queue->len - 1);
   
    swap(root, last_element, front->queue->dtS);

    front->queue->len--;
    heapify(front, 0);
    return 0;
}

void* peek(frontier * front) {
    return at(front->queue, 0);
}
void debug_print(frontier * front){
    for (int i =0; i < front->queue->len; i++){
     kv* l =(kv*) at(front->queue,i);
     fprintf(stdout, "%s, %s\n",(char*)l->key, (char*)l->value);
    }
}
void free_front(frontier * front) {
    free_list(front->queue, NULL);
    free(front);
}
