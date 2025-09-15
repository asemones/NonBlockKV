#include "api.h"
/*
char * get(storage_engine * engine, char * key){
    snapshot *temp_snap = engine->meta->current_tx_copy;
    ref_snapshot(engine->meta->current_tx_copy);
    char * ret = read_record_snap(engine, key, temp_snap);
    deref_with_free(temp_snap);
    return ret;
}
char ** scan(storage_engine * engine, char * start, char * end){
    snapshot *temp_snap = engine->meta->current_tx_copy;
    ref_snapshot(engine->meta->current_tx_copy);
    aseDB_iter * iter = create_aseDB_iter();
    init_aseDB_iter(iter, engine);
    //implement scan_records
    //return scan_records(engine, start, end);
    
    deref_with_free(temp_snap);
    return NULL;
}
*/