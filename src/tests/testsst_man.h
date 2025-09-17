#include "../db/backend/sst_manager.h"
#include "./unity/src/unity.h"
#include "../ds/bloomfilter.h"




static sst_manager create_env(sst_man_sst_inf_cf config, uint64_t memsize){
    sst_manager m = create_manager(config, memsize);
    return m;
}
static sst_man_sst_inf_cf default_test_config(){
    sst_man_sst_inf_cf config;
    set_level_options_lin(config.levels, 7, 4096 * 256, 10, 4 * 1096);
    config.block_index_size = 4 * 1024;
    return config;
}
void test_create(){
    sst_man_sst_inf_cf config = default_test_config();
    sst_manager m = create_env(config, 256 * 1024);
    TEST_ASSERT_NOT_NULL(m.non_zero_l);
    TEST_ASSERT_NOT_NULL(m.cache.frames);
    TEST_ASSERT_NOT_NULL(m.sst_memory.cached.region);
    TEST_ASSERT_NOT_NULL(m.sst_memory.non_cached.region);
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(2, m.parts_per);
}
void test_make_ssts(){
    sst_man_sst_inf_cf config = default_test_config();
    sst_manager m = create_env(config, 256 * 1024);
    sst_f_inf * sst =  allocate_sst(&m,  100, 0);
    sst_f_inf *sst_l_2 = allocate_non_l0(&m, 1000, 2);
    TEST_ASSERT_NOT_NULL(sst->block_indexs->arr);
    TEST_ASSERT_NOT_NULL(sst_l_2->sst_partitions->arr);
}
static sst_f_inf * make_dummy_sst(sst_manager * m, const char * min, const char * max, int level){
    sst_f_inf * sst;
    if (level <=0){
        sst = allocate_sst(m, 100,level);
    }
    else{
        sst=  allocate_non_l0(m, 100, level);
    }
    f_str r_min = make_fstr((char*)min, strlen(min));
    f_str r_max = make_fstr((char*)min, strlen(max));

    f_cpy(&sst->max, &r_max);
    f_cpy(&sst->min, &r_min);
    if (sst->filter){
        map_bit(min, sst->filter);
        map_bit(max, sst->filter);
    }
    return sst;
}
static void quick_verify_sst(sst_f_inf * cand, sst_f_inf *expected){
    TEST_ASSERT_NOT_NULL(cand);
    TEST_ASSERT_EQUAL_INT(f_cmp(cand->min, expected->min), 0);
    TEST_ASSERT_EQUAL_INT(f_cmp(cand->max, expected->max), 0);
}
static void test_sst_add_remove_levels(int levels){
    sst_man_sst_inf_cf config = default_test_config();
    sst_manager m = create_env(config, 256 * 1024);
    sst_f_inf*sst1 = make_dummy_sst(&m, "2","4",levels);
    sst_f_inf* sst2 = make_dummy_sst(&m, "41","9",levels);
    add_sst(&m, sst1, 0);
    add_sst(&m, sst2,0);
    sst_f_inf * sst=  get_sst(&m, make_fstr("2", strlen("2")), levels);
    quick_verify_sst(sst, sst1);
    sst = get_sst(&m, make_fstr("4", strlen("4")), levels);
    quick_verify_sst(sst, sst1);
    sst = get_sst(&m, make_fstr("41", strlen("41")), levels);
    quick_verify_sst(sst, sst2);
    remove_sst(&m, sst, 0);
    /*ideally the "best fit" sst is NEVER returned
    when a perfect fit exists*/
    sst = get_sst(&m, make_fstr("41", strlen("41")), levels);
    TEST_ASSERT_NULL(sst);
    
}
void test_sst_add_remove_search(){
    test_sst_add_remove_levels(0);
    test_sst_add_remove_levels(3);
}