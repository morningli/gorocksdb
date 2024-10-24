#include <stdlib.h>
#include "rocksdb/c.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Exported types */

typedef struct rocksdb_listener_t rocksdb_listener_t;
typedef struct rocksdb_status_t rocksdb_status_t;

// This API provides convenient C wrapper functions for rocksdb client.

/* Base */

extern void gorocksdb_destruct_handler(void* state);

/* CompactionFilter */

extern rocksdb_compactionfilter_t* gorocksdb_compactionfilter_create(uintptr_t idx);

/* Comparator */

extern rocksdb_comparator_t* gorocksdb_comparator_create(uintptr_t idx);

/* Filter Policy */

extern rocksdb_filterpolicy_t* gorocksdb_filterpolicy_create(uintptr_t idx);
extern void gorocksdb_filterpolicy_delete_filter(void* state, const char* v, size_t s);

/* Merge Operator */

extern rocksdb_mergeoperator_t* gorocksdb_mergeoperator_create(uintptr_t idx);
extern void gorocksdb_mergeoperator_delete_value(void* state, const char* v, size_t s);

/* Slice Transform */

extern rocksdb_slicetransform_t* gorocksdb_slicetransform_create(uintptr_t idx);

extern rocksdb_env_t* rocksdb_create_custom_env(uintptr_t idx);

extern rocksdb_listener_t* rocksdb_create_background_error_listener(uintptr_t idx);
extern rocksdb_listener_t* rocksdb_create_error_recover_listener(uintptr_t idx);

extern void rocksdb_options_set_listener(rocksdb_options_t * opts, rocksdb_listener_t* listener);

extern int rocksdb_status_code(rocksdb_status_t * status);
extern int rocksdb_status_subcode(rocksdb_status_t * status);
extern int rocksdb_status_severity(rocksdb_status_t * status);
extern char* rocksdb_status_getState(rocksdb_status_t * status);
extern char* rocksdb_status_ToString(rocksdb_status_t * status);

extern char* rocksdb_resume(rocksdb_t* db, char** errptr);

#ifdef __cplusplus
} /* end extern "C" */
#endif