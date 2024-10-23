// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include "listener.h"
#include "rocksdb/options.h"

#include <sys/time.h>
#include <time.h>

#include <cstdlib>
#include <mutex>
#include <utility>

#ifdef __cplusplus
extern "C" {
#endif

struct rocksdb_listener_t {
    rocksdb::EventListener* rep;
};
struct rocksdb_status_t {
    rocksdb::Status* rep;
};

struct rocksdb_options_t_tmp { 
    rocksdb::Options rep; 
};

#ifdef __cplusplus
} /* end extern "C" */
#endif


namespace rocksdb {

void BackgroundErrorListener::OnBackgroundError(BackgroundErrorReason reason ,Status* bg_error) {
    rocksdb_status_t* s = new(rocksdb_status_t);
    s->rep = bg_error;
    ((void (*)(void*, int, rocksdb_status_t*))(gorocksdb_listener_OnBackgroundError))(_pointer, static_cast<int>(reason), s);
    delete s;
}

}  // namespace rocksdb

#ifdef __cplusplus
extern "C" {
#endif

rocksdb_listener_t* rocksdb_create_background_error_listener(uintptr_t idx) {
    rocksdb_listener_t* result = new rocksdb_listener_t;
    result->rep = new rocksdb::BackgroundErrorListener((void*)idx);
    return result;
}

void rocksdb_options_set_listener(rocksdb_options_t * opts, rocksdb_listener_t* listener) {
  rocksdb_options_t_tmp* o = (rocksdb_options_t_tmp*)opts;  
  o->rep.listeners.emplace_back(listener->rep);
}

#ifdef __cplusplus
} /* end extern "C" */
#endif

#pragma GCC diagnostic pop