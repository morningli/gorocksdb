// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include "listener.h"
#include "_cgo_export.h"

#include <sys/time.h>
#include <time.h>
#include <cstdlib>
#include <mutex>
#include <utility>

#include "rocksdb/options.h"

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
    rocksdb_status_t* s = new rocksdb_status_t;
    s->rep = bg_error;
    ((void (*)(void*, int, rocksdb_status_t*))(gorocksdb_listener_OnBackgroundError))(_pointer, static_cast<int>(reason), s);
    delete s;
}

void ErrorRecoverListener::OnErrorRecoveryBegin(BackgroundErrorReason reason, Status bg_error, bool* auto_recovery ) {
    rocksdb_status_t* s = new rocksdb_status_t;
    s->rep = &bg_error;
    ((void (*)(void*, int, rocksdb_status_t*, bool*))(gorocksdb_listener_OnErrorRecoveryBegin))(_pointer, static_cast<int>(reason), s, auto_recovery);
    delete s;
}

void ErrorRecoverListener::OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& info) {
    rocksdb_status_t* o = new rocksdb_status_t;
    rocksdb_status_t* n = new rocksdb_status_t;
    o->rep = const_cast<rocksdb::Status*>(&info.old_bg_error);
    n->rep = const_cast<rocksdb::Status*>(&info.new_bg_error);
    ((void (*)(void*, rocksdb_status_t*, rocksdb_status_t*))(gorocksdb_listener_OnErrorRecoveryEnd))(_pointer, o, n);
    delete o;
    delete n;
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

rocksdb_listener_t* rocksdb_create_error_recover_listener(uintptr_t idx) {
    rocksdb_listener_t* result = new rocksdb_listener_t;
    result->rep = new rocksdb::ErrorRecoverListener((void*)idx);
    return result;
}

void rocksdb_options_set_listener(rocksdb_options_t * opts, rocksdb_listener_t* listener) {
  rocksdb_options_t_tmp* o = (rocksdb_options_t_tmp*)opts;  
  o->rep.listeners.emplace_back(listener->rep);
}

int rocksdb_status_code(rocksdb_status_t * status) {
    return static_cast<int>(status->rep->code());
}

int rocksdb_status_subcode(rocksdb_status_t * status) {
    return static_cast<int>(status->rep->subcode());
}

int rocksdb_status_severity(rocksdb_status_t * status) {
    return static_cast<int>(status->rep->severity());
}

char* rocksdb_status_getState(rocksdb_status_t * status) {
    return const_cast<char*>(status->rep->getState());
}

#ifdef __cplusplus
} /* end extern "C" */
#endif

#pragma GCC diagnostic pop