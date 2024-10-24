// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "rocksdb/status.h"
#include "rocksdb/utilities/env_mirror.h"
#include "rocksdb/listener.h"

namespace rocksdb {
class BackgroundErrorListener : public EventListener {
 public:
  BackgroundErrorListener(void* pointer): _pointer(pointer) {}

  // A callback function for RocksDB which will be called before setting the
  // background error status to a non-OK value. The new background error status
  // is provided in `bg_error` and can be modified by the callback. E.g., a
  // callback can suppress errors by resetting it to Status::OK(), thus
  // preventing the database from entering read-only mode. We do not provide any
  // guarantee when failed flushes/compactions will be rescheduled if the user
  // suppresses an error.
  //
  // Note that this function can run on the same threads as flush, compaction,
  // and user writes. So, it is extremely important not to perform heavy
  // computations or blocking calls in this function.
  virtual void OnBackgroundError(BackgroundErrorReason /* reason */,Status* /* bg_error */);
 
 private:
    void* _pointer;
};

class ErrorRecoverListener : public EventListener {
 public:
  ErrorRecoverListener(void* pointer): _pointer(pointer) {}
  
  // A callback function for RocksDB which will be called just before
  // starting the automatic recovery process for recoverable background
  // errors, such as NoSpace(). The callback can suppress the automatic
  // recovery by setting *auto_recovery to false. The database will then
  // have to be transitioned out of read-only mode by calling DB::Resume()
  virtual void OnErrorRecoveryBegin(BackgroundErrorReason /* reason */,
                                    Status /* bg_error */,
                                    bool* /* auto_recovery */);

  // A callback function for RocksDB which will be called once the recovery
  // attempt from a background retryable error is completed. The recovery
  // may have been successful or not. In either case, the callback is called
  // with the old and new error. If info.new_bg_error is Status::OK(), that
  // means the recovery succeeded.
  virtual void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& /*info*/);
 
 private:
    void* _pointer;
};
}  // namespace rocksdb

#pragma GCC diagnostic pop