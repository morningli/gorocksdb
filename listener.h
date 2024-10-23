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

#include "_cgo_export.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/env_mirror.h"
#include "rocksdb/listener.h"

namespace rocksdb {
class BackgroundErrorListener : public EventListener {
 public:
  BackgroundErrorListener(void* pointer): _pointer(pointer) {}
  virtual void OnBackgroundError(BackgroundErrorReason /* reason */,Status* /* bg_error */);
 private:
    void* _pointer;
};
}  // namespace rocksdb

#pragma GCC diagnostic pop