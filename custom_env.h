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

namespace rocksdb {
class CustomWritableFile;

class CustomEnv : public EnvWrapper {
   public:
    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores nullptr in *result and returns non-OK.  If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    Status NewSequentialFile(const std::string& fname,
                             std::unique_ptr<SequentialFile>* result,
                             const EnvOptions& options) override;

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    Status NewRandomAccessFile(const std::string& fname,
                               std::unique_ptr<RandomAccessFile>* result,
                               const EnvOptions& options) override;

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    Status NewWritableFile(const std::string& fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override;

    Status NewLogger(const std::string& fname,
                     std::shared_ptr<Logger>* result) override;

    // Reuse an existing file by renaming it and opening it as writable.
    Status ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             std::unique_ptr<WritableFile>* result,
                             const EnvOptions& options) override;

    // Create an object that represents a directory. Will fail if directory
    // doesn't exist. If the directory exists, it will open the directory
    // and create a new Directory object.
    //
    // On success, stores a pointer to the new Directory in
    // *result and returns OK. On failure stores nullptr in *result and
    // returns non-OK.
    Status NewDirectory(const std::string& name,
                        std::unique_ptr<Directory>* result) override;

    // Returns OK if the named file exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    Status FileExists(const std::string& fname) override;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    Status GetChildren(const std::string& dir, std::vector<std::string>* result);

    // Delete the named file.
    Status DeleteFile(const std::string& fname) override;

    // Create the specified directory. Returns error if directory exists.
    Status CreateDir(const std::string& dirname) override;

    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    Status CreateDirIfMissing(const std::string& dirname) override;

    // Delete the specified directory.
    Status DeleteDir(const std::string& dirname) override;

    // Store the size of fname in *file_size.
    Status GetFileSize(const std::string& fname, uint64_t* file_size) override;

    // Store the last modification time of fname in *file_mtime.
    Status GetFileModificationTime(const std::string& fname,
                                   uint64_t* file_mtime) override;
    // Rename file src to target.
    Status RenameFile(const std::string& src, const std::string& target) override;
    // Hard Link file src to target.
    Status LinkFile(const std::string& src, const std::string& target) override;

    // Lock the specified file.  Used to prevent concurrent access to
    // the same db by multiple processes.  On failure, stores nullptr in
    // *lock and returns non-OK.
    //
    // On success, stores a pointer to the object that represents the
    // acquired lock in *lock and returns OK.  The caller should call
    // UnlockFile(*lock) to release the lock.  If the process exits,
    // the lock will be automatically released.
    //
    // If somebody else already holds the lock, finishes immediately
    // with a failure.  I.e., this call does not wait for existing locks
    // to go away.
    //
    // May create the named file if it does not already exist.
    Status LockFile(const std::string& fname, FileLock** lock);

    // Release the lock acquired by a previous successful call to LockFile.
    // REQUIRES: lock was returned by a successful LockFile() call
    // REQUIRES: lock has not already been unlocked.
    Status UnlockFile(FileLock* lock);

    // Get full directory name for this db.
    Status GetAbsolutePath(const std::string& db_path, std::string* output_path);

    // Generate unique id
    std::string GenerateUniqueId();

    explicit CustomEnv(void* pointer);
    ~CustomEnv();

   private:
    void* _pointer;

    friend class CustomWritableFile;
};
}  // namespace rocksdb

#pragma GCC diagnostic pop