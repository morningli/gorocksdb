// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include "custom_env.h"

#include <sys/time.h>
#include <time.h>

#include <cstdlib>
#include <mutex>
#include <utility>

namespace rocksdb {
/**
 * @brief convert error code to status
 * @details Convert internal linux error code to Status
 *
 * @param r [description]
 * @return [description]
 */
Status err_to_status(int r) {
    switch (r) {
        case 0:
            return Status::OK();
        case -ENOENT:
            return Status::NotFound(strerror(-ENOENT));
        case -ENODATA:
            return Status::NotFound(strerror(-ENODATA));
        case -ENOTDIR:
            return Status::NotFound(strerror(-ENOTDIR));
        case -EINVAL:
            return Status::InvalidArgument(Status::kNone);
        case -EIO:
            return Status::IOError(Status::kNone);
        case -EEXIST:
            return Status::InvalidArgument("File exists");
        case -EACCES:
            return Status::NotSupported(strerror(-EACCES));
        case -EISDIR:
            return Status::InvalidArgument(strerror(-EISDIR));
        case -EMFILE:
            return Status::Busy(strerror(-ENOTDIR));
        case -ENOSPC:
            return Status::NoSpace();
        // TODO: case -EBADFD:
        default:
            // FIXME :(
            std::cerr << "unsupported code:" << r << std::endl;
            assert(0 == "unrecognized error code");
            return Status::NotSupported(Status::kNone);
    }
}

class SequentialFile;
// A file abstraction for reading sequentially through a file
class CustomSequentialFile : public SequentialFile {
    void* _pointer;

   public:
    CustomSequentialFile(void* pointer) : _pointer(pointer) {}

    ~CustomSequentialFile() override {
        ((void (*)(void*))(gorocksdb_SequentialFile_Release))(_pointer);
    }

    Status Read(size_t n, Slice* result, char* scratch) override {
        uint64_t readn = 0;
        int ret = ((int (*)(void*, uint64_t, char*, uint64_t*))(
            gorocksdb_SequentialFile_Read))(_pointer, n, scratch, &readn);
        if (ret < 0) {
            return err_to_status(ret);
        }
        *result = Slice(scratch, readn);
        return Status::OK();
    }

    Status Skip(uint64_t n) override {
        int ret = ((int (*)(void*, uint64_t))(gorocksdb_SequentialFile_Skip))(
            _pointer, n);
        return err_to_status(ret);
    }

    Status InvalidateCache(size_t offset, size_t length) {
        int ret = ((int (*)(void*, uint64_t, uint64_t))(
            gorocksdb_SequentialFile_InvalidateCache))(_pointer, offset, length);
        return err_to_status(ret);
    }
};

// A file abstraction for randomly reading the contents of a file.
class CustomRandomAccessFile : public RandomAccessFile {
    void* _pointer;

   public:
    CustomRandomAccessFile(void* pointer) : _pointer(pointer) {}

    virtual ~CustomRandomAccessFile() {
        ((void (*)(void*))(gorocksdb_RandomAccessFile_Release))(_pointer);
    }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
        uint64_t readn = 0;
        int ret = ((int (*)(void*, uint64_t, uint64_t, char*, uint64_t*))(
            gorocksdb_RandomAccessFile_Read))(_pointer, offset, n, scratch, &readn);
        if (ret < 0) {
            return err_to_status(ret);
        }
        *result = Slice(scratch, readn);
        return Status::OK();
    }

    size_t GetUniqueId(char* id, size_t max_size) const {
        return ((int (*)(void*, char*, uint64_t))(
            gorocksdb_RandomAccessFile_GetUniqueId))(_pointer, id, max_size);
    };

    // enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };
    void Hint(AccessPattern pattern) {
        ((int (*)(void*, AccessPattern))(gorocksdb_RandomAccessFile_Hint))(_pointer,
                                                                           pattern);
    }

    Status InvalidateCache(size_t offset, size_t length) {
        int ret = ((int (*)(void*, uint64_t, uint64_t))(
            gorocksdb_RandomAccessFile_InvalidateCache))(_pointer, offset, length);
        return err_to_status(ret);
    }
};

class WritableFile;
// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class CustomWritableFile : public WritableFile {
    void* _pointer;

   public:
    CustomWritableFile(void* pointer, const EnvOptions& options)
        : WritableFile(options), _pointer(pointer) {}

    virtual ~CustomWritableFile() { ((void (*)(void*))(gorocksdb_WritableFile_Release))(_pointer); }

    Status Append(const Slice& data) {
        int ret =
            ((int (*)(void*, const char*, uint64_t))(gorocksdb_WritableFile_Append))(
                _pointer, data.data(), data.size());
        return err_to_status(ret);
    }

    Status PositionedAppend(const Slice& data, uint64_t offset) {
        int ret = ((int (*)(void*, const char*, uint64_t, uint64_t))(
            gorocksdb_WritableFile_Append))(_pointer, data.data(), data.size(),
                                            offset);
        return err_to_status(ret);
    }

    Status Truncate(uint64_t size) {
        int ret = ((int (*)(void*, uint64_t))(gorocksdb_WritableFile_Truncate))(
            _pointer, size);
        return err_to_status(ret);
    }

    Status Close() {
        int ret = ((int (*)(void*))(gorocksdb_WritableFile_Close))(_pointer);
        return err_to_status(ret);
    }

    Status Flush() {
        int ret = ((int (*)(void*))(gorocksdb_WritableFile_Flush))(_pointer);
        return err_to_status(ret);
    }

    Status Sync() {  // sync data
        int ret = ((int (*)(void*))(gorocksdb_WritableFile_Sync))(_pointer);
        return err_to_status(ret);
    }

    bool IsSyncThreadSafe() const {
        return ((int (*)(void*))(gorocksdb_WritableFile_IsSyncThreadSafe))(
            _pointer);
    }

    bool use_direct_io() const {
        return ((int (*)(void*))(gorocksdb_WritableFile_use_direct_io))(_pointer);
    }

    uint64_t GetFileSize() {
        return ((int (*)(void*))(gorocksdb_WritableFile_GetFileSize))(_pointer);
    }

    size_t GetUniqueId(char* id, size_t max_size) const {
        return ((int (*)(void*, char*, uint64_t))(gorocksdb_WritableFile_GetUniqueId))(
            _pointer, id, max_size);
    };

    Status InvalidateCache(size_t offset, size_t length) {
        int ret = ((int (*)(void*, uint64_t, uint64_t))(
            gorocksdb_WritableFile_InvalidateCache))(_pointer, offset, length);
        return err_to_status(ret);
    }

    using WritableFile::RangeSync;
    Status RangeSync(uint64_t offset, uint64_t nbytes) {
        int ret = ((int (*)(void*, uint64_t, uint64_t))(
            gorocksdb_WritableFile_RangeSync))(_pointer, offset, nbytes);
        return err_to_status(ret);
    }

   protected:
    using WritableFile::Allocate;
    Status Allocate(uint64_t offset, uint64_t len) {
        int ret = ((int (*)(void*, uint64_t, uint64_t))(
            gorocksdb_WritableFile_Allocate))(_pointer, offset, len);
        return err_to_status(ret);
    }
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class CustomDirectory : public Directory {
    void* _pointer;

   public:
    explicit CustomDirectory(void* pointer) : _pointer(pointer) {}
    virtual ~CustomDirectory() { ((void (*)(void*))(gorocksdb_Directory_Release))(_pointer); }
    // Fsync directory. Can be called concurrently from multiple threads.
    virtual Status Fsync() {
        int ret = ((int (*)(void*))(gorocksdb_Directory_Fsync))(_pointer);
        return err_to_status(ret);
    }
};

class CustomLogger : public Logger {
   private:
    void* _pointer;

   public:
    CustomLogger(void* pointer) : _pointer(pointer) {}

    ~CustomLogger() override { ((void (*)(void*))(gorocksdb_Logger_Release))(_pointer); }

    using Logger::Logv;
    void Logv(const char* format, va_list ap) override {
        const uint64_t thread_id = (uint64_t)pthread_self();

        // We try twice: the first time with a fixed-size stack allocated buffer,
        // and the second time with a much larger dynamically allocated buffer.
        char buffer[500];
        for (int iter = 0; iter < 2; iter++) {
            char* base;
            int bufsize;
            if (iter == 0) {
                bufsize = sizeof(buffer);
                base = buffer;
            } else {
                bufsize = 30000;
                base = new char[bufsize];
            }
            char* p = base;
            char* limit = base + bufsize;

            struct timeval now_tv;
            gettimeofday(&now_tv, nullptr);
            const time_t seconds = now_tv.tv_sec;
            struct tm t;
            localtime_r(&seconds, &t);
            p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                          t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                          t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                          static_cast<long long unsigned int>(thread_id));

            // Print the message
            if (p < limit) {
                va_list backup_ap;
                va_copy(backup_ap, ap);
                p += vsnprintf(p, limit - p, format, backup_ap);
                va_end(backup_ap);
            }

            // Truncate to available space if necessary
            if (p >= limit) {
                if (iter == 0) {
                    continue;  // Try again with larger buffer
                } else {
                    p = limit - 1;
                }
            }

            // Add newline if necessary
            if (p == base || p[-1] != '\n') {
                *p++ = '\n';
            }

            assert(p <= limit);
            ((void (*)(void*, char*, int))(gorocksdb_Logger_Write))(_pointer, base, p - base);
            if (base != buffer) {
                delete[] base;
            }
            break;
        }
    }
};

class CustomFileLock : public FileLock {
    void* _pointer;

   public:
    CustomFileLock(void* pointer) : _pointer(pointer) {}

    ~CustomFileLock() { ((void (*)(void*))(gorocksdb_FileLock_Release))(_pointer); }

    void* GetPointer() { return _pointer; }
};

// --------------------
// --- CustomEnv ----
// --------------------

CustomEnv::CustomEnv(void* pointer)
    : EnvWrapper(Env::Default()), _pointer(pointer) {}

CustomEnv::~CustomEnv() { ((void (*)(void*))(gorocksdb_env_Release))(_pointer); }

std::string CustomEnv::GenerateUniqueId() {
    char result[256];
    size_t size = 256;
    size_t n  = ((size_t (*)(void*,char*,size_t))(gorocksdb_env_GenerateUniqueId))(_pointer, result, size);
    return std::string(result, n);
}

Status CustomEnv::NewSequentialFile(const std::string& fname,
                                    std::unique_ptr<SequentialFile>* result,
                                    const EnvOptions& options) {
    void* fid;
    int ret = ((int (*)(void*, const char*, void**, const EnvOptions*))(
        gorocksdb_env_NewSequentialFile))(_pointer, fname.c_str(), &fid,
                                          &options);
    if (ret < 0) {
        result->reset();
        return err_to_status(ret);
    }
    result->reset(new CustomSequentialFile(fid));
    return Status::OK();
}

class RandomAccessFile;
/**
 * @brief create a new random access file handler
 * @details it will check the existence of fname
 *
 * @param fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status CustomEnv::NewRandomAccessFile(const std::string& fname,
                                      std::unique_ptr<RandomAccessFile>* result,
                                      const EnvOptions& options) {
    void* fid;
    int ret = ((int (*)(void*, const char*, void**, const EnvOptions*))(
        gorocksdb_env_NewRandomAccessFile))(_pointer, fname.c_str(), &fid,
                                            &options);
    if (ret < 0) {
        result->reset();
        return err_to_status(ret);
    }
    result->reset(new CustomRandomAccessFile(fid));
    return Status::OK();
}

/**
 * @brief create a new write file handler
 * @details it will check the existence of fname
 *
 * @param fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status CustomEnv::NewWritableFile(const std::string& fname,
                                  std::unique_ptr<WritableFile>* result,
                                  const EnvOptions& options) {
    void* fid;
    int ret = ((int (*)(void*, const char*, void**, const EnvOptions*))(
        gorocksdb_env_NewWritableFile))(_pointer, fname.c_str(), &fid, &options);
    if (ret < 0) {
        result->reset();
        return err_to_status(ret);
    }
    result->reset(new CustomWritableFile(fid, options));
    return Status::OK();
}

/**
 * @brief reuse write file handler
 * @details
 *  This function will rename old_fname to new_fname,
 *  then return the handler of new_fname
 *
 * @param new_fname [description]
 * @param old_fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status CustomEnv::ReuseWritableFile(const std::string& new_fname,
                                    const std::string& old_fname,
                                    std::unique_ptr<WritableFile>* result,
                                    const EnvOptions& options) {
    void* fid;
    int ret = ((int (*)(void*, const char*, const char*, void**,
                        const EnvOptions*))(gorocksdb_env_ReuseWritableFile))(
        _pointer, new_fname.c_str(), old_fname.c_str(), &fid, &options);
    if (ret < 0) {
        result->reset();
        return err_to_status(ret);
    }
    result->reset(new CustomWritableFile(fid, options));
    return Status::OK();
}

Status CustomEnv::NewLogger(const std::string& fname,
                            std::shared_ptr<Logger>* result) {
    void* fid;
    int ret = ((int (*)(void*, const char*, void**))(gorocksdb_env_NewLogger))(
        _pointer, fname.c_str(), &fid);
    if (ret < 0) {
        result->reset();
        return err_to_status(ret);
    }
    result->reset(new CustomLogger(fid));
    return Status::OK();
}

/**
 * @brief create a new directory handler
 * @details [long description]
 *
 * @param name [description]
 * @param result [description]
 *
 * @return [description]
 */
Status CustomEnv::NewDirectory(const std::string& name,
                               std::unique_ptr<Directory>* result) {
    void* fid;
    int ret = ((int (*)(void*, const char*, void**))(gorocksdb_env_NewDirectory))(
        _pointer, name.c_str(), &fid);
    if (ret < 0) {
        result->reset();
        return err_to_status(ret);
    }
    result->reset(new CustomDirectory(fid));
    return Status::OK();
}

/**
 * @brief check if fname is exist
 * @details [long description]
 *
 * @param fname [description]
 * @return [description]
 */
Status CustomEnv::FileExists(const std::string& fname) {
    int ret = ((int (*)(void*, const char*))(gorocksdb_env_FileExists))(
        _pointer, fname.c_str());
    return err_to_status(ret);
}

/**
 * @brief get subfile name of dir_in
 * @details [long description]
 *
 * @param dir_in [description]
 * @param result [description]
 *
 * @return [description]
 */
Status CustomEnv::GetChildren(const std::string& dir_in,
                              std::vector<std::string>* result) {
    int num = 0;
    char** ns = nullptr;
    size_t* ss = nullptr;
    result->clear();
    int ret = ((int (*)(void*, const char*, int*, char***, size_t**))(
        gorocksdb_env_GetChildren))(_pointer, dir_in.c_str(), &num, &ns, &ss);
    if (ret < 0) {
        return err_to_status(ret);
    }
    for (int i = 0; i < num; i++) {
        result->push_back(std::string(ns[i], ss[i]));
    }
    return Status::OK();
}

/**
 * @brief delete fname
 * @details [long description]
 *
 * @param fname [description]
 * @return [description]
 */
Status CustomEnv::DeleteFile(const std::string& fname) {
    int ret = ((int (*)(void*, const char*))(gorocksdb_env_DeleteFile))(
        _pointer, fname.c_str());
    return err_to_status(ret);
}

/**
 * @brief create new dir
 * @details [long description]
 *
 * @param dirname [description]
 * @return [description]
 */
Status CustomEnv::CreateDir(const std::string& dirname) {
    int ret = ((int (*)(void*, const char*))(gorocksdb_env_CreateDir))(
        _pointer, dirname.c_str());
    return err_to_status(ret);
}

/**
 * @brief create dir if missing
 * @details [long description]
 *
 * @param dirname [description]
 * @return [description]
 */
Status CustomEnv::CreateDirIfMissing(const std::string& dirname) {
    int ret = ((int (*)(void*, const char*))(gorocksdb_env_CreateDirIfMissing))(
        _pointer, dirname.c_str());
    return err_to_status(ret);
}

/**
 * @brief delete dir
 * @details
 *
 * @param dirname [description]
 * @return [description]
 */
Status CustomEnv::DeleteDir(const std::string& dirname) {
    int ret = ((int (*)(void*, const char*))(gorocksdb_env_DeleteDir))(
        _pointer, dirname.c_str());
    return err_to_status(ret);
}

/**
 * @brief return file size
 * @details [long description]
 *
 * @param fname [description]
 * @param file_size [description]
 *
 * @return [description]
 */
Status CustomEnv::GetFileSize(const std::string& fname, uint64_t* file_size) {
    int ret =
        ((int (*)(void*, const char*, uint64_t*))(gorocksdb_env_GetFileSize))(
            _pointer, fname.c_str(), file_size);
    if (ret < 0) {
        *file_size = 0;
        return err_to_status(ret);
    }
    return Status::OK();
}

/**
 * @brief get file modification time
 * @details [long description]
 *
 * @param fname [description]
 * @param file_mtime [description]
 *
 * @return [description]
 */
Status CustomEnv::GetFileModificationTime(const std::string& fname,
                                          uint64_t* file_mtime) {
    int ret = ((int (*)(void*, const char*, uint64_t*))(
        gorocksdb_env_GetFileModificationTime))(_pointer, fname.c_str(),
                                                file_mtime);
    if (ret < 0) {
        *file_mtime = 0;
        return err_to_status(ret);
    }
    return Status::OK();
}

/**
 * @brief rename file
 * @details
 *
 * @param src [description]
 * @param target_in [description]
 *
 * @return [description]
 */
Status CustomEnv::RenameFile(const std::string& src,
                             const std::string& target_in) {
    int ret =
        ((int (*)(void*, const char*, const char*))(gorocksdb_env_RenameFile))(
            _pointer, src.c_str(), target_in.c_str());
    return err_to_status(ret);
}

/**
 * @brief not support
 * @details [long description]
 *
 * @param src [description]
 * @param target_in [description]
 *
 * @return [description]
 */
Status CustomEnv::LinkFile(const std::string& src,
                           const std::string& target_in) {
    int ret =
        ((int (*)(void*, const char*, const char*))(gorocksdb_env_LinkFile))(
            _pointer, src.c_str(), target_in.c_str());
    return err_to_status(ret);
}

/**
 * @brief lock file. create if missing.
 * @details [long description]
 *
 * It seems that LockFile is used for preventing other instance of RocksDB
 * from opening up the database at the same time. From RocksDB source code,
 * the invokes of LockFile are at following locations:
 *
 *  ./db/db_impl.cc:1159:    s = env_->LockFile(LockFileName(dbname_),
 * &db_lock_);    // DBImpl::Recover
 *  ./db/db_impl.cc:5839:  Status result = env->LockFile(lockname, &lock); //
 * Status DestroyDB
 *
 * When db recovery and db destroy, RocksDB will call LockFile
 *
 * @param fname [description]
 * @param lock [description]
 *
 * @return [description]
 */
Status CustomEnv::LockFile(const std::string& fname, FileLock** lock) {
    void* fd;
    int ret = ((int (*)(void*, const char*, void**))(gorocksdb_env_LockFile))(
        _pointer, fname.c_str(), &fd);
    if (ret < 0) {
        *lock = nullptr;
        return err_to_status(ret);
    }
    *lock = new CustomFileLock(fd);
    return Status::OK();
}

/**
 * @brief unlock file
 * @details [long description]
 *
 * @param lock [description]
 * @return [description]
 */
Status CustomEnv::UnlockFile(FileLock* lock) {
    CustomFileLock* tmp = dynamic_cast<CustomFileLock*>(lock);
    int ret = ((int (*)(void*, void*))(gorocksdb_env_UnlockFile))(
        _pointer, tmp->GetPointer());
    return err_to_status(ret);
}

/**
 * @brief not support
 * @details [long description]
 *
 * @param db_path [description]
 * @param output_path [description]
 *
 * @return [description]
 */
Status CustomEnv::GetAbsolutePath(const std::string& db_path,
                                  std::string* output_path) {
    char result[1024];
    size_t n = 1024;
    int ret = ((int (*)(void*, const char*, char*, size_t*))(gorocksdb_env_GetAbsolutePath))(
            _pointer, db_path.c_str(), result, &n);
    if (ret < 0) {
        return err_to_status(ret);
    }
    *output_path = std::string(result,n);
    return Status::OK();
}

}  // namespace rocksdb

#ifdef __cplusplus
extern "C" {
#endif

struct rocksdb_env_t_tmp {
    rocksdb::Env* rep;
    bool is_default;
};

rocksdb_env_t* rocksdb_create_custom_env(uintptr_t idx) {
    rocksdb_env_t_tmp* result = new rocksdb_env_t_tmp;
    result->rep = new rocksdb::CustomEnv((void*)idx);
    result->is_default = false;
    return (rocksdb_env_t*)result;
}

#ifdef __cplusplus
} /* end extern "C" */
#endif

#pragma GCC diagnostic pop