package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"
import (
	"io"
	"reflect"
	"syscall"
	"unsafe"
)

type SequentialFile interface {
	Read(p []byte) (int, error)
	Skip(n int64) error
	InvalidateCache(offset int64, length int) error
	UseDirectIo() bool
	Release()
}

type AccessPattern int

const (
	NORMAL AccessPattern = iota
	RANDOM
	SEQUENTIAL
	WILLNEED
	DONTNEED
)

type RandomAccessFile interface {
	GetUniqueId(p []byte) int
	Read(offset int64, p []byte) (int, error)
	Hint(pattern AccessPattern)
	InvalidateCache(offset int64, length int) error
	UseDirectIo() bool
	Release()
}

type WritableFile interface {
	GetUniqueId(p []byte) int
	Append(data []byte) error
	PositionedAppend(data []byte, offset int64) error
	Truncate(size int64) error
	Flush() error
	Sync() error
	IsSyncThreadSafe() bool
	UseDirectIo() bool
	GetFileSize() int64
	RangeSync(offset int64, nbytes int64) error
	InvalidateCache(offset int64, length int64) error
	Allocate(offset int64, len int64) error
	Close() error
	Release()
}

type Logger interface {
	Append([]byte)
	Release()
}

type Directory interface {
	// Fsync directory. Can be called concurrently from multiple threads.
	Fsync() error
	Release()
}

type FileLock interface {
	Release()
}

type CustomEnv interface {
	Name() string
	NewSequentialFile(fname string, options *EnvOptions) (SequentialFile, error)
	NewRandomAccessFile(fname string, options *EnvOptions) (RandomAccessFile, error)
	NewWritableFile(fname string, options *EnvOptions) (WritableFile, error)
	NewLogger(fname string) (Logger, error)
	ReuseWritableFile(fname, old_fname string, options *EnvOptions) (WritableFile, error)
	NewDirectory(name string) (Directory, error)
	FileExists(fname string) error
	GetChildren(dir string) ([][]byte, error)
	DeleteFile(fname string) error
	CreateDir(dirname string) error
	CreateDirIfMissing(dirname string) error
	DeleteDir(dirname string) error
	GetFileSize(fname string) (int64, error)
	GetFileModificationTime(fname string) (uint64, error)
	RenameFile(src, target string) error
	LinkFile(src, target string) error
	LockFile(fname string) (FileLock, error)
	UnlockFile(lock FileLock) error
	GetAbsolutePath(dbPath string, p []byte) (int, error)
	GenerateUniqueId(p []byte) int
	Release()
}

// Hold references to compaction filters.
var (
	envs  = NewCOWListEx()
	files = NewCOWListEx()
)

// Env is a system call environment used by a database.
type Env struct {
	c *C.rocksdb_env_t
}

// NewDefaultEnv creates a default environment.
func NewDefaultEnv() *Env {
	return NewNativeEnv(C.rocksdb_create_default_env())
}

// NewMemEnv creates MemEnv for in-memory testing.
func NewMemEnv() *Env {
	return NewNativeEnv(C.rocksdb_create_mem_env())
}

func NewCustomEnv(env CustomEnv) *Env {
	idx := envs.Append(env)
	return NewNativeEnv(C.rocksdb_create_custom_env(C.uintptr_t(idx)))
}

// NewNativeEnv creates a Environment object.
func NewNativeEnv(c *C.rocksdb_env_t) *Env {
	return &Env{c}
}

// SetBackgroundThreads sets the number of background worker threads
// of a specific thread pool for this environment.
// 'LOW' is the default pool.
// Default: 1
func (env *Env) SetBackgroundThreads(n int) {
	C.rocksdb_env_set_background_threads(env.c, C.int(n))
}

// SetHighPriorityBackgroundThreads sets the size of the high priority
// thread pool that can be used to prevent compactions from stalling
// memtable flushes.
func (env *Env) SetHighPriorityBackgroundThreads(n int) {
	C.rocksdb_env_set_high_priority_background_threads(env.c, C.int(n))
}

// Destroy deallocates the Env object.
func (env *Env) Destroy() {
	C.rocksdb_env_destroy(env.c)
	env.c = nil
}

// custom env

//export gorocksdb_env_NewSequentialFile
func gorocksdb_env_NewSequentialFile(idx int, fname *C.char, result *int, options *C.rocksdb_envoptions_t) C.int {
	f, err := envs.Get(idx).(CustomEnv).NewSequentialFile(C.GoString(fname), &EnvOptions{options})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	fid := files.Append(f)
	*result = fid
	return 0
}

//export gorocksdb_env_NewRandomAccessFile
func gorocksdb_env_NewRandomAccessFile(idx int, fname *C.char, result *int, options *C.rocksdb_envoptions_t) C.int {
	f, err := envs.Get(idx).(CustomEnv).NewRandomAccessFile(C.GoString(fname), &EnvOptions{options})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	fid := files.Append(f)
	*result = fid
	return 0
}

//export gorocksdb_env_NewWritableFile
func gorocksdb_env_NewWritableFile(idx int, fname *C.char, result *int, options *C.rocksdb_envoptions_t) C.int {
	f, err := envs.Get(idx).(CustomEnv).NewWritableFile(C.GoString(fname), &EnvOptions{options})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	fid := files.Append(f)
	*result = fid
	return 0
}

//export gorocksdb_env_NewLogger
func gorocksdb_env_NewLogger(idx int, fname *C.char, result *int) C.int {
	f, err := envs.Get(idx).(CustomEnv).NewLogger(C.GoString(fname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	fid := files.Append(f)
	*result = fid
	return 0
}

//export gorocksdb_env_ReuseWritableFile
func gorocksdb_env_ReuseWritableFile(idx int, fname *C.char, old_fname *C.char, result *int, options *C.rocksdb_envoptions_t) C.int {
	f, err := envs.Get(idx).(CustomEnv).ReuseWritableFile(C.GoString(fname), C.GoString(old_fname), &EnvOptions{options})
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	fid := files.Append(f)
	*result = fid
	return 0
}

//export gorocksdb_env_NewDirectory
func gorocksdb_env_NewDirectory(idx int, name *C.char, result *int) C.int {
	f, err := envs.Get(idx).(CustomEnv).NewDirectory(C.GoString(name))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	fid := files.Append(f)
	*result = fid
	return 0
}

//export gorocksdb_env_FileExists
func gorocksdb_env_FileExists(idx int, fname *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).FileExists(C.GoString(fname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_GetChildren
func gorocksdb_env_GetChildren(idx int, dir *C.char, num *C.int, names ***C.char, sizes **C.size_t) C.int {
	ns, err := envs.Get(idx).(CustomEnv).GetChildren(C.GoString(dir))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	ch, si := byteSlicesToCSlices(ns)
	*num = C.int(len(ns))
	*names = ch.c()
	*sizes = si.c()
	return 0
}

//export gorocksdb_env_DeleteFile
func gorocksdb_env_DeleteFile(idx int, fname *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).DeleteFile(C.GoString(fname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_CreateDir
func gorocksdb_env_CreateDir(idx int, dirname *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).CreateDir(C.GoString(dirname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_CreateDirIfMissing
func gorocksdb_env_CreateDirIfMissing(idx int, dirname *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).CreateDirIfMissing(C.GoString(dirname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_DeleteDir
func gorocksdb_env_DeleteDir(idx int, dirname *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).DeleteDir(C.GoString(dirname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_GetFileSize
func gorocksdb_env_GetFileSize(idx int, fname *C.char, file_size *C.uint64_t) C.int {
	s, err := envs.Get(idx).(CustomEnv).GetFileSize(C.GoString(fname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	*file_size = C.uint64_t(s)
	return 0
}

//export gorocksdb_env_GetFileModificationTime
func gorocksdb_env_GetFileModificationTime(idx int, fname *C.char, file_mtime *C.uint64_t) C.int {
	s, err := envs.Get(idx).(CustomEnv).GetFileSize(C.GoString(fname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	*file_mtime = C.uint64_t(s)
	return 0
}

//export gorocksdb_env_RenameFile
func gorocksdb_env_RenameFile(idx int, src *C.char, target *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).RenameFile(C.GoString(src), C.GoString(target))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_LinkFile
func gorocksdb_env_LinkFile(idx int, src *C.char, target *C.char) C.int {
	err := envs.Get(idx).(CustomEnv).LinkFile(C.GoString(src), C.GoString(target))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_LockFile
func gorocksdb_env_LockFile(idx int, fname *C.char, lock *int) C.int {
	l, err := envs.Get(idx).(CustomEnv).LockFile(C.GoString(fname))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	*lock = files.Append(l)
	return 0
}

//export gorocksdb_env_UnlockFile
func gorocksdb_env_UnlockFile(idx int, lock int) C.int {
	l := files.Get(lock).(FileLock)
	err := envs.Get(idx).(CustomEnv).UnlockFile(l)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_env_GetAbsolutePath
func gorocksdb_env_GetAbsolutePath(idx int, db_path *C.char, outputPath *C.char, size *C.size_t) C.int {
	buf := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(outputPath)), Len: int(*size), Cap: int(*size)}
	n, err := envs.Get(idx).(CustomEnv).GetAbsolutePath(C.GoString(db_path), *(*[]byte)(unsafe.Pointer(&buf)))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	*size = C.size_t(n)
	return 0
}

//export gorocksdb_env_GenerateUniqueId
func gorocksdb_env_GenerateUniqueId(idx int, output_path *C.char, maxsize C.size_t) C.size_t {
	buf := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(output_path)), Len: int(maxsize), Cap: int(maxsize)}
	n := envs.Get(idx).(CustomEnv).GenerateUniqueId(*(*[]byte)(unsafe.Pointer(&buf)))
	return C.size_t(n)
}

//export gorocksdb_env_Release
func gorocksdb_env_Release(idx int) {
	envs.Get(idx).(CustomEnv).Release()
	envs.Delete(idx)
}

//export  gorocksdb_SequentialFile_Read
func gorocksdb_SequentialFile_Read(idx int, n C.uint64_t, scratch *C.char, size *C.uint64_t) C.int {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(scratch)), Len: int(n), Cap: int(n)}
	r, err := files.Get(idx).(SequentialFile).Read(*(*[]byte)(unsafe.Pointer(&bh)))
	if err != nil {
		if err == io.EOF {
			*size = 0
			return 0
		}
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	*size = C.uint64_t(r)
	return 0
}

//export gorocksdb_SequentialFile_Skip
func gorocksdb_SequentialFile_Skip(idx int, n C.uint64_t) C.int {
	err := files.Get(idx).(SequentialFile).Skip(int64(n))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_SequentialFile_InvalidateCache
func gorocksdb_SequentialFile_InvalidateCache(idx int, offset C.uint64_t, length C.uint64_t) C.int {
	err := files.Get(idx).(SequentialFile).InvalidateCache(int64(offset), int(length))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_SequentialFile_use_direct_io
func gorocksdb_SequentialFile_use_direct_io(idx int) bool {
	return files.Get(idx).(SequentialFile).UseDirectIo()
}

//export gorocksdb_SequentialFile_Release
func gorocksdb_SequentialFile_Release(idx int) {
	files.Get(idx).(SequentialFile).Release()
	files.Delete(idx)
}

//export gorocksdb_RandomAccessFile_Read
func gorocksdb_RandomAccessFile_Read(idx int, offset C.uint64_t, n C.uint64_t, scratch *C.char, size *C.uint64_t) C.int {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(scratch)), Len: int(n), Cap: int(n)}
	r, err := files.Get(idx).(RandomAccessFile).Read(int64(offset), *(*[]byte)(unsafe.Pointer(&bh)))
	if err != nil {
		if err == io.EOF {
			*size = 0
			return 0
		}
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	*size = C.uint64_t(r)
	return 0
}

//export gorocksdb_RandomAccessFile_GetUniqueId
func gorocksdb_RandomAccessFile_GetUniqueId(idx int, id *C.char, max_size C.uint64_t) C.uint64_t {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(id)), Len: int(max_size), Cap: int(max_size)}
	r := files.Get(idx).(RandomAccessFile).GetUniqueId(*(*[]byte)(unsafe.Pointer(&bh)))
	return C.uint64_t(r)
}

//export gorocksdb_RandomAccessFile_Hint
func gorocksdb_RandomAccessFile_Hint(idx int, pattern C.int) {
	files.Get(idx).(RandomAccessFile).Hint(AccessPattern(pattern))
}

//export gorocksdb_RandomAccessFile_InvalidateCache
func gorocksdb_RandomAccessFile_InvalidateCache(idx int, offset C.uint64_t, length C.uint64_t) C.int {
	err := files.Get(idx).(RandomAccessFile).InvalidateCache(int64(offset), int(length))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_RandomAccessFile_use_direct_io
func gorocksdb_RandomAccessFile_use_direct_io(idx int) bool {
	return files.Get(idx).(RandomAccessFile).UseDirectIo()
}

//export gorocksdb_RandomAccessFile_Release
func gorocksdb_RandomAccessFile_Release(idx int) {
	files.Get(idx).(RandomAccessFile).Release()
	files.Delete(idx)
}

//export gorocksdb_WritableFile_Append
func gorocksdb_WritableFile_Append(idx int, data *C.char, n C.uint64_t) C.int {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(data)), Len: int(n), Cap: int(n)}
	err := files.Get(idx).(WritableFile).Append(*(*[]byte)(unsafe.Pointer(&bh)))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_PositionedAppend
func gorocksdb_WritableFile_PositionedAppend(idx int, data *C.char, n C.uint64_t, offset C.uint64_t) C.int {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(data)), Len: int(n), Cap: int(n)}
	err := files.Get(idx).(WritableFile).PositionedAppend(*(*[]byte)(unsafe.Pointer(&bh)), int64(offset))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_Truncate
func gorocksdb_WritableFile_Truncate(idx int, size C.uint64_t) C.int {
	err := files.Get(idx).(WritableFile).Truncate(int64(size))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_Close
func gorocksdb_WritableFile_Close(idx int) C.int {
	err := files.Get(idx).(WritableFile).Close()
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_Flush
func gorocksdb_WritableFile_Flush(idx int) C.int {
	err := files.Get(idx).(WritableFile).Flush()
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_Sync
func gorocksdb_WritableFile_Sync(idx int) C.int {
	err := files.Get(idx).(WritableFile).Sync()
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_IsSyncThreadSafe
func gorocksdb_WritableFile_IsSyncThreadSafe(idx int) bool {
	return files.Get(idx).(WritableFile).IsSyncThreadSafe()
}

//export gorocksdb_WritableFile_use_direct_io
func gorocksdb_WritableFile_use_direct_io(idx int) bool {
	return files.Get(idx).(WritableFile).UseDirectIo()
}

//export gorocksdb_WritableFile_GetFileSize
func gorocksdb_WritableFile_GetFileSize(idx int) C.uint64_t {
	size := files.Get(idx).(WritableFile).GetFileSize()
	return C.uint64_t(size)
}

//export gorocksdb_WritableFile_GetUniqueId
func gorocksdb_WritableFile_GetUniqueId(idx int, id *C.char, max_size C.uint64_t) C.uint64_t {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(id)), Len: int(max_size), Cap: int(max_size)}
	r := files.Get(idx).(WritableFile).GetUniqueId(*(*[]byte)(unsafe.Pointer(&bh)))
	return C.uint64_t(r)
}

//export gorocksdb_WritableFile_InvalidateCache
func gorocksdb_WritableFile_InvalidateCache(idx int, offset C.uint64_t, length C.uint64_t) C.int {
	err := files.Get(idx).(WritableFile).InvalidateCache(int64(offset), int64(length))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_RangeSync
func gorocksdb_WritableFile_RangeSync(idx int, offset C.uint64_t, nbytes C.uint64_t) C.int {
	err := files.Get(idx).(WritableFile).RangeSync(int64(offset), int64(nbytes))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_Allocate
func gorocksdb_WritableFile_Allocate(idx int, offset C.uint64_t, len C.uint64_t) C.int {
	err := files.Get(idx).(WritableFile).Allocate(int64(offset), int64(len))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_WritableFile_Release
func gorocksdb_WritableFile_Release(idx int) {
	files.Get(idx).(WritableFile).Release()
	files.Delete(idx)
}

//export gorocksdb_Directory_Fsync
func gorocksdb_Directory_Fsync(idx int) C.int {
	err := files.Get(idx).(Directory).Fsync()
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return -C.int(errno)
		}
		return -C.int(syscall.EIO)
	}
	return 0
}

//export gorocksdb_Directory_Release
func gorocksdb_Directory_Release(idx int) {
	files.Get(idx).(Directory).Release()
	files.Delete(idx)
}

//export gorocksdb_Logger_Write
func gorocksdb_Logger_Write(idx int, data *C.char, size C.int) {
	bh := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(data)), Len: int(size), Cap: int(size)}
	files.Get(idx).(Logger).Append(*(*[]byte)(unsafe.Pointer(&bh)))
}

//export gorocksdb_Logger_Release
func gorocksdb_Logger_Release(idx int) {
	files.Get(idx).(Logger).Release()
	files.Delete(idx)
}

//export gorocksdb_FileLock_Release
func gorocksdb_FileLock_Release(idx int) {
	files.Get(idx).(FileLock).Release()
	files.Delete(idx)
}
