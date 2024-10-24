package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

type (
	Code int
	SubCode int
	Severity int
	BackgroundErrorReason int
)

const (
	kFlush BackgroundErrorReason = iota
	kCompaction
	kWriteCallback 
	kMemTable 
	kManifestWrite
	kFlushNoWAL
	kManifestWriteNoWAL
)

const (
    Ok Code = 0
    NotFound Code = 1
    Corruption Code = 2
    NotSupported Code = 3
    InvalidArgument Code = 4
    IOError Code = 5
    MergeInProgress Code = 6
    Incomplete Code = 7
    ShutdownInProgress Code = 8
    TimedOut Code = 9
    Aborted Code = 10
    Busy Code = 11
    Expired Code = 12
    TryAgain Code = 13
    CompactionTooLarge Code = 14
    ColumnFamilyDropped Code = 15
    MaxCode Code = 16
)

const (
    None SubCode = 0
    MutexTimeout SubCode = 1
    LockTimeout SubCode = 2
    LockLimit SubCode = 3
    NoSpace SubCode = 4
    Deadlock SubCode = 5
    StaleFile SubCode = 6
    MemoryLimit SubCode = 7
    SpaceLimit SubCode = 8
    PathNotFound SubCode = 9
    MergeOperandsInsufficientCapacity SubCode = 10
    ManualCompactionPaused SubCode = 11
    Overwritten SubCode = 12
    TxnNotPrepared SubCode = 13
    IOFenced SubCode = 14
    MaxSubCode SubCode =15
)

const (
    NoError Severity = 0
    SoftError Severity = 1
    HardError Severity = 2
    FatalError Severity = 3
    UnrecoverableError Severity = 4
    MaxSeverity Severity = 5
)

type Status struct {
	c *C.rocksdb_status_t
}

func (status Status) Code() Code {
	c := C.rocksdb_status_code(status.c)
	return Code(c)
}

func (status Status) Subcode() SubCode {
	c := C.rocksdb_status_subcode(status.c)
	return SubCode(c)
}

func (status Status) Severity() Severity {
	c := C.rocksdb_status_severity(status.c)
	return Severity(c)
}

func (status Status) GetState() string {
	c := C.rocksdb_status_getState(status.c)
	return C.GoString(c)
}

type EnventListener struct {
	c *C.rocksdb_listener_t
}

type Listener interface {
	Release()
}

type (
	BackgroundErrorListener interface {
		OnBackgroundError(reason BackgroundErrorReason, bg_error *Status)
		Release()
	}

 	ErrorRecoverListener interface {
		OnErrorRecoveryBegin(reason BackgroundErrorReason, bg_error Status, auto_recovery *bool)
		OnErrorRecoveryEnd(old_error, new_error *Status)
		Release()
	}
)

var (
	listeners  = NewCOWListEx()
)

func NewBackgroundErrorListener(listener BackgroundErrorListener) *EnventListener {
	idx := listeners.Append(listener)
	return &EnventListener{C.rocksdb_create_background_error_listener(C.uintptr_t(idx))}
}

func NewErrorRecoverListener(listener ErrorRecoverListener) *EnventListener {
	idx := listeners.Append(listener)
	return &EnventListener{C.rocksdb_create_error_recover_listener(C.uintptr_t(idx))}
}

//export gorocksdb_listener_OnBackgroundError
func gorocksdb_listener_OnBackgroundError(idx int,  reason C.int, bg_error *C.rocksdb_status_t) {
	listeners.Get(idx).(BackgroundErrorListener).OnBackgroundError(BackgroundErrorReason(reason), &Status{bg_error})
}

//export gorocksdb_listener_OnErrorRecoveryBegin
func gorocksdb_listener_OnErrorRecoveryBegin(idx int, reason C.int, bg_error *C.rocksdb_status_t, auto_recovery *C._Bool) {
	r := true
	listeners.Get(idx).(ErrorRecoverListener).OnErrorRecoveryBegin(BackgroundErrorReason(reason), Status{bg_error}, &r)
	*auto_recovery = C._Bool(r)
}

//export gorocksdb_listener_OnErrorRecoveryEnd
func gorocksdb_listener_OnErrorRecoveryEnd(idx int, old_error *C.rocksdb_status_t, new_error *C.rocksdb_status_t) {
	listeners.Get(idx).(ErrorRecoverListener).OnErrorRecoveryEnd(&Status{old_error}, &Status{new_error})
}

//export gorocksdb_listener_Release
func gorocksdb_listener_Release(idx int) {
	listeners.Get(idx).(Listener).Release()
	listeners.Delete(idx)
}