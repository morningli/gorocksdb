package gorocksdb

// #include "rocksdb/c.h"
// #include "gorocksdb.h"
import "C"

var (
	listeners  = NewCOWListEx()
)

type Status struct {
	c *C.rocksdb_status_t
}

type Listener struct {
	c *C.rocksdb_listener_t
}


type BackgroundErrorListener interface {
	OnBackgroundError(reason int, bg_error *Status)
	Release()
}

func NewBackgroundErrorListener(listener BackgroundErrorListener) *Listener {
	idx := listeners.Append(listener)
	return &Listener{C.rocksdb_create_background_error_listener(C.uintptr_t(idx))}
}

//export gorocksdb_listener_OnBackgroundError
func gorocksdb_listener_OnBackgroundError(idx int,  reason C.int, bg_error *C.rocksdb_status_t) {
	listeners.Get(idx).(BackgroundErrorListener).OnBackgroundError(int(reason), &Status{bg_error})
}

//export gorocksdb_listener_Release
func gorocksdb_listener_Release(idx int) {
	listeners.Get(idx).(BackgroundErrorListener).Release()
	listeners.Delete(idx)
}