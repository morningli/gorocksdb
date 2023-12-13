package gorocksdb

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// COWList implements a copy-on-write list. It is intended to be used by go
// callback registry for CGO, which is read-heavy with occasional writes.
// Reads do not block; Writes do not block reads (or vice versa), but only
// one write can occur at once;
type COWListEx struct {
	idx int64
	v   sync.Map
}

// NewCOWList creates a new COWList.
func NewCOWListEx() *COWListEx {
	return &COWListEx{}
}

// Append appends an item to the COWList and returns the index for that item.
func (c *COWListEx) Append(i interface{}) int {
	idx := atomic.AddInt64(&c.idx, 1)
	c.v.Store(idx, i)
	return int(idx)
}

func (c *COWListEx) Delete(idx int) {
	c.v.Delete(int64(idx))
}

// Get gets the item at index.
func (c *COWListEx) Get(index int) interface{} {
	item, ok := c.v.Load(int64(index))
	if !ok {
		panic(fmt.Sprintf("COWListEx get %d out of range", index))
	}
	return item
}

func (c *COWListEx) Len() int {
	total := 0
	c.v.Range(func(key, value interface{}) bool {
		total++
		return true
	})
	return total
}
