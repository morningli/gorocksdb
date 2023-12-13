package gorocksdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestCOWListEx(t *testing.T) {
	cl := NewCOWListEx()
	cl.Append("hello")
	cl.Append("world")
	cl.Append("!")
	ensure.DeepEqual(t, cl.Get(0), "hello")
	ensure.DeepEqual(t, cl.Get(1), "world")
	ensure.DeepEqual(t, cl.Get(2), "!")
	ensure.DeepEqual(t, cl.Len(), 3)

	cl.Delete(0)
	ensure.DeepEqual(t, cl.Get(0), "world")
	ensure.DeepEqual(t, cl.Get(1), "!")
	ensure.DeepEqual(t, cl.Len(), 2)

	cl.Append("hello")

	cl.Delete(1)
	ensure.DeepEqual(t, cl.Get(3), "hello")
	ensure.DeepEqual(t, cl.Get(1), "!")
	ensure.DeepEqual(t, cl.Len(), 2)

	cl.Append("world")

	cl.Delete(2)
	ensure.DeepEqual(t, cl.Get(3), "hello")
	ensure.DeepEqual(t, cl.Get(4), "world")
	ensure.DeepEqual(t, cl.Len(), 2)

	cl.Append("!")

	cl.Delete(3)
	cl.Delete(4)
	cl.Delete(5)
	ensure.DeepEqual(t, cl.Len(), 0)
}

func TestCOWListExMT(t *testing.T) {
	cl := NewCOWListEx()
	expectedRes := make([]int, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			index := cl.Append(v)
			expectedRes[index] = v
		}(i)
	}
	wg.Wait()
	for i, v := range expectedRes {
		ensure.DeepEqual(t, cl.Get(i), v)
	}
}

func BenchmarkCOWListEx_Get(b *testing.B) {
	cl := NewCOWListEx()
	for i := 0; i < 10; i++ {
		cl.Append(fmt.Sprintf("helloworld%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cl.Get(i % 10).(string)
	}
}
