package transport

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
)

func pendingCount(c *TCPClient) int {
	n := 0
	c.pending.Range(func(_, _ interface{}) bool {
		n++
		return true
	})
	return n
}

// 服务端收到请求但不回应，请求应在 timeout 后失败并清理 pending
func TestSendAsyncTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	c, err := newTCPClient(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	srvConn := <-accepted
	defer srvConn.Close()

	msg := &protocol.Message{
		Header: &protocol.Header{ServiceName: "Arith", MethodName: "Add", Compression: codec.CompressionGzip},
		Body:   []byte(`{}`),
	}

	future, err := c.SendAsync(msg, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if _, err := future.WaitWithTimeout(2 * time.Second); !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("err = %v, want %v", err, ErrRequestTimeout)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("超时耗时过长: %v", elapsed)
	}

	if n := pendingCount(c); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

func TestFutureOnCompleteMultipleAndIdempotent(t *testing.T) {
	f := NewFuture()

	var mu sync.Mutex
	var got []error
	f.OnComplete(func(err error) {
		mu.Lock()
		got = append(got, err)
		mu.Unlock()
	})
	f.OnComplete(func(err error) {
		mu.Lock()
		got = append(got, err)
		mu.Unlock()
	})

	f.Done(nil, ErrRequestTimeout)
	f.Done(nil, ErrRequestTimeout) // 第二次应被忽略，且不 panic

	<-f.DoneChan()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("回调执行 %d 次, want 2", len(got))
	}
}

func TestFutureOnCompleteAfterDone(t *testing.T) {
	f := NewFuture()
	f.Done([]byte("ok"), nil)

	called := false
	f.OnComplete(func(error) { called = true })
	if !called {
		t.Fatal("已完成时注册回调应立即执行")
	}
}
