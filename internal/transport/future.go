package transport

import (
	"context"
	"kamaRPC/internal/codec"
	"sync"
	"time"
)

type Future struct {
	done  chan struct{}
	res   []byte
	err   error
	mu    sync.Mutex
	codec codec.Codec

	onComplete []func(error)
	completed  bool
}

func NewFuture() *Future {
	c, _ := codec.New(codec.JSON)
	return &Future{
		done:  make(chan struct{}),
		codec: c,
	}
}

// Done 只会生效一次，重复调用是安全的（避免 close 已关闭的 channel 而 panic）
func (f *Future) Done(res []byte, err error) {
	f.mu.Lock()
	if f.completed {
		f.mu.Unlock()
		return
	}
	f.completed = true
	f.res = res
	f.err = err
	callbacks := f.onComplete
	f.onComplete = nil
	f.mu.Unlock()

	for _, cb := range callbacks {
		cb(err)
	}

	close(f.done)
}

func (f *Future) Wait() ([]byte, error) {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.res, f.err
}

// OnComplete 注册完成回调，可注册多个；若已完成则立即回调。
func (f *Future) OnComplete(fn func(error)) {
	if fn == nil {
		return
	}

	f.mu.Lock()
	if f.completed {
		err := f.err
		f.mu.Unlock()
		fn(err)
		return
	}
	f.onComplete = append(f.onComplete, fn)
	f.mu.Unlock()
}

func (f *Future) WaitWithContext(ctx context.Context) ([]byte, error) {
	select {
	case <-f.done:
		return f.Wait()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *Future) DoneChan() <-chan struct{} {
	return f.done
}

func (f *Future) GetResult(reply interface{}) error {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	return f.codec.Unmarshal(f.res, reply)
}

func (f *Future) GetResultWithContext(ctx context.Context, reply interface{}) error {
	select {
	case <-f.done:
		return f.GetResult(reply)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Future) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

func (f *Future) WaitWithTimeout(timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.WaitWithContext(ctx)
}
