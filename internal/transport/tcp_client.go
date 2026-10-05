package transport

import (
	"context"
	"errors"
	"kamaRPC/internal/protocol"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRequestTimeout = errors.New("transport: request timeout")
	ErrConnClosed     = errors.New("connection closed")
)

type TCPClient struct {
	conn *TCPConnection
	addr string

	writeMu sync.Mutex
	seq     uint64

	pending sync.Map // map[uint64]*Future

	closed int32
}

func newTCPClient(addr string) (*TCPClient, error) {
	return dialTCPClient(context.Background(), addr)
}

// dialTCPClient 使用带超时的 Dialer 并在锁外建立连接，
// 支持通过 ctx 取消；默认 5s 超时兜底。
func dialTCPClient(ctx context.Context, addr string) (*TCPClient, error) {
	var d net.Dialer
	d.Timeout = 5 * time.Second

	rawConn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	c := &TCPClient{
		conn: NewTCPConnection(rawConn),
		addr: addr,
	}

	go c.readLoop()
	return c, nil
}

func (c *TCPClient) nextSeq() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}

// SendAsync 发送一个请求；timeout > 0 时对该请求单独计时，
// 超时只让这一个 Future 失败并清理 pending，不影响连接上的其它请求。
func (c *TCPClient) SendAsync(msg *protocol.Message, timeout time.Duration) (*Future, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, ErrConnClosed
	}

	seq := c.nextSeq()
	msg.Header.RequestID = seq

	future := NewFuture()
	c.pending.Store(seq, future)

	if timeout > 0 {
		timer := time.AfterFunc(timeout, func() {
			if _, ok := c.pending.LoadAndDelete(seq); ok {
				future.Done(nil, ErrRequestTimeout)
			}
		})
		future.OnComplete(func(error) { timer.Stop() })
	}

	c.writeMu.Lock()
	err := c.conn.Write(msg)
	c.writeMu.Unlock()

	if err != nil {
		c.pending.Delete(seq)
		future.Done(nil, err)
		c.fail(err) // 关键：write 失败也要彻底杀死连接(解决之前连接bug)
		return nil, err
	}

	return future, nil
}

func (c *TCPClient) readLoop() {
	for {
		msg, err := c.conn.Read()
		if err != nil {
			c.fail(err)
			return
		}

		seq := msg.Header.RequestID

		val, ok := c.pending.LoadAndDelete(seq)
		if !ok {
			continue
		}

		future := val.(*Future)

		if msg.Header.Error != "" {
			future.Done(nil, errors.New(msg.Header.Error))
		} else {
			future.Done(msg.Body, nil)
		}
	}
}

func (c *TCPClient) fail(err error) {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	// 关闭底层连接
	// log.Println("底层连接被关闭")
	_ = c.conn.Close()

	// 失败所有 pending
	c.pending.Range(func(key, value interface{}) bool {
		future := value.(*Future)
		future.Done(nil, err)
		c.pending.Delete(key)
		return true
	})
}

func (c *TCPClient) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

func (c *TCPClient) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	return c.conn.Close()
}
