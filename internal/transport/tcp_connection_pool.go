package transport

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrPoolClosed    = errors.New("connection pool closed")
	ErrPoolExhausted = errors.New("connection pool exhausted")
)

// PoolOption 用于配置连接池的生命周期策略
type PoolOption func(*ConnectionPool)

// WithIdleTimeout 设置空闲回收时间：连接空闲超过该时长将被关闭；<=0 表示不回收
func WithIdleTimeout(d time.Duration) PoolOption {
	return func(p *ConnectionPool) { p.idleTimeout = d }
}

// WithMaxLifetime 设置连接最长存活时间：超过则强制重建；<=0 表示不限制
func WithMaxLifetime(d time.Duration) PoolOption {
	return func(p *ConnectionPool) { p.maxLifetime = d }
}

type connEntry struct {
	client    *TCPClient
	lastUsed  time.Time
	createdAt time.Time
}

// ConnectionPool 维护到某个地址的一组长连接，按轮询复用。
// 连接是多路复用的（靠 RequestID 区分请求），因此没有借出/归还的概念：
// Acquire 只是从池里取一条可用连接，用完后连接自动留在池中。
type ConnectionPool struct {
	addr string

	maxIdle   int // 空闲保留上限；<=0 表示不额外限制
	maxActive int // 总连接数上限；<=0 表示不限制

	idleTimeout time.Duration // 空闲回收时间；<=0 不回收
	maxLifetime time.Duration // 连接最长存活；<=0 不限制

	mu     sync.Mutex
	conns  []*connEntry
	next   int
	closed bool
}

func NewConnectionPool(addr string, maxIdle, maxActive int, opts ...PoolOption) *ConnectionPool {
	p := &ConnectionPool{
		addr:      addr,
		maxIdle:   maxIdle,
		maxActive: maxActive,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *ConnectionPool) Acquire(ctx context.Context) (*TCPClient, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	now := time.Now()
	p.reapLocked(now)

	// 优先复用：轮询找一条可用连接
	if client, ok := p.reuseLocked(now); ok {
		p.mu.Unlock()
		return client, nil
	}

	// 没有可用连接且已达上限
	if p.maxActive > 0 && len(p.conns) >= p.maxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}

	p.mu.Unlock()

	// 锁外 dial：避免长时间阻塞其它 Acquire 与 Close
	client, err := dialTCPClient(ctx, p.addr)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		client.Close()
		return nil, ErrPoolClosed
	}

	// 并发下可能已被其它 goroutine 填满：关闭多余连接，退回复用
	if p.maxActive > 0 && len(p.conns) >= p.maxActive {
		client.Close()
		if c, ok := p.reuseLocked(time.Now()); ok {
			return c, nil
		}
		return nil, ErrPoolExhausted
	}

	now = time.Now()
	p.conns = append(p.conns, &connEntry{
		client:    client,
		lastUsed:  now,
		createdAt: now,
	})
	return client, nil
}

// reuseLocked 轮询取一条可用连接；调用方需持有 p.mu。
func (p *ConnectionPool) reuseLocked(now time.Time) (*TCPClient, bool) {
	n := len(p.conns)
	for i := 0; i < n; i++ {
		idx := (p.next + i) % n
		e := p.conns[idx]
		if p.usable(e, now) {
			e.lastUsed = now
			p.next = (idx + 1) % n
			return e.client, true
		}
	}
	return nil, false
}

// usable 判断连接当前是否可复用；调用方需持有 p.mu。
func (p *ConnectionPool) usable(e *connEntry, now time.Time) bool {
	if e.client.isClosed() {
		return false
	}
	if p.maxLifetime > 0 && now.Sub(e.createdAt) > p.maxLifetime {
		return false
	}
	if p.idleTimeout > 0 && now.Sub(e.lastUsed) > p.idleTimeout {
		return false
	}
	return true
}

// reapLocked 清理不可用连接（已关闭 / 超过最长存活 / 空闲超时），
// 并在超过 maxIdle 时按最近使用时间保留最近的若干条；调用方需持有 p.mu。
func (p *ConnectionPool) reapLocked(now time.Time) {
	if len(p.conns) == 0 {
		return
	}

	kept := p.conns[:0]
	for _, e := range p.conns {
		if p.usable(e, now) {
			kept = append(kept, e)
		} else {
			e.client.Close()
		}
	}
	p.conns = kept

	if p.maxIdle > 0 && len(p.conns) > p.maxIdle {
		sort.SliceStable(p.conns, func(i, j int) bool {
			return p.conns[i].lastUsed.After(p.conns[j].lastUsed)
		})
		for _, e := range p.conns[p.maxIdle:] {
			e.client.Close()
		}
		p.conns = p.conns[:p.maxIdle]
	}

	switch {
	case len(p.conns) == 0:
		p.next = 0
	case p.next >= len(p.conns):
		p.next %= len(p.conns)
	}
}

func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true

	for _, e := range p.conns {
		e.client.Close()
	}
	p.conns = nil
}
