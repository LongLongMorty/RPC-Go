package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// 起一个只 accept、不回应也不主动关闭的 TCP 服务，供连接池测试使用
func startHoldListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn
		}
	}()
	return ln
}

func poolSize(p *ConnectionPool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

func TestPoolReusesConnection(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 0, 1)
	defer p.Close()

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatalf("应复用同一条连接: %p != %p", c1, c2)
	}
	if n := poolSize(p); n != 1 {
		t.Fatalf("pool size = %d, want 1", n)
	}
}

func TestPoolReconnectsAfterDead(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 0, 1)
	defer p.Close()

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c1.Close() // 标记为已关闭

	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c2 == c1 {
		t.Fatal("连接已死，应重建新连接")
	}
	if !c1.isClosed() {
		t.Fatal("旧连接应处于关闭状态")
	}
}

// 活连接夹在死连接中间时，修复后的实现不应漏检/误建
func TestPoolReuseSkipsDeadWithoutExtraDial(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 0, 1)
	defer p.Close()

	live, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// 白盒：在池里放一条“死”连接，制造 [live, dead] 且 next 指向 dead
	dead, err := newTCPClient(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	dead.Close()

	p.mu.Lock()
	p.conns = append(p.conns, &connEntry{client: dead, lastUsed: time.Now(), createdAt: time.Now()})
	p.next = 1
	p.mu.Unlock()

	got, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != live {
		t.Fatalf("应复用活连接 %p，实际 %p", live, got)
	}
	if n := poolSize(p); n != 1 {
		t.Fatalf("死连接未被清理: pool size = %d", n)
	}
}

func TestPoolIdleReap(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 0, 1, WithIdleTimeout(50*time.Millisecond))
	defer p.Close()

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(80 * time.Millisecond)

	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c2 == c1 {
		t.Fatal("空闲超时后应重建连接")
	}
	if n := poolSize(p); n != 1 {
		t.Fatalf("pool size = %d, want 1", n)
	}
}

func TestPoolMaxIdleTrims(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 1, 5) // maxIdle=1, maxActive=5
	defer p.Close()

	// 白盒塞入 3 条连接，最后一条 lastUsed 最新
	p.mu.Lock()
	now := time.Now()
	for i := 0; i < 3; i++ {
		c, err := newTCPClient(ln.Addr().String())
		if err != nil {
			p.mu.Unlock()
			t.Fatal(err)
		}
		p.conns = append(p.conns, &connEntry{
			client:    c,
			lastUsed:  now.Add(time.Duration(i) * time.Second),
			createdAt: now,
		})
	}
	p.mu.Unlock()

	if _, err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := poolSize(p); n > 1 {
		t.Fatalf("maxIdle 未生效: pool size = %d", n)
	}
}

func TestPoolClosed(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 0, 1)
	p.Close()

	if _, err := p.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("err = %v, want %v", err, ErrPoolClosed)
	}
}

func TestPoolConcurrentAcquire(t *testing.T) {
	ln := startHoldListener(t)
	p := NewConnectionPool(ln.Addr().String(), 2, 8)
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.Acquire(context.Background())
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if c.isClosed() {
				t.Errorf("返回了已关闭的连接")
			}
		}()
	}
	wg.Wait()

	// 触发一次 reap，把并发期间临时膨胀的连接压回 maxIdle
	if _, err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := poolSize(p); n > 2 {
		t.Fatalf("pool size = %d, 超过 maxIdle=2", n)
	}
}
