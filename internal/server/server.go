package server

import (
	"kamaRPC/internal/codec"
	"kamaRPC/internal/limiter"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
	"log"
	"net"
	"sync"
)

type Server struct {
	addr     string
	services map[string]interface{}
	limiter  *limiter.TokenBucket
	listener net.Listener
	handler  *Handler
	codec    codec.Codec

	pool      *workerPool
	poolSize  int
	poolQueue int

	connMu    sync.Mutex
	conns     map[*transport.TCPConnection]struct{}
	closing   chan struct{}
	handlerWG sync.WaitGroup
}

// 这边用了另外一种go规范去创建对象
func mustNewHandler() *Handler {
	h, err := NewHandler(nil, WithHandlerCodec(codec.JSON))
	if err != nil {
		panic(err)
	}
	return h
}

func NewServer(addr string, opts ...ServerOption) (*Server, error) {
	s := &Server{
		addr:      addr,
		services:  make(map[string]interface{}),
		limiter:   limiter.NewTokenBucket(10000),
		handler:   mustNewHandler(),
		conns:     make(map[*transport.TCPConnection]struct{}),
		closing:   make(chan struct{}),
		poolSize:  defaultPoolSize,
		poolQueue: defaultPoolQueue,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	s.pool = newWorkerPool(s.poolSize, s.poolQueue)
	return s, nil
}

func (s *Server) Register(name string, service interface{}) {
	s.services[name] = service
}

// 读与执行解耦：读循环只负责读取与限流，业务处理交给协程池异步执行，
// 处理完后再并发写回（TCPConnection.Write 内部有写锁，保证写安全）。
// 连接关闭前会等待本连接所有在途请求处理完毕。
func (s *Server) Handle(conn *transport.TCPConnection) {
	defer conn.Close()

	var inflight sync.WaitGroup
	defer inflight.Wait()

	for {
		// 读取请求
		msg, err := conn.Read()
		if err != nil {
			// 连接被关闭或出错，退出
			return
		}

		// 限流检查
		if !s.limiter.Allow() {
			conn.Write(&protocol.Message{
				Header: &protocol.Header{
					RequestID:   msg.Header.RequestID,
					Error:       "rate limit exceeded",
					Compression: codec.CompressionGzip,
				},
			})
			continue
		}

		service := s.services[msg.Header.ServiceName]

		// 扔进协程池异步处理，读循环立即继续读下一个请求
		inflight.Add(1)
		if !s.pool.submit(func() {
			defer inflight.Done()
			s.handler.Process(conn, msg, service)
		}) {
			inflight.Done()
			conn.Write(&protocol.Message{
				Header: &protocol.Header{
					RequestID:   msg.Header.RequestID,
					Error:       "server shutting down",
					Compression: codec.CompressionGzip,
				},
			})
			return
		}
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closing:
				return nil
			default:
				continue
			}
		}

		tcpConn := transport.NewTCPConnection(conn)

		s.connMu.Lock()
		s.conns[tcpConn] = struct{}{}
		s.connMu.Unlock()

		s.handlerWG.Add(1)
		go func() {
			defer s.handlerWG.Done()
			s.Handle(tcpConn)
			s.connMu.Lock()
			delete(s.conns, tcpConn)
			s.connMu.Unlock()
		}()
	}

}

func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) Shutdown() {
	close(s.closing)

	if s.listener != nil {
		s.listener.Close()
	}

	s.connMu.Lock()
	conns := make([]*transport.TCPConnection, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.connMu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}

	// 等待所有连接的在途请求处理完，再优雅停止协程池
	s.handlerWG.Wait()
	s.pool.stop()

	log.Println("server shutdown complete")
}
