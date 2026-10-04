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

	connMu  sync.Mutex
	conns   map[*transport.TCPConnection]struct{}
	closing chan struct{}
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
		addr:     addr,
		services: make(map[string]interface{}),
		limiter:  limiter.NewTokenBucket(10000),
		handler:  mustNewHandler(),
		conns:    make(map[*transport.TCPConnection]struct{}),
		closing:  make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) Register(name string, service interface{}) {
	s.services[name] = service
}

// 单连接单协程串行模型,请求层面可以像http1.1一样复用一个连接
// 但是现在是响应层面, 他只会顺序执行第一个请求,执行完之后才执行完第二个请求
// todo:后续需要以流的形式去优化
func (s *Server) Handle(conn *transport.TCPConnection) {
	defer conn.Close()
	log.Println("测试一次")
	for {
		// 读取请求
		msg, err := conn.Read()
		if err != nil {
			// 连接被关闭或出错，退出
			return
		}

		// 限流检查
		if !s.limiter.Allow() {
			resp := &protocol.Message{
				Header: &protocol.Header{
					RequestID:   msg.Header.RequestID,
					Error:       "rate limit exceeded",
					Compression: codec.CompressionGzip,
				},
			}
			conn.Write(resp)
			continue
		}
		// 处理请求
		s.handler.Process(conn, msg, s.services[msg.Header.ServiceName])
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

		go func() {
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

	log.Println("server shutdown complete")
}
