package server

import "kamaRPC/internal/codec"

type HandleOption func(*Handler) error

func WithHandlerCodec(t codec.Type) HandleOption {
	return func(c *Handler) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		return nil
	}
}

type ServerOption func(*Server) error

func WithServerCodec(t codec.Type) ServerOption {
	return func(c *Server) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		return nil
	}
}

// WithWorkerPool 配置业务处理的协程池大小与队列长度
func WithWorkerPool(size, queue int) ServerOption {
	return func(s *Server) error {
		if size > 0 {
			s.poolSize = size
		}
		if queue >= 0 {
			s.poolQueue = queue
		}
		return nil
	}
}
