package server

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
)

type testReq struct {
	A int
	B int
}

type testResp struct {
	Result int
}

type blockingService struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingService) Do(in *testReq, out *testResp) error {
	b.started <- struct{}{}
	<-b.release
	out.Result = in.A + in.B
	return nil
}

// 在同一条连接上先发的请求若阻塞，后发的请求也必须能被读取并处理
func TestHandleProcessesConcurrently(t *testing.T) {
	s, err := NewServer(":0", WithServerCodec(codec.JSON), WithWorkerPool(8, 8))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.pool.stop() })

	svc := &blockingService{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	s.Register("Svc", svc)

	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() {
		clientEnd.Close()
		serverEnd.Close()
	})

	go s.Handle(transport.NewTCPConnection(serverEnd))

	tcClient := transport.NewTCPConnection(clientEnd)
	body, err := json.Marshal(&testReq{A: 1, B: 2})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 0; i < 2; i++ {
			tcClient.Write(&protocol.Message{
				Header: &protocol.Header{
					ServiceName: "Svc",
					MethodName:  "Do",
					Compression: codec.CompressionGzip,
				},
				Body: body,
			})
		}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-svc.started:
		case <-time.After(2 * time.Second):
			t.Fatal("两个请求未并发处理（被串行阻塞）")
		}
	}

	close(svc.release)
}
