package transport

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
)

func newPipeConn(t *testing.T) (*TCPConnection, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return NewTCPConnection(server), client
}

func sampleMsg(id uint64, body string) *protocol.Message {
	return &protocol.Message{
		Header: &protocol.Header{
			RequestID:   id,
			ServiceName: "Arith",
			MethodName:  "Add",
			Compression: codec.CompressionGzip,
		},
		Body: []byte(body),
	}
}

// 两个包一次性发出（粘包），应能分别正确读出
func TestReadStickyPackets(t *testing.T) {
	tc, peer := newPipeConn(t)

	msgs := []*protocol.Message{
		sampleMsg(1, `{"A":1,"B":2}`),
		sampleMsg(2, `{"A":3,"B":4}`),
	}

	var wire []byte
	for _, m := range msgs {
		b, err := protocol.Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, b...)
	}

	go peer.Write(wire)

	for i, want := range msgs {
		got, err := tc.Read()
		if err != nil {
			t.Fatalf("msg %d read error: %v", i, err)
		}
		if got.Header.RequestID != want.Header.RequestID {
			t.Fatalf("msg %d id = %d, want %d", i, got.Header.RequestID, want.Header.RequestID)
		}
		if string(got.Body) != string(want.Body) {
			t.Fatalf("msg %d body = %s, want %s", i, got.Body, want.Body)
		}
	}
}

// 一个字节一个字节地发（半包），io.ReadFull 应能拼回完整消息
func TestReadSplitPackets(t *testing.T) {
	tc, peer := newPipeConn(t)

	want := sampleMsg(7, `{"A":5,"B":6}`)
	wire, err := protocol.Encode(want)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for _, b := range wire {
			if _, err := peer.Write([]byte{b}); err != nil {
				return
			}
		}
	}()

	got, err := tc.Read()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if got.Header.RequestID != want.Header.RequestID {
		t.Fatalf("id = %d, want %d", got.Header.RequestID, want.Header.RequestID)
	}
	if string(got.Body) != string(want.Body) {
		t.Fatalf("body = %s, want %s", got.Body, want.Body)
	}
}

func TestReadRejectsOversizedBody(t *testing.T) {
	tc, peer := newPipeConn(t)

	prefix := make([]byte, protocol.HeaderSize)
	binary.BigEndian.PutUint16(prefix[0:2], protocol.Magic)
	binary.BigEndian.PutUint32(prefix[2:6], 0)
	binary.BigEndian.PutUint32(prefix[6:10], protocol.MaxBodyLen+1)

	go peer.Write(prefix)

	if _, err := tc.Read(); !errors.Is(err, protocol.ErrBodyTooLarge) {
		t.Fatalf("err = %v, want %v", err, protocol.ErrBodyTooLarge)
	}
}

func TestReadRejectsOversizedHeader(t *testing.T) {
	tc, peer := newPipeConn(t)

	prefix := make([]byte, protocol.HeaderSize)
	binary.BigEndian.PutUint16(prefix[0:2], protocol.Magic)
	binary.BigEndian.PutUint32(prefix[2:6], protocol.MaxHeaderLen+1)
	binary.BigEndian.PutUint32(prefix[6:10], 0)

	go peer.Write(prefix)

	if _, err := tc.Read(); !errors.Is(err, protocol.ErrHeaderTooLarge) {
		t.Fatalf("err = %v, want %v", err, protocol.ErrHeaderTooLarge)
	}
}

func TestReadRejectsBadMagic(t *testing.T) {
	tc, peer := newPipeConn(t)

	prefix := make([]byte, protocol.HeaderSize)
	binary.BigEndian.PutUint16(prefix[0:2], 0xDEAD)
	binary.BigEndian.PutUint32(prefix[2:6], 0)
	binary.BigEndian.PutUint32(prefix[6:10], 0)

	go peer.Write(prefix)

	if _, err := tc.Read(); !errors.Is(err, protocol.ErrInvalidMagic) {
		t.Fatalf("err = %v, want %v", err, protocol.ErrInvalidMagic)
	}
}
