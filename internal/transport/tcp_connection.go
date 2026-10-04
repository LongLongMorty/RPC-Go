package transport

import (
	"bufio"
	"encoding/binary"
	"io"
	"kamaRPC/internal/protocol"
	"net"
	"sync"
)

const BufferSize = 4096

type TCPConnection struct {
	conn   net.Conn
	reader *bufio.Reader

	writeMu sync.Mutex
}

// 创建连接
func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, BufferSize),
	}
}

// Read 按「固定包头 → header → body」精确读取一条完整消息。
// 用 io.ReadFull 交给 bufio.Reader 处理 TCP 的粘包/半包，
// 并对包头声明的长度做上限校验，避免超长包导致 OOM。
func (tc *TCPConnection) Read() (*protocol.Message, error) {

	var prefix [protocol.HeaderSize]byte
	if _, err := io.ReadFull(tc.reader, prefix[:]); err != nil {
		return nil, err
	}

	if binary.BigEndian.Uint16(prefix[0:2]) != protocol.Magic {
		return nil, protocol.ErrInvalidMagic
	}

	headerLen := protocol.DecodeHeaderLen(prefix[2:6])
	bodyLen := protocol.DecodeBodyLen(prefix[6:10])

	if err := protocol.ValidateLengths(headerLen, bodyLen); err != nil {
		return nil, err
	}

	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(tc.reader, headerBytes); err != nil {
		return nil, err
	}

	bodyBytes := make([]byte, bodyLen)
	if _, err := io.ReadFull(tc.reader, bodyBytes); err != nil {
		return nil, err
	}

	return protocol.DecodeParts(headerBytes, bodyBytes)
}

func (tc *TCPConnection) Write(msg *protocol.Message) error {
	data, err := protocol.Encode(msg)
	if err != nil {
		return err
	}

	tc.writeMu.Lock()
	defer tc.writeMu.Unlock()

	total := 0
	for total < len(data) {
		n, err := tc.conn.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}

	return nil
}

// 关闭连接
func (tc *TCPConnection) Close() error {
	if tcp, ok := tc.conn.(*net.TCPConn); ok {
		tcp.SetLinger(0)
	}
	return tc.conn.Close()
}

func (tc *TCPConnection) RemoteAddr() string {
	return tc.conn.RemoteAddr().String()
}
