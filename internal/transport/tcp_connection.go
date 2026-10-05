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
	reader *bufio.Reader // 给 TCP 连接挂载了一个带 4KB 缓冲区的读取器

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
	// 精准读取 10 字节定长帧头
	var prefix [protocol.HeaderSize]byte
	if _, err := io.ReadFull(tc.reader, prefix[:]); err != nil {
		return nil, err
	}

	// 校验魔数 (Magic Number)
	if binary.BigEndian.Uint16(prefix[0:2]) != protocol.Magic {
		return nil, protocol.ErrInvalidMagic
	}
	// 解析 Header 长度和 Body 长度
	headerLen := protocol.DecodeHeaderLen(prefix[2:6])
	bodyLen := protocol.DecodeBodyLen(prefix[6:10])
	// 长度合法性校验（防攻击）
	if err := protocol.ValidateLengths(headerLen, bodyLen); err != nil {
		return nil, err
	}
	// 精确读取 HeaderBytes 与 BodyBytes
	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(tc.reader, headerBytes); err != nil {
		return nil, err
	}

	bodyBytes := make([]byte, bodyLen)
	if _, err := io.ReadFull(tc.reader, bodyBytes); err != nil {
		return nil, err
	}
	// 交给 protocol 包反序列化并还原成 protocol.Message
	return protocol.DecodeParts(headerBytes, bodyBytes)
}

func (tc *TCPConnection) Write(msg *protocol.Message) error {
	// 调用协议层的 Encode，将 Message 编码为符合协议的 []byte
	data, err := protocol.Encode(msg)
	if err != nil {
		return err
	}
	// Go 标准库 net.Conn.Write
	tc.writeMu.Lock()
	defer tc.writeMu.Unlock()

	_, err = tc.conn.Write(data)
	return err
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
