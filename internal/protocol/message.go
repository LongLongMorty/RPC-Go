package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"kamaRPC/internal/codec"
)

const Magic uint16 = 0x1234

// 包头固定长度：Magic(2) + headerLen(4) + bodyLen(4)
const HeaderSize = 10

// 单条消息的最大长度上限，防止恶意超长包打爆内存
const (
	MaxHeaderLen = 64 * 1024        // 64 KB
	MaxBodyLen   = 16 * 1024 * 1024 // 16 MB
)

var (
	ErrInvalidMagic   = errors.New("protocol: invalid magic number")
	ErrHeaderTooLarge = errors.New("protocol: header length exceeds limit")
	ErrBodyTooLarge   = errors.New("protocol: body length exceeds limit")
	ErrIncomplete     = errors.New("protocol: incomplete packet")
)

type Message struct {
	Header *Header
	Body   []byte
}

func Encode(msg *Message) ([]byte, error) {

	if msg.Header == nil {
		return nil, fmt.Errorf("header is nil")
	}

	bodyBytes := msg.Body

	if msg.Header.Compression != codec.CompressionNone {
		var err error
		bodyBytes, err = codec.Compress(bodyBytes, msg.Header.Compression)
		if err != nil {
			return nil, err
		}
	}

	headerCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}

	headerBytes, err := headerCodec.Marshal(msg.Header)
	if err != nil {
		return nil, err
	}

	headerLen := uint32(len(headerBytes))
	bodyLen := uint32(len(bodyBytes))

	total := 2 + 4 + 4 + headerLen + bodyLen
	buf := make([]byte, total)

	binary.BigEndian.PutUint16(buf[0:2], Magic)

	binary.BigEndian.PutUint32(buf[2:6], headerLen)

	binary.BigEndian.PutUint32(buf[6:10], bodyLen)

	copy(buf[10:], headerBytes)

	copy(buf[10+headerLen:], bodyBytes)

	return buf, nil
}

// DecodeHeaderLen 从字节切片解析 headerLen
func DecodeHeaderLen(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// DecodeBodyLen 从字节切片解析 bodyLen
func DecodeBodyLen(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// ValidateLengths 校验包头声明的长度是否在安全范围内（防 OOM）
func ValidateLengths(headerLen, bodyLen uint32) error {
	if headerLen > MaxHeaderLen {
		return ErrHeaderTooLarge
	}
	if bodyLen > MaxBodyLen {
		return ErrBodyTooLarge
	}
	return nil
}

// Decode 从完整字节数组解码 Message（用于缓冲区/测试场景）
func Decode(data []byte) (*Message, error) {

	if len(data) < HeaderSize {
		return nil, ErrIncomplete
	}

	if binary.BigEndian.Uint16(data[0:2]) != Magic {
		return nil, ErrInvalidMagic
	}

	headerLen := binary.BigEndian.Uint32(data[2:6])
	bodyLen := binary.BigEndian.Uint32(data[6:10])

	if err := ValidateLengths(headerLen, bodyLen); err != nil {
		return nil, err
	}

	total := uint64(HeaderSize) + uint64(headerLen) + uint64(bodyLen)
	if uint64(len(data)) < total {
		return nil, ErrIncomplete
	}

	bodyStart := HeaderSize + int(headerLen)
	return DecodeParts(data[HeaderSize:bodyStart], data[bodyStart:bodyStart+int(bodyLen)])
}

// DecodeParts 从已按长度切好的 header / body 字节解码 Message
func DecodeParts(headerBytes, bodyBytes []byte) (*Message, error) {

	headerCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}

	var header Header
	if err := headerCodec.Unmarshal(headerBytes, &header); err != nil {
		return nil, err
	}

	if header.Compression != codec.CompressionNone {
		bodyBytes, err = codec.Decompress(bodyBytes, header.Compression)
		if err != nil {
			return nil, err
		}
	}

	return &Message{
		Header: &header,
		Body:   bodyBytes,
	}, nil
}
