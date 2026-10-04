package protocol

import "kamaRPC/internal/codec"

// CodecType 编解码器类型
type CodecType byte

const (
	CodecTypeJSON CodecType = iota + 1  // 1: JSON 序列化
	CodecTypeProto  // 2: Protobuf 序列化
)

type Header struct {
	RequestID   uint64 // 全局唯一的请求 ID
	ServiceName string // 目标服务/接口名称
	MethodName  string // 具体调用的方法名
	Error       string // 专门用于 RPC Response（响应头） 中传递服务端执行过程中的错误描述
	CodecType   CodecType // 指出 Body（消息体）所使用的序列化协议（JSON、Protobuf)
	Compression codec.CompressionType // 指出 Body 数据是否经过压缩，以及使用了哪种压缩算法
}
