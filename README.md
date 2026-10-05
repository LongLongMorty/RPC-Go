# RPC-GO

一个使用 Go 从零实现的 RPC 框架，用于学习 RPC 的核心原理与工程实践。

## 特性

- 自定义二进制协议（Magic + 长度字段 + JSON Header + Body）
- 可插拔编解码器：JSON / Protobuf（注册表 + 工厂模式）
- gzip 压缩
- 基于 etcd 的服务注册与发现（Lease + KeepAlive + Watch）
- 负载均衡：轮询 / 随机 / 平滑加权轮询
- 熔断器（Closed / Open / HalfOpen 三态）
- 令牌桶限流（客户端与服务端）
- TCP 连接池，长连接复用
- 异步调用（`Future`）+ 同步封装，靠 `RequestID` 在单连接上多路复用
- 最大包头/包体长度限制，防止超长包导致 OOM
- 优雅关闭

## 调用流程

```
client.Invoke
   │
   ▼
限流 ──► 服务发现(etcd) ──► 负载均衡选中实例 ──► 熔断判断
   │                                                │
   │                                          连接池取连接
   ▼                                                │
序列化(codec) + 打包(protocol) + gzip ◄─────────────┘
   │
   ▼  TCP 字节流
════════════════ 网络 ════════════════
   │
   ▼
Accept ──► 每连接一个 goroutine ──► 拆包(io.ReadFull)
   │
   ▼
限流 ──► 反射调用业务方法 ──► 序列化 + gzip ──► 回写
```
## 具体过程
```
【发送端 Encode 封包流程】
 内存中的结构体 (Struct) 
       │
       ▼  1. 序列化 (JSON / Protobuf)
 原始字节流 ([]byte)
       │
       ▼  2. 数据压缩 (Gzip / Snappy / Zstd)
 压缩后的字节流 ([]byte)
       │
       ▼  3. 网络封包 (拼接 Magic + Length + Header + Body)
 二进制网络数据帧 ──────( TCP 传输 )──────>

────────────────────────────────────────────────────────

【接收端 Decode 解包流程】
 二进制网络数据帧
       │
       ▼  1. 拆包提取 (根据 Length 切割 HeaderBytes 与 BodyBytes)
 压缩的字节流
       │
       ▼  2. 数据解压 (Decompress)
 原始字节流
       │
       ▼  3. 反序列化 (Unmarshal 为 Struct)
 内存中的结构体 (Struct)
```
## 目录结构

```
cmd/
  server1/    示例服务端（注册 Arith、Arith2，监听 :9090）
  server2/    示例服务端（注册 Arith，监听 :9091）
  client/     示例客户端（周期性调用）
  bench1/     基准测试：定量请求（-n / -c / -b）
  bench2/     基准测试：定时压测（-c / -d），输出 P50/P90/P99
pkg/api/      示例服务实现（net/rpc 风格）
internal/
  protocol/   协议编解码、包长校验
  transport/  TCP 连接、连接池、Future 异步结果
  codec/      编解码器注册表（JSON / Protobuf）与 gzip 压缩
  registry/   基于 etcd 的服务注册与发现
  loadbalance/ 负载均衡算法
  breaker/    熔断器
  limiter/    令牌桶限流
  server/     监听、Handler 反射调用、优雅关闭
  client/     组合注册中心 / 负载均衡 / 熔断 / 连接池
  config/     配置读取（flag / 环境变量 / 默认值）
```

## 协议格式

```
┌────────┬───────────┬─────────┬──────────────┬──────────────┐
│ Magic  │ headerLen │ bodyLen │ header(JSON) │  body(gzip)  │
│ 2 字节 │  4 字节   │ 4 字节  │    变长      │    变长      │
│ 0x1234 │           │         │              │              │
└────────┴───────────┴─────────┴──────────────┴──────────────┘
```

- 固定包头 10 字节，`HeaderSize = 10`。
- 读取时先校验 `Magic` 与长度上限（`MaxHeaderLen = 64KB`，`MaxBodyLen = 16MB`），再用 `io.ReadFull` 精确读取，天然处理 TCP 粘包/半包。
- Header 使用 JSON 编码，便于扩展与调试。

## 快速开始

### 1. 环境要求

- Go 1.25+
- 一个可用的 etcd（默认 `localhost:2379`）

### 2. 启动 etcd（Docker）

```bash
docker run -d --name etcd -p 2379:2379 -p 2380:2380 \
  quay.io/coreos/etcd:v3.5.16 \
  /usr/local/bin/etcd \
  --name s1 --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://0.0.0.0:2380 \
  --initial-cluster s1=http://0.0.0.0:2380 \
  --initial-cluster-token tkn --initial-cluster-state new
```

### 3. 启动服务端与客户端

```bash
go run ./cmd/server1
go run ./cmd/server2
go run ./cmd/client
```

## 配置

所有可执行程序均支持命令行参数，也可用环境变量覆盖。优先级：**flag > 环境变量 > 默认值**。

| 程序 | flag | 环境变量 | 默认值 |
|---|---|---|---|
| server1 | `-etcd` | `KAMARPC_ETCD` | `localhost:2379` |
| server1 | `-listen` | `KAMARPC_LISTEN` | `:9090` |
| server1 | `-advertise` | `KAMARPC_ADVERTISE` | `localhost:9090` |
| server2 | `-etcd` | `KAMARPC_ETCD` | `localhost:2379` |
| server2 | `-listen` | `KAMARPC_LISTEN` | `:9091` |
| server2 | `-advertise` | `KAMARPC_ADVERTISE` | `localhost:9091` |
| client / bench | `-etcd` | `KAMARPC_ETCD` | `localhost:2379` |

`-etcd` 支持逗号分隔的多个地址：

```bash
go run ./cmd/client -etcd 10.0.0.1:2379,10.0.0.2:2379
```

## 压测

```bash
# 定量：共 200 请求，20 并发，批大小 20
go run ./cmd/bench1 -n 200 -c 20 -b 20

# 定时：50 并发，持续 10 秒
go run ./cmd/bench2 -c 50 -d 10
```

## 测试

```bash
go test ./...
```

## Roadmap

- [ ] 同一连接内并发处理请求（当前为串行，见 `internal/server/server.go`）
- [ ] 读写超时与连接数上限（防 Slowloris）
- [ ] 真实令牌桶（当前为固定窗口简化实现）
- [ ] 服务下线时主动注销 + KeepAlive goroutine 生命周期管理
- [ ] 一致性哈希 / P2C 负载均衡
- [ ] 支持流式（streaming）调用

## 已知问题

- `internal/server/server.go` 的 `Handle` 对同一连接串行处理请求，吞吐受限。
- `internal/limiter/token_bucket.go` 为固定窗口实现，非连续补充。
- `internal/registry/registry.go` 的 KeepAlive 协程缺少退出机制。
- `internal/loadbalance/weighted_rr.go` 在实例数量变化时直接返回空实例。

## License

[AGPL-3.0](LICENSE)
