package nats

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Command NATS命令接口
type Command interface {
	// 获取命令的主题
	Subject() string
	// 获取命令的负载数据
	Payload() []byte
	// 是否需要JetStream上下文
	RequiresJetStream() bool
	// 基础NATS连接执行
	Execute(conn *nats.Conn) error
}

// JetStreamCommand JetStream专用命令接口
type JetStreamCommand interface {
	Command
	// JetStream上下文执行
	ExecuteJS(js jetstream.JetStream) error
}

// RequestCommand 请求响应命令接口
type RequestCommand interface {
	Command
	// 获取超时时间
	Timeout() int64
	// 执行请求并返回响应
	Request(conn *nats.Conn) (*nats.Msg, error)
}

// Result 命令执行结果
type Result struct {
	Success bool
	Data    interface{}
	Error   error
	Message string
}