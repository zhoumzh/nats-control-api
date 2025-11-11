package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PublishCommand 发布消息命令
type PublishCommand struct {
	subject string
	payload []byte
}

// NewPublishCommand 创建发布命令
func NewPublishCommand(subject string, payload []byte) *PublishCommand {
	return &PublishCommand{
		subject: subject,
		payload: payload,
	}
}

func (c *PublishCommand) Subject() string {
	return c.subject
}

func (c *PublishCommand) Payload() []byte {
	return c.payload
}

func (c *PublishCommand) RequiresJetStream() bool {
	return false
}

func (c *PublishCommand) Execute(conn *nats.Conn) error {
	return conn.Publish(c.subject, c.payload)
}

// RequestResponseCommand 请求响应命令
type RequestResponseCommand struct {
	subject string
	payload []byte
	timeout time.Duration
}

// NewRequestCommand 创建请求命令
func NewRequestCommand(subject string, payload []byte, timeout time.Duration) *RequestResponseCommand {
	return &RequestResponseCommand{
		subject: subject,
		payload: payload,
		timeout: timeout,
	}
}

func (c *RequestResponseCommand) Subject() string {
	return c.subject
}

func (c *RequestResponseCommand) Payload() []byte {
	return c.payload
}

func (c *RequestResponseCommand) RequiresJetStream() bool {
	return false
}

func (c *RequestResponseCommand) Execute(conn *nats.Conn) error {
	_, err := conn.Request(c.subject, c.payload, c.timeout)
	return err
}

func (c *RequestResponseCommand) Timeout() int64 {
	return c.timeout.Milliseconds()
}

func (c *RequestResponseCommand) Request(conn *nats.Conn) (*nats.Msg, error) {
	return conn.Request(c.subject, c.payload, c.timeout)
}

// PushJWTCommand JWT推送命令
type PushJWTCommand struct {
	accountID string
	jwtToken  string
}

// NewPushJWTCommand 创建JWT推送命令
func NewPushJWTCommand(accountID, jwtToken string) *PushJWTCommand {
	return &PushJWTCommand{
		accountID: accountID,
		jwtToken:  jwtToken,
	}
}

func (c *PushJWTCommand) Subject() string {
	return fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.CLAIMS.UPDATE", c.accountID)
}

func (c *PushJWTCommand) Payload() []byte {
	return []byte(c.jwtToken)
}

func (c *PushJWTCommand) RequiresJetStream() bool {
	return false
}

func (c *PushJWTCommand) Execute(conn *nats.Conn) error {
	_, err := conn.Request(c.Subject(), c.Payload(), 10*time.Second)
	return err
}

func (c *PushJWTCommand) Timeout() int64 {
	return 10000 // 10秒
}

func (c *PushJWTCommand) Request(conn *nats.Conn) (*nats.Msg, error) {
	return conn.Request(c.Subject(), c.Payload(), 10*time.Second)
}

// CreateStreamCommand 创建流命令
type CreateStreamCommand struct {
	config jetstream.StreamConfig
}

// NewCreateStreamCommand 创建流命令
func NewCreateStreamCommand(config jetstream.StreamConfig) *CreateStreamCommand {
	return &CreateStreamCommand{config: config}
}

func (c *CreateStreamCommand) Subject() string {
	return fmt.Sprintf("$JS.API.STREAM.CREATE.%s", c.config.Name)
}

func (c *CreateStreamCommand) Payload() []byte {
	data, _ := json.Marshal(c.config)
	return data
}

func (c *CreateStreamCommand) RequiresJetStream() bool {
	return true
}

func (c *CreateStreamCommand) Execute(conn *nats.Conn) error {
	return fmt.Errorf("创建流需要使用JetStream上下文")
}

func (c *CreateStreamCommand) ExecuteJS(js jetstream.JetStream) error {
	ctx := context.Background()
	_, err := js.CreateStream(ctx, c.config)
	return err
}

// DeleteStreamCommand 删除流命令
type DeleteStreamCommand struct {
	streamName string
}

// NewDeleteStreamCommand 创建删除流命令
func NewDeleteStreamCommand(streamName string) *DeleteStreamCommand {
	return &DeleteStreamCommand{streamName: streamName}
}

func (c *DeleteStreamCommand) Subject() string {
	return fmt.Sprintf("$JS.API.STREAM.DELETE.%s", c.streamName)
}

func (c *DeleteStreamCommand) Payload() []byte {
	return []byte{}
}

func (c *DeleteStreamCommand) RequiresJetStream() bool {
	return true
}

func (c *DeleteStreamCommand) Execute(conn *nats.Conn) error {
	return fmt.Errorf("删除流需要使用JetStream上下文")
}

func (c *DeleteStreamCommand) ExecuteJS(js jetstream.JetStream) error {
	ctx := context.Background()
	return js.DeleteStream(ctx, c.streamName)
}

// Ensure interfaces are implemented
var (
	_ Command             = (*PublishCommand)(nil)
	_ RequestCommand      = (*RequestResponseCommand)(nil)
	_ RequestCommand      = (*PushJWTCommand)(nil)
	_ JetStreamCommand    = (*CreateStreamCommand)(nil)
	_ JetStreamCommand    = (*DeleteStreamCommand)(nil)
)