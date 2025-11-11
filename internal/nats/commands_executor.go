package nats

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Executor 命令执行器
type Executor struct{}

// NewExecutor 创建新的命令执行器
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute 执行命令
func (e *Executor) Execute(cmd Command, conn *nats.Conn) *Result {
	if cmd.RequiresJetStream() {
		// 如果需要JetStream，创建JetStream上下文
		js, err := jetstream.New(conn)
		if err != nil {
			return &Result{
				Success: false,
				Error:   fmt.Errorf("创建JetStream上下文失败: %v", err),
				Message: "JetStream初始化失败",
			}
		}

		// 执行JetStream命令
		if jsCmd, ok := cmd.(JetStreamCommand); ok {
			err = jsCmd.ExecuteJS(js)
		} else {
			err = fmt.Errorf("命令未实现JetStreamCommand接口")
		}
	} else {
		// 执行基础NATS命令
		err := cmd.Execute(conn)
		if err != nil {
			return &Result{
				Success: false,
				Error:   err,
				Message: "命令执行失败",
			}
		}
	}

	return &Result{
		Success: true,
		Message: "命令执行成功",
	}
}

// ExecuteRequest 执行请求命令并返回响应
func (e *Executor) ExecuteRequest(cmd RequestCommand, conn *nats.Conn) *Result {
	resp, err := cmd.Request(conn)
	if err != nil {
		return &Result{
			Success: false,
			Error:   err,
			Message: "请求执行失败",
		}
	}

	return &Result{
		Success: true,
		Data:    resp,
		Message: "请求执行成功",
	}
}

// ExecuteWithTimeout 带超时的命令执行
func (e *Executor) ExecuteWithTimeout(cmd Command, conn *nats.Conn, timeout time.Duration) *Result {
	resultChan := make(chan *Result, 1)

	go func() {
		resultChan <- e.Execute(cmd, conn)
	}()

	select {
	case result := <-resultChan:
		return result
	case <-time.After(timeout):
		return &Result{
			Success: false,
			Error:   fmt.Errorf("命令执行超时"),
			Message: "命令执行超时",
		}
	}
}