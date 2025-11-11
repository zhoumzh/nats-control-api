package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"nats-control-api/pkg/models"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ======================== 核心服务类型 ========================

type Service struct {
	executor *Executor
}

type JetStreamContext nats.JetStreamContext

// ======================== 消息（Message）操作 ========================

func NewService() *Service {
	return &Service{
		executor: NewExecutor(),
	}
}

// Publish 发布消息
func (s *Service) PublishMessage(conn *nats.Conn, subject string, payload []byte) error {
	log.WithContext(context.Background()).Infof("PublishMessage - subject: %s, payload: %s", subject, string(payload))
	cmd := NewPublishCommand(subject, payload)
	result := s.executor.Execute(cmd, conn)
	if result.Error != nil {
		log.WithContext(context.Background()).Errorf("PublishMessage failed - subject: %s, error: %v", subject, result.Error)
	} else {
		log.WithContext(context.Background()).Infof("PublishMessage success - subject: %s", subject)
	}
	return result.Error
}

// ======================== JWT 操作 ========================

// Request 请求响应 - 支持多响应收集
func (s *Service) RequestMessage(conn *nats.Conn, subject string, payload []byte, timeout time.Duration, expectedResponses int, responseHandler func(*nats.Msg)) error {
	log.WithContext(context.Background()).Infof("RequestMessage - subject: %s, payload: %s, timeout: %v, expected: %d", subject, string(payload), timeout, expectedResponses)

	if timeout == 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	respInbox := nats.NewInbox()

	responseCount := 0
	done := make(chan struct{})

	sub, err := conn.Subscribe(respInbox, func(msg *nats.Msg) {
		responseHandler(msg)
		responseCount++

		if expectedResponses > 0 && responseCount >= expectedResponses {
			close(done)
		}
	})
	if err != nil {
		log.WithContext(context.Background()).Errorf("RequestMessage failed to subscribe to response inbox: %v", err)
		return fmt.Errorf("failed to subscribe to response inbox: %w", err)
	}
	defer sub.Unsubscribe()

	msg := &nats.Msg{
		Subject: subject,
		Reply:   respInbox,
		Data:    payload,
	}

	if err := conn.PublishMsg(msg); err != nil {
		log.WithContext(context.Background()).Errorf("RequestMessage failed to publish request: %v", err)
		return fmt.Errorf("failed to publish request: %w", err)
	}

	log.WithContext(context.Background()).Infof("RequestMessage sent to %s, waiting for responses", subject)

	select {
	case <-done:
		log.WithContext(context.Background()).Infof("RequestMessage collected %d responses", responseCount)
	case <-ctx.Done():
		log.WithContext(context.Background()).Warnf("RequestMessage timeout reached, collected %d responses", responseCount)
	}

	return nil
}

// ======================== Stream 操作 ========================

// Push 推送JWT到集群
func (s *Service) PushJWT(conn *nats.Conn, accountID, jwtToken string) error {
	log.WithContext(context.Background()).Infof("PushJWT - accountID: %s, jwtToken: %s", accountID, jwtToken)
	cmd := NewPushJWTCommand(accountID, jwtToken)
	result := s.executor.ExecuteRequest(cmd, conn)
	if result.Error != nil {
		log.WithContext(context.Background()).Errorf("PushJWT failed - accountID: %s, error: %v", accountID, result.Error)
	} else {
		log.WithContext(context.Background()).Infof("PushJWT success - accountID: %s", accountID)
	}
	return result.Error
}

// Create 创建流
func (s *Service) CreateStream(js jetstream.JetStream, config jetstream.StreamConfig) (jetstream.Stream, error) {
	log.WithContext(context.Background()).Infof("CreateStream - config: %+v", config)
	ctx := context.Background()
	stream, err := js.CreateStream(ctx, config)
	if err != nil {
		log.WithContext(context.Background()).Errorf("CreateStream failed - name: %s, error: %v", config.Name, err)
		return nil, err
	}
	log.WithContext(context.Background()).Infof("CreateStream success - name: %s", config.Name)
	return stream, nil
}

// Update 更新流
func (s *Service) UpdateStream(js jetstream.JetStream, config jetstream.StreamConfig) (jetstream.Stream, error) {
	log.WithContext(context.Background()).Infof("UpdateStream - config: %+v", config)
	ctx := context.Background()
	stream, err := js.UpdateStream(ctx, config)
	if err != nil {
		log.WithContext(context.Background()).Errorf("UpdateStream failed - name: %s, error: %v", config.Name, err)
		return nil, err
	}
	log.WithContext(context.Background()).Infof("UpdateStream success - name: %s", config.Name)
	return stream, nil
}

// Delete 删除流
func (s *Service) DeleteStream(js jetstream.JetStream, streamName string) error {
	log.WithContext(context.Background()).Infof("DeleteStream - name: %s", streamName)
	ctx := context.Background()
	err := js.DeleteStream(ctx, streamName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("DeleteStream failed - name: %s, error: %v", streamName, err)
	} else {
		log.WithContext(context.Background()).Infof("DeleteStream success - name: %s", streamName)
	}
	return err
}

// Get 获取流信息
func (s *Service) GetStreamInfo(js jetstream.JetStream, streamName string) (*jetstream.StreamInfo, error) {
	log.WithContext(context.Background()).Infof("GetStreamInfo - name: %s", streamName)
	ctx := context.Background()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("GetStreamInfo failed to get stream - name: %s, error: %v", streamName, err)
		return nil, err
	}

	info, err := stream.Info(ctx)
	if err != nil {
		log.WithContext(context.Background()).Errorf("GetStreamInfo failed to get info - name: %s, error: %v", streamName, err)
		return nil, err
	}
	log.WithContext(context.Background()).Infof("GetStreamInfo success - name: %s, messages: %d", streamName, info.State.Msgs)
	return info, nil
}

// ======================== Consumer 操作 ========================

// List 列出所有流
func (s *Service) ListStreams(js jetstream.JetStream) ([]string, error) {
	log.WithContext(context.Background()).Infof("ListStreams")
	ctx := context.Background()
	streams := js.ListStreams(ctx)

	streamNamesSet := make(map[string]struct{})
	for stream := range streams.Info() {
		streamNamesSet[stream.Config.Name] = struct{}{}
	}

	if streams.Err() != nil {
		log.WithContext(context.Background()).Errorf("ListStreams failed - error: %v", streams.Err())
		return nil, streams.Err()
	}

	var streamNames []string
	for name := range streamNamesSet {
		streamNames = append(streamNames, name)
	}

	log.WithContext(context.Background()).Infof("ListStreams success - found %d streams: %v", len(streamNames), streamNames)
	return streamNames, nil
}

// Create 创建消费者
func (s *Service) CreateConsumer(js jetstream.JetStream, streamName string, config jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	log.WithContext(context.Background()).Infof("CreateConsumer - stream: %s, config: %+v", streamName, config)
	ctx := context.Background()
	consumer, err := js.CreateConsumer(ctx, streamName, config)
	if err != nil {
		log.WithContext(context.Background()).Errorf("CreateConsumer failed - stream: %s, consumer: %s, error: %v", streamName, config.Name, err)
		return nil, err
	}
	log.WithContext(context.Background()).Infof("CreateConsumer success - stream: %s, consumer: %s", streamName, config.Name)
	return consumer, nil
}

// Update 更新消费者
func (s *Service) UpdateConsumer(js jetstream.JetStream, streamName string, config jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	log.WithContext(context.Background()).Infof("UpdateConsumer - stream: %s, config: %+v", streamName, config)
	ctx := context.Background()
	consumer, err := js.UpdateConsumer(ctx, streamName, config)
	if err != nil {
		log.WithContext(context.Background()).Errorf("UpdateConsumer failed - stream: %s, consumer: %s, error: %v", streamName, config.Name, err)
		return nil, err
	}
	log.WithContext(context.Background()).Infof("UpdateConsumer success - stream: %s, consumer: %s", streamName, config.Name)
	return consumer, nil
}

// Delete 删除消费者
func (s *Service) DeleteConsumer(js jetstream.JetStream, streamName, consumerName string) error {
	log.WithContext(context.Background()).Infof("DeleteConsumer - stream: %s, consumer: %s", streamName, consumerName)
	ctx := context.Background()
	err := js.DeleteConsumer(ctx, streamName, consumerName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("DeleteConsumer failed - stream: %s, consumer: %s, error: %v", streamName, consumerName, err)
	} else {
		log.WithContext(context.Background()).Infof("DeleteConsumer success - stream: %s, consumer: %s", streamName, consumerName)
	}
	return err
}

// Get 获取消费者信息
func (s *Service) GetConsumerInfo(jsc JetStreamContext, streamName, consumerName string) (*nats.ConsumerInfo, error) {
	log.WithContext(context.Background()).Infof("GetConsumerInfo - stream: %s, consumer: %s", streamName, consumerName)

	info, err := jsc.ConsumerInfo(streamName, consumerName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("GetConsumerInfo failed - stream: %s, consumer: %s, error: %v", streamName, consumerName, err)
		return nil, err
	}
	return info, nil
}

// List 列出流的所有消费者
func (s *Service) ListConsumers(js jetstream.JetStream, streamName string) ([]string, error) {
	log.WithContext(context.Background()).Infof("ListConsumers - stream: %s", streamName)
	ctx := context.Background()

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("ListConsumers failed to get stream - stream: %s, error: %v", streamName, err)
		return nil, err
	}

	consumers := stream.ListConsumers(ctx)
	consumerNamesSet := make(map[string]struct{})

	for consumer := range consumers.Info() {
		consumerNamesSet[consumer.Name] = struct{}{}
	}

	if consumers.Err() != nil {
		log.WithContext(context.Background()).Errorf("ListConsumers failed - stream: %s, error: %v", streamName, consumers.Err())
		return nil, consumers.Err()
	}

	var consumerNames []string
	for name := range consumerNamesSet {
		consumerNames = append(consumerNames, name)
	}

	log.WithContext(context.Background()).Infof("ListConsumers success - stream: %s, found %d consumers: %v", streamName, len(consumerNames), consumerNames)
	return consumerNames, nil
}

// Pause 暂停消费者
func (s *Service) PauseConsumer(js jetstream.JetStream, streamName, consumerName string, pauseUntil time.Time) error {
	log.WithContext(context.Background()).Infof("PauseConsumer - stream: %s, consumer: %s, until: %v", streamName, consumerName, pauseUntil)
	ctx := context.Background()

	_, err := js.PauseConsumer(ctx, streamName, consumerName, pauseUntil)
	if err != nil {
		log.WithContext(context.Background()).Errorf("PauseConsumer failed - stream: %s, consumer: %s, error: %v", streamName, consumerName, err)
	} else {
		log.WithContext(context.Background()).Infof("PauseConsumer success - stream: %s, consumer: %s", streamName, consumerName)
	}
	return err
}

// Resume 恢复消费者
func (s *Service) ResumeConsumer(js jetstream.JetStream, streamName, consumerName string) error {
	log.WithContext(context.Background()).Infof("ResumeConsumer - stream: %s, consumer: %s", streamName, consumerName)
	ctx := context.Background()

	_, err := js.ResumeConsumer(ctx, streamName, consumerName)
	if err != nil {
		log.WithContext(context.Background()).Errorf("ResumeConsumer failed - stream: %s, consumer: %s, error: %v", streamName, consumerName, err)
	} else {
		log.WithContext(context.Background()).Infof("ResumeConsumer success - stream: %s, consumer: %s", streamName, consumerName)
	}
	return err
}

// ======================== Server 操作 ========================

// List 使用NATS系统API获取服务器列表
func (s *Service) ListServers(conn *nats.Conn, timeout time.Duration, expectedServers int) (*models.ServerListResponse, error) {
	log.WithContext(context.Background()).Infof("ListServers - timeout: %v, expected: %d", timeout, expectedServers)

	collector := &serverResponseCollector{
		responses:       make([]models.ServerStatsMsg, 0),
		expectedServers: expectedServers,
		timeout:         timeout,
		done:            make(chan struct{}),
	}

	err := s.RequestMessage(conn, "$SYS.REQ.SERVER.PING", []byte{}, timeout, expectedServers,
		func(msg *nats.Msg) {
			collector.handleResponse(msg)
		})

	if err != nil {
		log.WithContext(context.Background()).Errorf("ListServers failed: %v", err)
		return nil, fmt.Errorf("failed to request server list: %w", err)
	}

	response := &models.ServerListResponse{
		Servers:      collector.responses,
		TotalServers: len(collector.responses),
		RequestedAt:  models.NewCustomTime(time.Now()),
		Timeout:      timeout.String(),
	}

	if len(collector.responses) == 0 {
		return response, fmt.Errorf("no servers responded - ensure the account has system privileges")
	}

	log.WithContext(context.Background()).Infof("ListServers success - found %d servers", len(collector.responses))
	return response, nil
}

// ======================== 内部辅助类型 ========================

type serverResponseCollector struct {
	mu              sync.Mutex
	responses       []models.ServerStatsMsg
	expectedServers int
	timeout         time.Duration
	done            chan struct{}
	finished        bool
}

func (c *serverResponseCollector) handleResponse(msg *nats.Msg) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.finished {
		return
	}

	var serverStats models.ServerStatsMsg
	if err := json.Unmarshal(msg.Data, &serverStats); err != nil {
		log.WithContext(context.Background()).Errorf("Failed to unmarshal server response: %v", err)
		return
	}

	c.responses = append(c.responses, serverStats)
	log.WithContext(context.Background()).Debugf("Received server response from %s (ID: %s), total: %d",
		serverStats.Server.Name, serverStats.Server.ID, len(c.responses))

	if c.expectedServers > 0 && len(c.responses) >= c.expectedServers {
		c.finished = true
		close(c.done)
	}
}
