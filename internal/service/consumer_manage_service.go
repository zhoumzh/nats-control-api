package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"nats-control-api/internal/config"
	"nats-control-api/internal/db"
	"nats-control-api/internal/jwt"
	"nats-control-api/internal/nats"
	"nats-control-api/pkg/models"

	natsgo "github.com/nats-io/nats.go"
	jsApi "github.com/nats-io/nats.go/jetstream"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type ConsumerManageService struct {
	repo             *db.Repository
	config           *config.Config
	jetStreamService *JetStreamManageService
	clusterService   *ClusterService
	natsService      *nats.Service
	natsManager      *jwt.NATSManager
}

func NewConsumerManageService(repo *db.Repository, config *config.Config, jetStreamService *JetStreamManageService, clusterService *ClusterService, natsManager *jwt.NATSManager) *ConsumerManageService {
	return &ConsumerManageService{
		repo:             repo,
		config:           config,
		jetStreamService: jetStreamService,
		clusterService:   clusterService,
		natsService:      nats.NewService(),
		natsManager:      natsManager,
	}
}

func (s *ConsumerManageService) CreateConsumer(req *models.CreateConsumerRequest) (*models.Consumer, error) {
	log.WithContext(context.Background()).Infof("使用新架构启动Consumer创建进程: consumer_name=%s, stream_name=%s, mode=%s", req.ConsumerName, req.StreamName, req.Mode)

	if err := req.Validate(); err != nil {
		log.WithContext(context.Background()).Errorf("Consumer请求验证失败: %v", err)
		return nil, err
	}

	jetStream, err := s.repo.GetJetStreamByNameAndCluster(req.StreamName, req.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get jetstream by name: %w", err)
	}
	if jetStream == nil {
		return nil, fmt.Errorf("jetstream not found with name: %s", req.StreamName)
	}

	if req.NatsOperateUserID == "" {
		req.NatsOperateUserID = jetStream.NatsOperateUserID
	}

	user, err := s.repo.GetUser(req.NatsOperateUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get nats operate user: %w", err)
	}
	if user == nil {
		return nil, fmt.Errorf("nats operate user not found: %s", req.NatsOperateUserID)
	}

	consumerName := req.ConsumerName
	if req.Name != "" {
		consumerName = req.Name
	}

	existing, err := s.repo.GetConsumerByNameAndJetStream(consumerName, jetStream.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to check name uniqueness: %w", err)
	}
	if existing != nil {
		return nil, fmt.Errorf("consumer with name '%s' already exists in jetstream '%s'", consumerName, jetStream.ID)
	}

	var consumerType models.ConsumerType
	if req.Mode == "push" {
		consumerType = models.ConsumerTypePush
	} else if req.Mode == "pull" {
		consumerType = models.ConsumerTypePull
	} else if req.ConsumerType != "" {
		consumerType = req.ConsumerType
	} else {
		consumerType = models.ConsumerType(req.Mode)
	}

	ackWaitSeconds, err := models.ParseDurationToSeconds(req.AckWait)
	if err != nil {
		return nil, fmt.Errorf("invalid ack_wait format: %w", err)
	}

	backOffSeconds, err := models.ParseDurationSliceToSeconds(req.BackOff)
	if err != nil {
		return nil, fmt.Errorf("invalid backoff format: %w", err)
	}

	idleHeartbeatSeconds, err := models.ParseDurationToSeconds(req.IdleHeartbeat)
	if err != nil {
		return nil, fmt.Errorf("invalid idle_heartbeat format: %w", err)
	}

	maxExpiresSeconds, err := models.ParseDurationToSeconds(req.MaxExpires)
	if err != nil {
		return nil, fmt.Errorf("invalid max_expires format: %w", err)
	}

	var startSeq uint64
	if req.StartSeq != nil {
		startSeq = *req.StartSeq
	} else {
		startSeq = req.OptStartSeq
	}

	var startTime *time.Time
	if req.StartTime != nil {
		startTime = req.StartTime
	} else {
		startTime = req.OptStartTime
	}

	var rateLimitBps uint64
	if req.RateLimitBps != nil {
		rateLimitBps = *req.RateLimitBps
	} else {
		rateLimitBps = req.RateLimit
	}

	var maxBatch, maxBytes int
	if req.MaxBatch != nil {
		maxBatch = *req.MaxBatch
	}
	if req.MaxBytes != nil {
		maxBytes = *req.MaxBytes
	}

	var maxDeliver int
	if req.MaxDeliver != nil {
		maxDeliver = *req.MaxDeliver
	} else {
		maxDeliver = -1
	}

	consumer := &models.Consumer{
		ID:                 models.GenID(models.ConsumerObjType),
		Name:               consumerName,
		Durable:            req.Durable,
		Description:        req.Description,
		JetStreamID:        jetStream.ID,
		NatsOperateUserID:  req.NatsOperateUserID,
		Mode:               req.Mode,
		ConsumerType:       consumerType,
		DeliverPolicy:      req.DeliverPolicy,
		StartSeq:           startSeq,
		StartTime:          startTime,
		OptStartSeq:        req.OptStartSeq,
		OptStartTime:       req.OptStartTime,
		AckPolicy:          req.AckPolicy,
		AckWait:            ackWaitSeconds,
		MaxDeliver:         maxDeliver,
		BackOff:            backOffSeconds,
		RetryPolicy:        req.RetryPolicy,
		FilterSubject:      req.FilterSubject,
		FilterSubjects:     req.FilterSubjects,
		ReplayPolicy:       req.ReplayPolicy,
		RateLimitBps:       rateLimitBps,
		RateLimit:          req.RateLimit,
		SampleFrequency:    req.SampleFrequency,
		MaxWaiting:         req.MaxWaiting,
		MaxAckPending:      req.MaxAckPending,
		MaxBatch:           maxBatch,
		MaxBytes:           maxBytes,
		MaxExpires:         maxExpiresSeconds,
		MaxRequestBatch:    req.MaxRequestBatch,
		MaxRequestExpires:  req.MaxRequestExpires,
		MaxRequestMaxBytes: req.MaxRequestMaxBytes,
		DeliverSubject:     req.DeliverSubject,
		DeliverGroup:       req.DeliverGroup,
		FlowControl:        req.FlowControl,
		IdleHeartbeat:      idleHeartbeatSeconds,
		HeadersOnly:        req.HeadersOnly,
		InactiveThreshold:  req.InactiveThreshold,
		Replicas:           req.Replicas,
		MemoryStorage:      req.MemoryStorage,
		Metadata:           req.Metadata,
		Status:             "active",
		SyncStatus:         models.ConsumerSyncPending,
		SyncMessage:        "等待同步到NATS服务器",
		CreatedAt:          models.CustomTime{Time: time.Now()},
		UpdatedAt:          models.CustomTime{Time: time.Now()},
	}

	if err := s.repo.CreateConsumer(consumer); err != nil {
		return nil, fmt.Errorf("failed to save consumer to database: %w", err)
	}

	go s.asyncCreateConsumerOnCluster(consumer, jetStream)

	log.WithContext(context.Background()).Infof("成功创建 Consumer 记录，异步同步中: consumer_id=%s", consumer.ID)
	return consumer, nil
}

func (s *ConsumerManageService) UpdateConsumer(id string, req *models.UpdateConsumerRequest) (*models.Consumer, error) {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始更新Consumer: consumer_id=%s", id)

	consumer, err := s.repo.GetConsumer(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}
	if consumer == nil {
		return nil, fmt.Errorf("consumer not found: %s", id)
	}

	if err := req.Validate(consumer.Mode); err != nil {
		return nil, err
	}

	deliverPolicy := consumer.DeliverPolicy
	if req.DeliverPolicy != "" {
		deliverPolicy = req.DeliverPolicy
	}

	filterSubject := consumer.FilterSubject
	if req.FilterSubject != "" {
		filterSubject = req.FilterSubject
	}

	filterSubjects := consumer.FilterSubjects
	if len(req.FilterSubjects) > 0 {
		filterSubjects = req.FilterSubjects
	}

	if deliverPolicy == models.DeliverLastPerSubject {
		if filterSubject == "" && len(filterSubjects) == 0 {
			return nil, models.NewValidationError("filter_subject", "filter_subject or filter_subjects is required when deliver_policy is 'last_per_subject'")
		}
	}

	if req.Mode != "" {
		consumer.Mode = req.Mode
		if req.Mode == "push" {
			consumer.ConsumerType = models.ConsumerTypePush
		} else if req.Mode == "pull" {
			consumer.ConsumerType = models.ConsumerTypePull
		}
	}

	if req.Name != "" {
		consumer.Name = req.Name
	}
	if req.Durable != "" {
		consumer.Durable = req.Durable
	}
	if req.Description != "" {
		consumer.Description = req.Description
	}
	if req.JetStreamID != "" {
		consumer.JetStreamID = req.JetStreamID
	}
	if req.ConsumerType != "" {
		consumer.ConsumerType = req.ConsumerType
	}
	if req.DeliverPolicy != "" {
		consumer.DeliverPolicy = req.DeliverPolicy
	}
	if req.AckPolicy != "" {
		consumer.AckPolicy = req.AckPolicy
	}
	if req.AckWait != nil {
		ackWaitSeconds, err := models.ParseDurationToSeconds(*req.AckWait)
		if err != nil {
			return nil, fmt.Errorf("invalid ack_wait format: %w", err)
		}
		consumer.AckWait = ackWaitSeconds
	}
	if req.MaxDeliver != nil {
		consumer.MaxDeliver = *req.MaxDeliver
	}
	if len(req.BackOff) > 0 {
		backOffSeconds, err := models.ParseDurationSliceToSeconds(req.BackOff)
		if err != nil {
			return nil, fmt.Errorf("invalid backoff format: %w", err)
		}
		consumer.BackOff = backOffSeconds
	}
	if req.RetryPolicy != "" {
		consumer.RetryPolicy = req.RetryPolicy
	}
	if req.ReplayPolicy != "" {
		consumer.ReplayPolicy = req.ReplayPolicy
	}
	if req.RateLimit != nil {
		consumer.RateLimit = *req.RateLimit
	}
	if req.FilterSubject != "" {
		consumer.FilterSubject = req.FilterSubject
	}
	if len(req.FilterSubjects) > 0 {
		consumer.FilterSubjects = req.FilterSubjects
	}
	if req.SampleFrequency != "" {
		consumer.SampleFrequency = req.SampleFrequency
	}
	if req.MaxWaiting != nil {
		consumer.MaxWaiting = *req.MaxWaiting
	}
	if req.MaxAckPending != nil {
		consumer.MaxAckPending = *req.MaxAckPending
	}
	if req.RateLimitBps != nil {
		consumer.RateLimitBps = *req.RateLimitBps
	}
	if req.MaxBatch != nil {
		consumer.MaxBatch = *req.MaxBatch
	}
	if req.MaxBytes != nil {
		consumer.MaxBytes = *req.MaxBytes
	}
	if req.MaxExpires != nil {
		maxExpiresSeconds, err := models.ParseDurationToSeconds(*req.MaxExpires)
		if err != nil {
			return nil, fmt.Errorf("invalid max_expires format: %w", err)
		}
		consumer.MaxExpires = maxExpiresSeconds
	}
	if req.DeliverGroup != nil {
		consumer.DeliverGroup = *req.DeliverGroup
	}
	if req.DeliverSubject != "" {
		consumer.DeliverSubject = req.DeliverSubject
	}
	if req.FlowControl != nil {
		consumer.FlowControl = *req.FlowControl
	}
	if req.IdleHeartbeat != nil {
		idleHeartbeatSeconds, err := models.ParseDurationToSeconds(*req.IdleHeartbeat)
		if err != nil {
			return nil, fmt.Errorf("invalid idle_heartbeat format: %w", err)
		}
		consumer.IdleHeartbeat = idleHeartbeatSeconds
	}
	if req.HeadersOnly != nil {
		consumer.HeadersOnly = *req.HeadersOnly
	}
	if req.InactiveThreshold != nil {
		consumer.InactiveThreshold = *req.InactiveThreshold
	}
	if req.Replicas != nil {
		consumer.Replicas = *req.Replicas
	}
	if req.Metadata != nil {
		consumer.Metadata = req.Metadata
	}

	consumer.SyncStatus = models.ConsumerSyncPending
	consumer.SyncMessage = "等待同步更新到NATS服务器"
	consumer.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateConsumer(consumer); err != nil {
		return nil, fmt.Errorf("failed to update consumer in database: %w", err)
	}

	jetStream, err := s.repo.GetJetStream(consumer.JetStreamID)
	if err != nil {
		log.WithContext(ctx).Errorf("获取JetStream失败: %v", err)
		return consumer, nil
	}

	go s.asyncUpdateConsumerOnCluster(consumer, jetStream)

	log.WithContext(ctx).Infof("成功更新Consumer记录，异步同步中: consumer_id=%s", id)
	return consumer, nil
}

func (s *ConsumerManageService) DeleteConsumer(id string) error {
	log.WithContext(context.Background()).Infof("使用新架构启动Consumer删除: consumer_id=%s", id)

	consumer, err := s.repo.GetConsumer(id)
	if err != nil {
		return fmt.Errorf("failed to get Consumer: %w", err)
	}
	if consumer == nil {
		return fmt.Errorf("Consumer not found: %s", id)
	}

	if err := s.repo.DeleteConsumer(id); err != nil {
		return fmt.Errorf("failed to delete Consumer from database: %w", err)
	}

	log.WithContext(context.Background()).Infof("使用新架构成功删除Consumer: consumer_id=%s", id)
	return nil
}

func (s *ConsumerManageService) GetConsumer(id string) (*models.Consumer, error) {
	consumer, err := s.repo.GetConsumer(id)
	if err != nil {
		return nil, err
	}
	if consumer == nil {
		return nil, fmt.Errorf("consumer not found: %s", id)
	}

	jetStream, err := s.repo.GetJetStream(consumer.JetStreamID)
	if err != nil {
		log.Warnf("Failed to get JetStream for consumer %s: %v", id, err)
		return consumer, nil
	}

	conn, js, err := s.clusterService.GetNatsJetStreamContextWithAccount(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		log.Warnf("Failed to get live stats for consumer %s: %v", id, err)
		return consumer, nil
	}
	defer conn.Close()
	consumerInfo, err := s.natsService.GetConsumerInfo(js, jetStream.Name, consumer.Name)
	if err != nil {
		log.Warnf("Failed to get live stats for consumer %s: %v", id, err)
		return consumer, nil
	}

	consumer.Stats = s.convertConsumerInfoToStats(consumerInfo)
	return consumer, nil
}

func (s *ConsumerManageService) ListConsumers(req *models.ConsumerListRequest) ([]*models.Consumer, int64, error) {
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	offset := 0
	if req.Page > 1 {
		offset = (req.Page - 1) * limit
	}

	return s.repo.ListConsumers(
		req.ClusterID,
		req.JetStreamID,
		req.NatsOperateUserID,
		string(req.ConsumerType),
		req.Status,
		req.SyncStatus,
		req.Search,
		req.IsDurable,
		limit,
		offset,
	)
}

func (s *ConsumerManageService) RetryFailedSync(consumerID string) error {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始重试失败的同步: consumer_id=%s", consumerID)

	consumer, err := s.repo.GetConsumer(consumerID)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	if consumer == nil {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	if consumer.SyncStatus != models.ConsumerSyncFailed {
		return fmt.Errorf("consumer sync status is not failed: %s", consumer.SyncStatus)
	}

	jetStream, err := s.repo.GetJetStream(consumer.JetStreamID)
	if err != nil {
		return fmt.Errorf("failed to get jetstream: %w", err)
	}

	natsConn, jsCtx, err := s.clusterService.GetNatsJetStreamContextWithUsr(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		return fmt.Errorf("failed to get NATS connection: %w", err)
	}
	defer natsConn.Close()

	_, err = s.natsService.GetConsumerInfo(jsCtx, jetStream.Name, consumer.Name)
	consumerExists := err == nil

	err = s.repo.UpdateConsumerFields(consumerID, map[string]interface{}{
		"sync_status":  models.ConsumerSyncPending,
		"sync_message": "重试同步中...",
		"updated_at":   models.CustomTime{Time: time.Now()},
	})
	if err != nil {
		return fmt.Errorf("failed to update sync status: %w", err)
	}

	if consumerExists {
		log.WithContext(ctx).Infof("Consumer存在，执行更新重试: consumer_id=%s, consumer_name=%s", consumerID, consumer.Name)
		go s.asyncUpdateConsumerOnCluster(consumer, jetStream)
	} else {
		log.WithContext(ctx).Infof("Consumer不存在，执行创建重试: consumer_id=%s, consumer_name=%s", consumerID, consumer.Name)
		go s.asyncCreateConsumerOnCluster(consumer, jetStream)
	}

	log.WithContext(ctx).Infof("成功启动重试同步: consumer_id=%s", consumerID)
	return nil
}

func (s *ConsumerManageService) PauseConsumer(id string, until *time.Time) error {
	consumer, err := s.repo.GetConsumer(id)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	if consumer == nil {
		return fmt.Errorf("consumer not found: %s", id)
	}

	jetStream, err := s.repo.GetJetStream(consumer.JetStreamID)
	if err != nil {
		return fmt.Errorf("failed to get jetstream: %w", err)
	}

	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer conn.Close()

	pauseUntil := time.Now().Add(24 * time.Hour)
	if until != nil {
		pauseUntil = *until
	}

	err = s.natsService.PauseConsumer(js, jetStream.Name, consumer.Name, pauseUntil)
	if err != nil {
		return fmt.Errorf("failed to pause consumer: %w", err)
	}

	consumer.Status = "paused"
	consumer.PauseUntil = until
	consumer.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateConsumer(consumer); err != nil {
		return fmt.Errorf("failed to update consumer status: %w", err)
	}

	return nil
}

func (s *ConsumerManageService) ResumeConsumer(id string) error {
	consumer, err := s.repo.GetConsumer(id)
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	if consumer == nil {
		return fmt.Errorf("consumer not found: %s", id)
	}

	jetStream, err := s.repo.GetJetStream(consumer.JetStreamID)
	if err != nil {
		return fmt.Errorf("failed to get jetstream: %w", err)
	}

	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer conn.Close()

	err = s.natsService.ResumeConsumer(js, jetStream.Name, consumer.Name)
	if err != nil {
		return fmt.Errorf("failed to resume consumer: %w", err)
	}

	consumer.Status = "active"
	consumer.PauseUntil = nil
	consumer.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateConsumer(consumer); err != nil {
		return fmt.Errorf("failed to update consumer status: %w", err)
	}

	return nil
}

func (s *ConsumerManageService) asyncCreateConsumerOnCluster(consumer *models.Consumer, jetStream *models.JetStream) {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始异步创建Consumer到NATS集群: consumer_id=%s, consumer_name=%s", consumer.ID, consumer.Name)

	consumerConfig := s.buildConsumerConfig(consumer)

	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		log.WithContext(ctx).Errorf("获取JetStream上下文失败: %v", err)
		s.updateSyncStatus(consumer.ID, models.ConsumerSyncFailed, fmt.Sprintf("获取连接失败: %s", err.Error()))
		return
	}
	defer conn.Close()

	_, err = s.natsService.CreateConsumer(js, jetStream.Name, consumerConfig)
	if err != nil {
		log.WithContext(ctx).Errorf("异步创建Consumer失败: consumer_id=%s, error=%v", consumer.ID, err)
		s.updateSyncStatus(consumer.ID, models.ConsumerSyncFailed, fmt.Sprintf("创建Consumer失败: %s", err.Error()))
	} else {
		log.WithContext(ctx).Infof("异步创建Consumer成功: consumer_id=%s", consumer.ID)
		s.updateSyncStatus(consumer.ID, models.ConsumerSyncSynced, "成功同步到NATS服务器")
	}
}

func (s *ConsumerManageService) asyncUpdateConsumerOnCluster(consumer *models.Consumer, jetStream *models.JetStream) {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始异步更新Consumer到NATS集群: consumer_id=%s, consumer_name=%s", consumer.ID, consumer.Name)

	consumerConfig := s.buildConsumerConfig(consumer)

	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		log.WithContext(ctx).Errorf("获取JetStream上下文失败: %v", err)
		s.updateSyncStatus(consumer.ID, models.ConsumerSyncFailed, fmt.Sprintf("获取连接失败: %s", err.Error()))
		return
	}
	defer conn.Close()

	_, err = s.natsService.UpdateConsumer(js, jetStream.Name, consumerConfig)
	if err != nil {
		log.WithContext(ctx).Errorf("异步更新Consumer失败: consumer_id=%s, error=%v", consumer.ID, err)
		s.updateSyncStatus(consumer.ID, models.ConsumerSyncFailed, fmt.Sprintf("更新Consumer失败: %s", err.Error()))
	} else {
		log.WithContext(ctx).Infof("异步更新Consumer成功: consumer_id=%s", consumer.ID)
		s.updateSyncStatus(consumer.ID, models.ConsumerSyncSynced, "成功同步到NATS服务器")
	}
}

func (s *ConsumerManageService) updateSyncStatus(consumerID string, status models.ConsumerSyncStatus, message string) {
	err := s.repo.UpdateConsumerFields(consumerID, map[string]interface{}{
		"sync_status":  status,
		"sync_message": message,
		"updated_at":   models.CustomTime{Time: time.Now()},
	})
	if err != nil {
		log.WithContext(context.Background()).Errorf("更新同步状态失败: consumer_id=%s, error=%v", consumerID, err)
	}
}

func (s *ConsumerManageService) buildConsumerConfig(consumer *models.Consumer) jsApi.ConsumerConfig {
	config := jsApi.ConsumerConfig{
		Name:        consumer.Name,
		Durable:     consumer.Durable,
		Description: consumer.Description,
	}

	switch strings.ToLower(string(consumer.DeliverPolicy)) {
	case "all":
		config.DeliverPolicy = jsApi.DeliverAllPolicy
	case "last":
		config.DeliverPolicy = jsApi.DeliverLastPolicy
	case "new":
		config.DeliverPolicy = jsApi.DeliverNewPolicy
	case "by_start_sequence":
		config.DeliverPolicy = jsApi.DeliverByStartSequencePolicy
		if consumer.StartSeq > 0 {
			config.OptStartSeq = consumer.StartSeq
		} else {
			config.OptStartSeq = consumer.OptStartSeq
		}
	case "by_start_time":
		config.DeliverPolicy = jsApi.DeliverByStartTimePolicy
		if consumer.StartTime != nil {
			config.OptStartTime = consumer.StartTime
		} else {
			config.OptStartTime = consumer.OptStartTime
		}
	case "last_per_subject":
		config.DeliverPolicy = jsApi.DeliverLastPerSubjectPolicy
	default:
		config.DeliverPolicy = jsApi.DeliverAllPolicy
	}

	switch strings.ToLower(string(consumer.AckPolicy)) {
	case "ackexplicit", "explicit":
		config.AckPolicy = jsApi.AckExplicitPolicy
	case "ackall", "all":
		config.AckPolicy = jsApi.AckAllPolicy
	case "acknone", "none":
		config.AckPolicy = jsApi.AckNonePolicy
	default:
		config.AckPolicy = jsApi.AckExplicitPolicy
	}

	if consumer.AckWait > 0 {
		config.AckWait = time.Duration(consumer.AckWait) * time.Second
	}

	if consumer.MaxDeliver > 0 {
		config.MaxDeliver = consumer.MaxDeliver
	}

	if len(consumer.BackOff) > 0 {
		config.BackOff = make([]time.Duration, len(consumer.BackOff))
		for i, seconds := range consumer.BackOff {
			config.BackOff[i] = time.Duration(seconds) * time.Second
		}
	}

	if consumer.FilterSubject != "" {
		config.FilterSubject = consumer.FilterSubject
	}

	if len(consumer.FilterSubjects) > 0 {
		config.FilterSubjects = consumer.FilterSubjects
	}

	switch strings.ToLower(string(consumer.ReplayPolicy)) {
	case "instant":
		config.ReplayPolicy = jsApi.ReplayInstantPolicy
	case "original":
		config.ReplayPolicy = jsApi.ReplayOriginalPolicy
	default:
		config.ReplayPolicy = jsApi.ReplayInstantPolicy
	}

	if consumer.RateLimitBps > 0 {
		config.RateLimit = consumer.RateLimitBps
	} else if consumer.RateLimit > 0 {
		config.RateLimit = consumer.RateLimit
	}

	if consumer.SampleFrequency != "" {
		config.SampleFrequency = consumer.SampleFrequency
	}

	if consumer.Mode == "pull" || consumer.ConsumerType == models.ConsumerTypePull {
		if consumer.MaxWaiting > 0 {
			config.MaxWaiting = consumer.MaxWaiting
		}

		if consumer.MaxBatch > 0 {
			config.MaxRequestBatch = consumer.MaxBatch
		} else if consumer.MaxRequestBatch > 0 {
			config.MaxRequestBatch = consumer.MaxRequestBatch
		}

		if consumer.MaxExpires > 0 {
			config.MaxRequestExpires = time.Duration(consumer.MaxExpires) * time.Second
		} else if consumer.MaxRequestExpires > 0 {
			config.MaxRequestExpires = time.Duration(consumer.MaxRequestExpires) * time.Second
		}

		if consumer.MaxBytes > 0 {
			config.MaxRequestMaxBytes = consumer.MaxBytes
		} else if consumer.MaxRequestMaxBytes > 0 {
			config.MaxRequestMaxBytes = consumer.MaxRequestMaxBytes
		}
	}

	if consumer.MaxAckPending > 0 {
		config.MaxAckPending = consumer.MaxAckPending
	}

	if consumer.Mode == "push" || consumer.ConsumerType == models.ConsumerTypePush {
		if consumer.DeliverSubject != "" {
			config.DeliverSubject = consumer.DeliverSubject
		}

		if consumer.DeliverGroup != "" {
			config.DeliverGroup = consumer.DeliverGroup
		}

		config.FlowControl = consumer.FlowControl

		if consumer.IdleHeartbeat > 0 {
			config.IdleHeartbeat = time.Duration(consumer.IdleHeartbeat) * time.Second
		}
	}

	config.HeadersOnly = consumer.HeadersOnly

	if consumer.InactiveThreshold > 0 {
		config.InactiveThreshold = time.Duration(consumer.InactiveThreshold) * time.Second
	}

	if consumer.Replicas > 0 {
		config.Replicas = consumer.Replicas
	}

	config.MemoryStorage = consumer.MemoryStorage

	if len(consumer.Metadata) > 0 {
		config.Metadata = consumer.Metadata
	}

	return config
}

func (s *ConsumerManageService) convertConsumerInfoToStats(info *natsgo.ConsumerInfo) *models.ConsumerStats {
	stats := &models.ConsumerStats{
		StreamName:      info.Stream,
		ConsumerName:    info.Name,
		Created:         models.CustomTime{Time: info.Created},
		DeliveredSeq:    info.Delivered.Consumer,
		DeliveredStream: info.Delivered.Stream,
		AckFloorSeq:     info.AckFloor.Consumer,
		AckFloorStream:  info.AckFloor.Stream,
		NumAckPending:   info.NumAckPending,
		NumRedelivered:  info.NumRedelivered,
		NumWaiting:      info.NumWaiting,
		NumPending:      info.NumPending,
		PushBound:       info.PushBound,
	}

	if info.Delivered.Last != nil && !info.Delivered.Last.IsZero() {
		stats.DeliveredTime = &models.CustomTime{Time: *info.Delivered.Last}
	}

	if info.Cluster != nil {
		replicas := make([]string, len(info.Cluster.Replicas))
		for i, peer := range info.Cluster.Replicas {
			replicas[i] = peer.Name
		}
		stats.Cluster = &models.ClusterInfo{
			Name:     info.Cluster.Name,
			Leader:   info.Cluster.Leader,
			Replicas: replicas,
		}
	}

	return stats
}

func (s *ConsumerManageService) CompareConsumerConfig(id string) (*models.ConsumerConfigDiff, error) {
	consumer, err := s.repo.GetConsumer(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get Consumer: %w", err)
	}
	if consumer == nil {
		return nil, fmt.Errorf("Consumer not found")
	}

	jetStream, err := s.repo.GetJetStream(consumer.JetStreamID)
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream: %w", err)
	}
	if jetStream == nil {
		return nil, fmt.Errorf("JetStream not found: %s", consumer.JetStreamID)
	}

	nc, jcc, err := s.clusterService.GetNatsJetStreamContextWithUsr(jetStream.ClusterID, consumer.NatsOperateUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer nc.Close()

	consumerInfo, err := s.natsService.GetConsumerInfo(jcc, jetStream.Name, consumer.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info from cluster: %w", err)
	}

	dbConfig := s.convertConsumerToConfigView(consumer)
	clusterConfig := s.convertConsumerInfoToConfigView(consumerInfo)

	diff := s.compareConsumerConfigs(dbConfig, clusterConfig)

	return diff, nil
}

func (s *ConsumerManageService) convertConsumerToConfigView(consumer *models.Consumer) *models.ConsumerConfigView {
	return &models.ConsumerConfigView{
		Name:              consumer.Name,
		Durable:           consumer.Durable,
		Description:       consumer.Description,
		DeliverPolicy:     consumer.DeliverPolicy,
		OptStartSeq:       consumer.OptStartSeq,
		OptStartTime:      consumer.OptStartTime,
		AckPolicy:         consumer.AckPolicy,
		AckWait:           consumer.AckWait,
		MaxDeliver:        consumer.MaxDeliver,
		FilterSubject:     consumer.FilterSubject,
		FilterSubjects:    consumer.FilterSubjects,
		ReplayPolicy:      consumer.ReplayPolicy,
		RateLimit:         consumer.RateLimit,
		SampleFrequency:   consumer.SampleFrequency,
		MaxWaiting:        consumer.MaxWaiting,
		MaxAckPending:     consumer.MaxAckPending,
		DeliverSubject:    consumer.DeliverSubject,
		DeliverGroup:      consumer.DeliverGroup,
		FlowControl:       consumer.FlowControl,
		IdleHeartbeat:     consumer.IdleHeartbeat,
		HeadersOnly:       consumer.HeadersOnly,
		InactiveThreshold: consumer.InactiveThreshold,
		Replicas:          consumer.Replicas,
		MemoryStorage:     consumer.MemoryStorage,
	}
}

func (s *ConsumerManageService) convertConsumerInfoToConfigView(info *natsgo.ConsumerInfo) *models.ConsumerConfigView {
	config := info.Config

	var deliverPolicy models.ConsumerDeliverPolicy
	switch config.DeliverPolicy {
	case natsgo.DeliverAllPolicy:
		deliverPolicy = models.DeliverAll
	case natsgo.DeliverLastPolicy:
		deliverPolicy = models.DeliverLast
	case natsgo.DeliverNewPolicy:
		deliverPolicy = models.DeliverNew
	case natsgo.DeliverByStartSequencePolicy:
		deliverPolicy = models.DeliverByStartSeq
	case natsgo.DeliverByStartTimePolicy:
		deliverPolicy = models.DeliverByStartTime
	case natsgo.DeliverLastPerSubjectPolicy:
		deliverPolicy = models.DeliverLastPerSubject
	default:
		deliverPolicy = models.DeliverAll
	}

	var ackPolicy models.ConsumerAckPolicy
	switch config.AckPolicy {
	case natsgo.AckExplicitPolicy:
		ackPolicy = models.AckExplicit
	case natsgo.AckAllPolicy:
		ackPolicy = models.AckAll
	case natsgo.AckNonePolicy:
		ackPolicy = models.AckNone
	default:
		ackPolicy = models.AckExplicit
	}

	var replayPolicy models.ConsumerReplayPolicy
	if config.ReplayPolicy == natsgo.ReplayOriginalPolicy {
		replayPolicy = models.ReplayOriginal
	} else {
		replayPolicy = models.ReplayInstant
	}

	return &models.ConsumerConfigView{
		Name:              config.Name,
		Durable:           config.Durable,
		Description:       config.Description,
		DeliverPolicy:     deliverPolicy,
		OptStartSeq:       config.OptStartSeq,
		OptStartTime:      config.OptStartTime,
		AckPolicy:         ackPolicy,
		AckWait:           int64(config.AckWait.Seconds()),
		MaxDeliver:        config.MaxDeliver,
		FilterSubject:     config.FilterSubject,
		FilterSubjects:    config.FilterSubjects,
		ReplayPolicy:      replayPolicy,
		RateLimit:         config.RateLimit,
		SampleFrequency:   config.SampleFrequency,
		MaxWaiting:        config.MaxWaiting,
		MaxAckPending:     config.MaxAckPending,
		DeliverSubject:    config.DeliverSubject,
		DeliverGroup:      config.DeliverGroup,
		FlowControl:       config.FlowControl,
		IdleHeartbeat:     int64(config.Heartbeat.Seconds()),
		HeadersOnly:       config.HeadersOnly,
		InactiveThreshold: int64(config.InactiveThreshold.Seconds()),
		Replicas:          config.Replicas,
		MemoryStorage:     config.MemoryStorage,
	}
}

func (s *ConsumerManageService) compareConsumerConfigs(dbConfig, clusterConfig *models.ConsumerConfigView) *models.ConsumerConfigDiff {
	var differences []models.ConfigDifference

	if dbConfig.Name != clusterConfig.Name {
		differences = append(differences, models.ConfigDifference{
			Field:         "name",
			DatabaseValue: dbConfig.Name,
			ClusterValue:  clusterConfig.Name,
		})
	}

	if dbConfig.Durable != clusterConfig.Durable {
		differences = append(differences, models.ConfigDifference{
			Field:         "durable",
			DatabaseValue: dbConfig.Durable,
			ClusterValue:  clusterConfig.Durable,
		})
	}

	if dbConfig.Description != clusterConfig.Description {
		differences = append(differences, models.ConfigDifference{
			Field:         "description",
			DatabaseValue: dbConfig.Description,
			ClusterValue:  clusterConfig.Description,
		})
	}

	if dbConfig.DeliverPolicy != clusterConfig.DeliverPolicy {
		differences = append(differences, models.ConfigDifference{
			Field:         "deliver_policy",
			DatabaseValue: dbConfig.DeliverPolicy,
			ClusterValue:  clusterConfig.DeliverPolicy,
		})
	}

	if dbConfig.OptStartSeq != clusterConfig.OptStartSeq {
		differences = append(differences, models.ConfigDifference{
			Field:         "opt_start_seq",
			DatabaseValue: dbConfig.OptStartSeq,
			ClusterValue:  clusterConfig.OptStartSeq,
		})
	}

	if !s.compareTimePointers(dbConfig.OptStartTime, clusterConfig.OptStartTime) {
		differences = append(differences, models.ConfigDifference{
			Field:         "opt_start_time",
			DatabaseValue: dbConfig.OptStartTime,
			ClusterValue:  clusterConfig.OptStartTime,
		})
	}

	if dbConfig.AckPolicy != clusterConfig.AckPolicy {
		differences = append(differences, models.ConfigDifference{
			Field:         "ack_policy",
			DatabaseValue: dbConfig.AckPolicy,
			ClusterValue:  clusterConfig.AckPolicy,
		})
	}

	if dbConfig.AckWait != clusterConfig.AckWait {
		differences = append(differences, models.ConfigDifference{
			Field:         "ack_wait",
			DatabaseValue: dbConfig.AckWait,
			ClusterValue:  clusterConfig.AckWait,
		})
	}

	if dbConfig.MaxDeliver != clusterConfig.MaxDeliver {
		differences = append(differences, models.ConfigDifference{
			Field:         "max_deliver",
			DatabaseValue: dbConfig.MaxDeliver,
			ClusterValue:  clusterConfig.MaxDeliver,
		})
	}

	if dbConfig.FilterSubject != clusterConfig.FilterSubject {
		differences = append(differences, models.ConfigDifference{
			Field:         "filter_subject",
			DatabaseValue: dbConfig.FilterSubject,
			ClusterValue:  clusterConfig.FilterSubject,
		})
	}

	if !s.compareStringSlices(dbConfig.FilterSubjects, clusterConfig.FilterSubjects) {
		differences = append(differences, models.ConfigDifference{
			Field:         "filter_subjects",
			DatabaseValue: dbConfig.FilterSubjects,
			ClusterValue:  clusterConfig.FilterSubjects,
		})
	}

	if dbConfig.ReplayPolicy != clusterConfig.ReplayPolicy {
		differences = append(differences, models.ConfigDifference{
			Field:         "replay_policy",
			DatabaseValue: dbConfig.ReplayPolicy,
			ClusterValue:  clusterConfig.ReplayPolicy,
		})
	}

	if dbConfig.RateLimit != clusterConfig.RateLimit {
		differences = append(differences, models.ConfigDifference{
			Field:         "rate_limit",
			DatabaseValue: dbConfig.RateLimit,
			ClusterValue:  clusterConfig.RateLimit,
		})
	}

	if dbConfig.SampleFrequency != clusterConfig.SampleFrequency {
		differences = append(differences, models.ConfigDifference{
			Field:         "sample_frequency",
			DatabaseValue: dbConfig.SampleFrequency,
			ClusterValue:  clusterConfig.SampleFrequency,
		})
	}

	if dbConfig.MaxWaiting != clusterConfig.MaxWaiting {
		differences = append(differences, models.ConfigDifference{
			Field:         "max_waiting",
			DatabaseValue: dbConfig.MaxWaiting,
			ClusterValue:  clusterConfig.MaxWaiting,
		})
	}

	if dbConfig.MaxAckPending != clusterConfig.MaxAckPending {
		differences = append(differences, models.ConfigDifference{
			Field:         "max_ack_pending",
			DatabaseValue: dbConfig.MaxAckPending,
			ClusterValue:  clusterConfig.MaxAckPending,
		})
	}

	if dbConfig.DeliverSubject != clusterConfig.DeliverSubject {
		differences = append(differences, models.ConfigDifference{
			Field:         "deliver_subject",
			DatabaseValue: dbConfig.DeliverSubject,
			ClusterValue:  clusterConfig.DeliverSubject,
		})
	}

	if dbConfig.DeliverGroup != clusterConfig.DeliverGroup {
		differences = append(differences, models.ConfigDifference{
			Field:         "deliver_group",
			DatabaseValue: dbConfig.DeliverGroup,
			ClusterValue:  clusterConfig.DeliverGroup,
		})
	}

	if dbConfig.FlowControl != clusterConfig.FlowControl {
		differences = append(differences, models.ConfigDifference{
			Field:         "flow_control",
			DatabaseValue: dbConfig.FlowControl,
			ClusterValue:  clusterConfig.FlowControl,
		})
	}

	if dbConfig.IdleHeartbeat != clusterConfig.IdleHeartbeat {
		differences = append(differences, models.ConfigDifference{
			Field:         "idle_heartbeat",
			DatabaseValue: dbConfig.IdleHeartbeat,
			ClusterValue:  clusterConfig.IdleHeartbeat,
		})
	}

	if dbConfig.HeadersOnly != clusterConfig.HeadersOnly {
		differences = append(differences, models.ConfigDifference{
			Field:         "headers_only",
			DatabaseValue: dbConfig.HeadersOnly,
			ClusterValue:  clusterConfig.HeadersOnly,
		})
	}

	if dbConfig.InactiveThreshold != clusterConfig.InactiveThreshold {
		differences = append(differences, models.ConfigDifference{
			Field:         "inactive_threshold",
			DatabaseValue: dbConfig.InactiveThreshold,
			ClusterValue:  clusterConfig.InactiveThreshold,
		})
	}

	if dbConfig.Replicas != clusterConfig.Replicas {
		differences = append(differences, models.ConfigDifference{
			Field:         "replicas",
			DatabaseValue: dbConfig.Replicas,
			ClusterValue:  clusterConfig.Replicas,
		})
	}

	if dbConfig.MemoryStorage != clusterConfig.MemoryStorage {
		differences = append(differences, models.ConfigDifference{
			Field:         "memory_storage",
			DatabaseValue: dbConfig.MemoryStorage,
			ClusterValue:  clusterConfig.MemoryStorage,
		})
	}

	return &models.ConsumerConfigDiff{
		HasDifference:  len(differences) > 0,
		DatabaseConfig: dbConfig,
		ClusterConfig:  clusterConfig,
		Differences:    differences,
	}
}

func (s *ConsumerManageService) compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *ConsumerManageService) compareTimePointers(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}
