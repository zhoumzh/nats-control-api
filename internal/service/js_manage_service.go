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

	jsApi "github.com/nats-io/nats.go/jetstream"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

// JetStreamManageService 使用新架构的JetStream服务
type JetStreamManageService struct {
	repo                  *db.Repository
	config                *config.Config
	clusterService        *ClusterService
	clusterMonitorService *ClusterMonitorService
	jetStreamService      *nats.Service
	natsManager           *jwt.NATSManager
}

// NewJetStreamManagerService 创建使用新架构的JetStream服务
func NewJetStreamManagerService(repo *db.Repository, config *config.Config, natsServer *nats.Service, clusterService *ClusterService, clusterMonitorService *ClusterMonitorService, natsManager *jwt.NATSManager) *JetStreamManageService {
	return &JetStreamManageService{
		repo:                  repo,
		config:                config,
		clusterService:        clusterService,
		clusterMonitorService: clusterMonitorService,
		jetStreamService:      natsServer,
		natsManager:           natsManager,
	}
}

// validateReplicaCount 校验副本数不能超过集群节点数
func (s *JetStreamManageService) validateReplicaCount(clusterID string, replicas int) error {
	if replicas <= 0 {
		return nil // 不需要校验
	}

	nodeCount, err := s.clusterMonitorService.GetClusterNodeCount(clusterID)
	if err != nil {
		return fmt.Errorf("获取集群节点数失败: %w", err)
	}

	if replicas > nodeCount {
		return fmt.Errorf("副本数(%d)不能超过集群节点数(%d)", replicas, nodeCount)
	}

	return nil
}

// CreateJetStream 创建新的JetStream（使用新架构）
func (s *JetStreamManageService) CreateJetStream(req *models.CreateJetStreamRequest) (*models.JetStream, error) {
	log.WithContext(context.Background()).Infof("使用新架构启动JetStream创建进程: stream_name=%s, nats_operate_user_id=%s, cluster_id=%s, subjects=%v", req.Name, req.NatsOperateUserID, req.ClusterID, req.Subjects)

	// 验证请求
	if err := req.Validate(); err != nil {
		log.WithContext(context.Background()).Errorf("JetStream请求验证失败: %v", err)
		return nil, err
	}

	// 获取NATS操作用户信息
	user, err := s.repo.GetUser(req.NatsOperateUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get nats operate user: %w", err)
	}
	if user == nil {
		return nil, fmt.Errorf("nats operate user not found: %s", req.NatsOperateUserID)
	}

	// 检查名称唯一性 - 按集群维度校验
	existing, err := s.repo.GetJetStreamByNameAndCluster(req.Name, req.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to check name uniqueness: %w", err)
	}
	if existing != nil {
		return nil, fmt.Errorf("jetstream with name '%s' already exists in cluster '%s'", req.Name, req.ClusterID)
	}

	// 校验副本数不能超过集群节点数
	if err := s.validateReplicaCount(req.ClusterID, req.Replicas); err != nil {
		return nil, err
	}

	// 1. 先保存到数据库（同步状态为'pending'）
	js := &models.JetStream{
		ID:                models.GenID(models.JetStreamObjType),
		Name:              req.Name,
		ClusterID:         req.ClusterID,
		NatsOperateUserID: req.NatsOperateUserID,
		Subjects:          req.Subjects,
		MaxMsgs:           req.MaxMsgs,
		MaxBytesValue:     req.MaxBytesValue,
		MaxBytesUnit:      req.MaxBytesUnit,
		MaxAge:            req.MaxAge,
		MaxMsgSizeValue:   req.MaxMsgSizeValue,
		MaxMsgSizeUnit:    req.MaxMsgSizeUnit,
		Storage:           req.Storage,
		Replicas:          req.Replicas,
		Description:       req.Description,
		Retention:         req.Retention,
		Discard:           req.Discard,
		Compression:       req.Compression,
		MaxConsumers:      req.MaxConsumers,
		NoAck:             req.NoAck,
		DuplicateWindow:   req.DuplicateWindow,
		AllowRollupHdrs:   req.AllowRollupHdrs,
		AllowDirect:       req.AllowDirect,
		MirrorDirect:      req.MirrorDirect,
		DenyDelete:        req.DenyDelete,
		DenyPurge:         req.DenyPurge,
		PlacementCluster:  req.PlacementCluster,
		PlacementTags:     req.PlacementTags,
		Metadata:          req.Metadata,
		Status:            "active",
		SyncStatus:        models.JetStreamSyncPending, // 待同步
		SyncMessage:       "等待同步到NATS服务器",
		CreatedAt:         models.CustomTime{Time: time.Now()},
		UpdatedAt:         models.CustomTime{Time: time.Now()},
	}

	// 保存到数据库
	if err := s.repo.CreateJetStream(js); err != nil {
		return nil, fmt.Errorf("failed to save jetstream to database: %w", err)
	}

	// 2. 启动异步goroutine执行NATS操作
	go s.asyncCreateStreamOnCluster(js)

	log.WithContext(context.Background()).Infof("成功创建 JetStream 记录，异步同步中: jetstream_id=%s", js.ID)
	return js, nil
}

// RetryJetStreamCreation retries creating a failed JetStream
func (s *JetStreamManageService) RetryJetStreamCreation(id string, userID string) (*models.JetStream, error) {
	// Get JetStream from database
	js, err := s.repo.GetJetStream(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream: %w", err)
	}
	if js == nil {
		return nil, fmt.Errorf("JetStream not found: %s", id)
	}

	log.WithContext(context.Background()).Infof("JetStream创建重试: jetstream_id=%s, stream_name=%s, cluster_id=%s, user_id=%s", id, js.Name, js.ClusterID, userID)

	// Create stream configuration from saved JetStream
	streamConfig := jsApi.StreamConfig{
		Name:     js.Name,
		Subjects: js.Subjects,
		MaxMsgs:  js.MaxMsgs,
		MaxBytes: models.ConvertToBytes(js.MaxBytesValue, js.MaxBytesUnit), // 使用转换函数
		Replicas: int(js.Replicas),
	}

	if js.MaxAge > 0 {
		streamConfig.MaxAge = time.Duration(js.MaxAge) * time.Second
	}

	switch strings.ToLower(string(js.Storage)) {
	case "file":
		streamConfig.Storage = jsApi.FileStorage
	case "memory":
		streamConfig.Storage = jsApi.MemoryStorage
	default:
		streamConfig.Storage = jsApi.FileStorage
	}

	// Try to create stream on cluster
	err = s.createStreamOnCluster(js.ClusterID, userID, streamConfig)
	if err != nil {
		log.WithContext(context.Background()).Errorf("JetStream创建重试失败: %v", err)
		return nil, fmt.Errorf("failed to retry stream creation on cluster: %w", err)
	}

	// Update status to active
	js.Status = "active"
	js.UpdatedAt = models.CustomTime{Time: time.Now()}
	if err := s.repo.UpdateJetStream(js); err != nil {
		return nil, fmt.Errorf("failed to update JetStream status after retry: %w", err)
	}

	log.WithContext(context.Background()).Infof("JetStream创建重试成功: jetstream_id=%s", id)
	return js, nil
}

// DeleteJetStream 删除JetStream（使用新架构）
func (s *JetStreamManageService) DeleteJetStream(id string) error {
	log.WithContext(context.Background()).Infof("使用新架构启动JetStream删除: jetstream_id=%s", id)

	// 获取JetStream信息
	js, err := s.repo.GetJetStream(id)
	if err != nil {
		return fmt.Errorf("failed to get JetStream: %w", err)
	}
	if js == nil {
		return fmt.Errorf("JetStream not found: %s", id)
	}

	// 从数据库删除记录
	if err := s.repo.DeleteJetStream(id); err != nil {
		return fmt.Errorf("failed to delete JetStream from database: %w", err)
	}

	log.WithContext(context.Background()).Infof("使用新架构成功删除JetStream: jetstream_id=%s", id)
	return nil
}

// createStreamOnCluster 在集群上创建流
func (s *JetStreamManageService) createStreamOnCluster(clusterID, userID string, config jsApi.StreamConfig) error {
	// 使用指定用户身份获取JetStream上下文
	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(clusterID, userID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer s.clusterService.CloseConnection(conn)

	// 使用JetStream服务创建流
	_, err = s.jetStreamService.CreateStream(js, config)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}

// deleteStreamOnCluster 在集群上删除流
func (s *JetStreamManageService) deleteStreamOnCluster(clusterID, userID, streamName string) error {
	// 使用指定用户身份获取JetStream上下文
	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(clusterID, userID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer s.clusterService.CloseConnection(conn)

	// 使用JetStream服务删除流
	err = s.jetStreamService.DeleteStream(js, streamName)
	if err != nil {
		return fmt.Errorf("failed to delete stream: %w", err)
	}

	return nil
}

// updateStreamOnCluster 在集群上更新流
func (s *JetStreamManageService) updateStreamOnCluster(jetStream *models.JetStream) error {
	// Use the comprehensive buildStreamConfig function instead of building config manually
	streamConfig := s.buildStreamConfig(jetStream)

	// 使用NATS操作用户身份获取JetStream上下文
	conn, js, err := s.clusterService.GetNatsJetStreamWithUser(jetStream.ClusterID, jetStream.NatsOperateUserID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer s.clusterService.CloseConnection(conn)

	// 使用JetStream服务更新流
	_, err = s.jetStreamService.UpdateStream(js, streamConfig)
	if err != nil {
		return fmt.Errorf("failed to update stream: %w", err)
	}

	return nil
}

// ListJetStreams lists JetStreams with filtering and pagination
func (s *JetStreamManageService) ListJetStreams(req *models.JetStreamListRequest) ([]*models.JetStream, int64, error) {
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	offset := 0
	if req.Page > 1 {
		offset = (req.Page - 1) * limit
	}

	return s.repo.ListJetStreams(req.NatsOperateUserID, req.Status, req.Search, req.ClusterID, req.SyncStatus, limit, offset)
}

// GetJetStream gets a JetStream by ID with live statistics
func (s *JetStreamManageService) GetJetStream(id string) (*models.JetStream, error) {
	// Get JetStream from database
	jetStream, err := s.repo.GetJetStream(id)
	if err != nil {
		return nil, err
	}
	if jetStream == nil {
		return nil, fmt.Errorf("JetStream not found: %s", id)
	}

	// Try to get live statistics from NATS cluster
	streamInfo, err := s.clusterService.GetStreamInfo(jetStream.ClusterID, jetStream.NatsOperateUserID, jetStream.Name)
	if err != nil {
		log.WithContext(context.Background()).Warnf("Failed to get live statistics for JetStream %s: %v", id, err)
		// Return database data without live statistics
		return jetStream, nil
	}

	// Merge live statistics into JetStream object
	s.mergeStreamInfoToJetStream(jetStream, streamInfo)

	return jetStream, nil
}

// UpdateJetStream updates a JetStream
func (s *JetStreamManageService) UpdateJetStreamDirect(jetStream *models.JetStream) error {
	// Update in database first
	if err := s.repo.UpdateJetStream(jetStream); err != nil {
		return fmt.Errorf("failed to update JetStream in database: %w", err)
	}

	// Update the actual JetStream on NATS server
	err := s.updateStreamOnCluster(jetStream)
	if err != nil {
		log.WithContext(context.Background()).Errorf("NATS集群上流更新失败: %v", err)
		// Continue even if NATS update fails, as database is already updated
		// This allows for eventual consistency and retry mechanisms
	}

	log.WithContext(context.Background()).Infof("JetStream更新成功: jetstream_id=%s", jetStream.ID)
	return nil
}

// CompareJetStreamConfig 获取当前流在目标集群上的状态，对比数据库中的配置，返回diff信息
func (s *JetStreamManageService) CompareJetStreamConfig(id string) (*models.JetStreamConfigDiff, error) {
	// Get JetStream from database
	js, err := s.repo.GetJetStream(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream: %w", err)
	}
	if js == nil {
		return nil, fmt.Errorf("JetStream not found: %s", id)
	}

	// Get stream info from NATS cluster
	streamInfo, err := s.clusterService.GetStreamInfo(js.ClusterID, js.NatsOperateUserID, js.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info from cluster: %w", err)
	}

	// Convert database config to StreamConfig
	dbConfig := s.convertJetStreamToStreamConfig(js)

	// Convert cluster config to StreamConfig
	clusterConfig := s.convertStreamInfoToStreamConfig(streamInfo)

	// Compare configurations and generate diff
	diff := s.compareStreamConfigs(dbConfig, clusterConfig)

	return diff, nil
}

// GetJetStreamStats gets live statistics from NATS cluster
func (s *JetStreamManageService) GetJetStreamStats(id string, userID string) (*jsApi.StreamInfo, error) {
	// Get JetStream from database
	js, err := s.repo.GetJetStream(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream: %w", err)
	}
	if js == nil {
		return nil, fmt.Errorf("JetStream not found: %s", id)
	}

	// Get live statistics from cluster
	return s.clusterService.GetStreamInfo(js.ClusterID, userID, js.Name)
}

// DeleteJetStreamFromCluster deletes JetStream from cluster only (not from database)
func (s *JetStreamManageService) DeleteJetStreamFromCluster(jetStreamID, creatorUserID string) error {
	log.WithContext(context.Background()).Infof("开始从集群删除JetStream: jetstream_id=%s, creator_user_id=%s", jetStreamID, creatorUserID)

	// Get JetStream from database
	jetStream, err := s.repo.GetJetStream(jetStreamID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream: %w", err)
	}
	if jetStream == nil {
		return fmt.Errorf("JetStream not found")
	}

	// Get the creator user information to find accountID
	creatorUser, err := s.repo.GetUser(creatorUserID)
	if err != nil {
		return fmt.Errorf("failed to get creator user: %w", err)
	}
	if creatorUser == nil {
		return fmt.Errorf("creator user not found")
	}

	// Get first admin user by accountID to perform the deletion
	adminUser, err := s.repo.GetFirstAdminUserByAccountID(creatorUser.AccountID)
	if err != nil {
		return fmt.Errorf("failed to get admin user for account %s: %w", creatorUser.AccountID, err)
	}
	if adminUser == nil {
		return fmt.Errorf("no admin user found for account %s", creatorUser.AccountID)
	}

	// Delete from NATS cluster using the admin user
	err = s.deleteStreamOnCluster(jetStream.ClusterID, adminUser.ID, jetStream.Name)
	if err != nil {
		return fmt.Errorf("failed to delete stream from cluster: %w", err)
	}

	log.WithContext(context.Background()).Infof("JetStream从集群删除成功: jetstream_id=%s, jetstream_name=%s, admin_user_id=%s", jetStreamID, jetStream.Name, adminUser.ID)
	return nil
}

// asyncCreateStreamOnCluster 异步在集群上创建流
func (s *JetStreamManageService) asyncCreateStreamOnCluster(js *models.JetStream) {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始异步创建流到NATS集群: jetstream_id=%s, stream_name=%s", js.ID, js.Name)

	// 创建流配置
	streamConfig := s.buildStreamConfig(js)

	// 尝试在集群上创建流
	err := s.createStreamOnCluster(js.ClusterID, js.NatsOperateUserID, streamConfig)

	// 更新同步状态
	if err != nil {
		// 同步失败
		log.WithContext(ctx).Errorf("异步创建流失败: jetstream_id=%s, error=%v", js.ID, err)
		updateErr := s.repo.UpdateJetStreamFields(js.ID, map[string]interface{}{
			"sync_status":  models.JetStreamSyncFailed,
			"sync_message": fmt.Sprintf("创建流失败: %s", err.Error()),
			"updated_at":   models.CustomTime{Time: time.Now()},
		})
		if updateErr != nil {
			log.WithContext(ctx).Errorf("更新同步状态失败: jetstream_id=%s, error=%v", js.ID, updateErr)
		}
	} else {
		// 同步成功
		log.WithContext(ctx).Infof("异步创建流成功: jetstream_id=%s", js.ID)
		updateErr := s.repo.UpdateJetStreamFields(js.ID, map[string]interface{}{
			"sync_status":  models.JetStreamSyncSynced,
			"sync_message": "成功同步到NATS服务器",
			"updated_at":   models.CustomTime{Time: time.Now()},
		})
		if updateErr != nil {
			log.WithContext(ctx).Errorf("更新同步状态失败: jetstream_id=%s, error=%v", js.ID, updateErr)
		}
	}
}

// asyncUpdateStreamOnCluster 异步在集群上更新流
func (s *JetStreamManageService) asyncUpdateStreamOnCluster(js *models.JetStream) {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始异步更新流到NATS集群: jetstream_id=%s, stream_name=%s", js.ID, js.Name)

	// 尝试在集群上更新流
	err := s.updateStreamOnCluster(js)

	// 更新同步状态
	if err != nil {
		// 同步失败
		log.WithContext(ctx).Errorf("异步更新流失败: jetstream_id=%s, error=%v", js.ID, err)
		updateErr := s.repo.UpdateJetStreamFields(js.ID, map[string]interface{}{
			"sync_status":  models.JetStreamSyncFailed,
			"sync_message": fmt.Sprintf("更新流失败: %s", err.Error()),
			"updated_at":   models.CustomTime{Time: time.Now()},
		})
		if updateErr != nil {
			log.WithContext(ctx).Errorf("更新同步状态失败: jetstream_id=%s, error=%v", js.ID, updateErr)
		}
	} else {
		// 同步成功
		log.WithContext(ctx).Infof("异步更新流成功: jetstream_id=%s", js.ID)
		updateErr := s.repo.UpdateJetStreamFields(js.ID, map[string]interface{}{
			"sync_status":  models.JetStreamSyncSynced,
			"sync_message": "成功同步到NATS服务器",
			"updated_at":   models.CustomTime{Time: time.Now()},
		})
		if updateErr != nil {
			log.WithContext(ctx).Errorf("更新同步状态失败: jetstream_id=%s, error=%v", js.ID, updateErr)
		}
	}
}

// buildStreamConfig 根据JetStream模型构建流配置
func (s *JetStreamManageService) buildStreamConfig(js *models.JetStream) jsApi.StreamConfig {
	streamConfig := jsApi.StreamConfig{
		Name:        js.Name,
		Subjects:    js.Subjects,
		Description: js.Description,
	}

	// 设置基础策略配置
	switch strings.ToLower(string(js.Retention)) {
	case "limits":
		streamConfig.Retention = jsApi.LimitsPolicy
	case "interest":
		streamConfig.Retention = jsApi.InterestPolicy
	case "workqueue":
		streamConfig.Retention = jsApi.WorkQueuePolicy
	default:
		streamConfig.Retention = jsApi.LimitsPolicy
	}

	switch strings.ToLower(string(js.Discard)) {
	case "old":
		streamConfig.Discard = jsApi.DiscardOld
	case "new":
		streamConfig.Discard = jsApi.DiscardNew
	default:
		streamConfig.Discard = jsApi.DiscardOld
	}

	switch strings.ToLower(string(js.Compression)) {
	case "s2":
		streamConfig.Compression = jsApi.S2Compression
	case "none":
		streamConfig.Compression = jsApi.NoCompression
	default:
		streamConfig.Compression = jsApi.NoCompression
	}

	// 设置存储类型
	switch strings.ToLower(string(js.Storage)) {
	case "file":
		streamConfig.Storage = jsApi.FileStorage
	case "memory":
		streamConfig.Storage = jsApi.MemoryStorage
	default:
		streamConfig.Storage = jsApi.FileStorage
	}

	// 设置限制配置
	if js.MaxMsgs > 0 {
		streamConfig.MaxMsgs = js.MaxMsgs
	}
	if js.MaxBytesValue > 0 {
		streamConfig.MaxBytes = models.ConvertToBytes(js.MaxBytesValue, js.MaxBytesUnit)
	}
	if js.MaxAge > 0 {
		streamConfig.MaxAge = time.Duration(js.MaxAge) * time.Second
	}
	if js.MaxMsgSizeValue > 0 {
		streamConfig.MaxMsgSize = int32(models.ConvertToBytes(js.MaxMsgSizeValue, js.MaxMsgSizeUnit))
	}
	if js.MaxConsumers > 0 {
		streamConfig.MaxConsumers = js.MaxConsumers
	}
	if js.MaxMsgsPerSubject > 0 {
		streamConfig.MaxMsgsPerSubject = js.MaxMsgsPerSubject
	}

	// 设置副本数量
	if js.Replicas > 0 {
		streamConfig.Replicas = js.Replicas
	}

	// 设置高级配置选项
	streamConfig.NoAck = js.NoAck
	streamConfig.DiscardNewPerSubject = js.DiscardNewPerSubject
	streamConfig.Sealed = js.Sealed

	// 设置重复检测窗口
	if js.DuplicateWindow > 0 {
		streamConfig.Duplicates = time.Duration(js.DuplicateWindow) * time.Second
	}

	// 设置权限和直接访问配置
	streamConfig.AllowRollup = js.AllowRollupHdrs
	streamConfig.AllowDirect = js.AllowDirect
	streamConfig.MirrorDirect = js.MirrorDirect
	streamConfig.DenyDelete = js.DenyDelete
	streamConfig.DenyPurge = js.DenyPurge

	// 设置放置配置
	if js.PlacementCluster != "" || len(js.PlacementTags) > 0 {
		placement := &jsApi.Placement{}
		if js.PlacementCluster != "" {
			placement.Cluster = js.PlacementCluster
		}
		if len(js.PlacementTags) > 0 {
			placement.Tags = js.PlacementTags
		}
		streamConfig.Placement = placement
	}

	// 设置元数据
	if len(js.Metadata) > 0 {
		streamConfig.Metadata = js.Metadata
	}

	// 设置FirstSeq（如果是从统计信息中设置的话）
	if js.FirstSeq > 0 {
		streamConfig.FirstSeq = js.FirstSeq
	}

	return streamConfig
}

// RetryFailedSync 重试失败的同步操作
func (s *JetStreamManageService) RetryFailedSync(jetstreamID string) error {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始重试失败的同步: jetstream_id=%s", jetstreamID)

	// 获取JetStream记录
	js, err := s.repo.GetJetStream(jetstreamID)
	if err != nil {
		return fmt.Errorf("failed to get jetstream: %w", err)
	}
	if js == nil {
		return fmt.Errorf("jetstream not found: %s", jetstreamID)
	}

	// 检查是否需要重试
	if js.SyncStatus != models.JetStreamSyncFailed {
		return fmt.Errorf("jetstream sync status is not failed: %s", js.SyncStatus)
	}

	// 获取NATS连接检查stream是否存在
	natsConn, jsCtx, err := s.clusterService.GetNatsJetStreamWithUser(js.ClusterID, js.NatsOperateUserID)
	if err != nil {
		return fmt.Errorf("failed to get NATS connection: %w", err)
	}
	defer natsConn.Close()

	// 检查stream是否存在于NATS服务器上
	_, err = s.jetStreamService.GetStreamInfo(jsCtx, js.Name)
	streamExists := err == nil

	// 重置同步状态为pending
	err = s.repo.UpdateJetStreamFields(jetstreamID, map[string]interface{}{
		"sync_status":  models.JetStreamSyncPending,
		"sync_message": "重试同步中...",
		"updated_at":   models.CustomTime{Time: time.Now()},
	})
	if err != nil {
		return fmt.Errorf("failed to update sync status: %w", err)
	}

	// 根据stream是否存在选择操作类型
	if streamExists {
		log.WithContext(ctx).Infof("Stream存在，执行更新重试: jetstream_id=%s, stream_name=%s", jetstreamID, js.Name)
		go s.asyncUpdateStreamOnCluster(js)
	} else {
		log.WithContext(ctx).Infof("Stream不存在，执行创建重试: jetstream_id=%s, stream_name=%s", jetstreamID, js.Name)
		go s.asyncCreateStreamOnCluster(js)
	}

	log.WithContext(ctx).Infof("成功启动重试同步: jetstream_id=%s", jetstreamID)
	return nil
}

// UpdateJetStream 更新JetStream配置
func (s *JetStreamManageService) UpdateJetStream(id string, req *models.UpdateJetStreamRequest) (*models.JetStream, error) {
	ctx := context.Background()
	log.WithContext(ctx).Infof("开始更新JetStream: jetstream_id=%s, nats_operate_user_id=%s", id, req.NatsOperateUserID)

	// 获取现有记录
	js, err := s.repo.GetJetStream(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get jetstream: %w", err)
	}
	if js == nil {
		return nil, fmt.Errorf("jetstream not found: %s", id)
	}

	// 更新字段
	if req.Description != "" {
		js.Description = req.Description
	}
	if req.NatsOperateUserID != "" {
		js.NatsOperateUserID = req.NatsOperateUserID
	}
	if len(req.Subjects) > 0 {
		js.Subjects = req.Subjects
	}
	if req.Storage != "" {
		js.Storage = req.Storage
	}
	if req.MaxMsgs != nil {
		js.MaxMsgs = *req.MaxMsgs
	}
	if req.MaxBytesValue != nil {
		js.MaxBytesValue = *req.MaxBytesValue
	}
	if req.MaxBytesUnit != nil {
		js.MaxBytesUnit = *req.MaxBytesUnit
	}
	if req.MaxAge != nil {
		js.MaxAge = *req.MaxAge
	}
	if req.MaxMsgSizeValue != nil {
		js.MaxMsgSizeValue = *req.MaxMsgSizeValue
	}
	if req.MaxMsgSizeUnit != nil {
		js.MaxMsgSizeUnit = *req.MaxMsgSizeUnit
	}
	if req.MaxConsumers != nil {
		js.MaxConsumers = *req.MaxConsumers
	}
	if req.Replicas != nil {
		// 校验副本数不能超过集群节点数
		if err := s.validateReplicaCount(js.ClusterID, *req.Replicas); err != nil {
			return nil, err
		}
		js.Replicas = *req.Replicas
	}
	if req.Retention != "" {
		js.Retention = req.Retention
	}
	if req.Discard != "" {
		js.Discard = req.Discard
	}
	if req.DuplicateWindow != 0 {
		js.DuplicateWindow = req.DuplicateWindow
	}
	if req.NoAck != nil {
		js.NoAck = *req.NoAck
	}
	if req.AllowRollupHdrs != nil {
		js.AllowRollupHdrs = *req.AllowRollupHdrs
	}
	if req.AllowDirect != nil {
		js.AllowDirect = *req.AllowDirect
	}
	if req.MirrorDirect != nil {
		js.MirrorDirect = *req.MirrorDirect
	}
	if req.DenyDelete != nil {
		js.DenyDelete = *req.DenyDelete
	}
	if req.DenyPurge != nil {
		js.DenyPurge = *req.DenyPurge
	}

	// 处理 metadata 字段
	if req.Metadata != nil {
		js.Metadata = req.Metadata
	}

	// 处理 placement_tags 字段
	js.PlacementTags = req.PlacementTags

	// 设置同步状态为pending
	js.SyncStatus = models.JetStreamSyncPending
	js.SyncMessage = "等待同步更新到NATS服务器"
	js.UpdatedAt = models.CustomTime{Time: time.Now()}

	// 先更新数据库
	if err := s.repo.UpdateJetStream(js); err != nil {
		return nil, fmt.Errorf("failed to update jetstream in database: %w", err)
	}

	// 异步更新到集群
	go s.asyncUpdateStreamOnCluster(js)

	log.WithContext(ctx).Infof("成功更新JetStream记录，异步同步中: jetstream_id=%s", id)
	return js, nil
}

// convertJetStreamToStreamConfig 将数据库中的JetStream转换为StreamConfig
func (s *JetStreamManageService) convertJetStreamToStreamConfig(js *models.JetStream) *models.StreamConfig {
	return &models.StreamConfig{
		Name:            js.Name,
		Subjects:        js.Subjects,
		Storage:         js.Storage,
		Retention:       js.Retention,
		Discard:         js.Discard,
		Compression:     js.Compression,
		MaxMsgs:         js.MaxMsgs,
		MaxBytesValue:   js.MaxBytesValue,
		MaxBytesUnit:    js.MaxBytesUnit,
		MaxAge:          js.MaxAge,
		Replicas:        js.Replicas,
		NoAck:           js.NoAck,
		AllowDirect:     js.AllowDirect,
		AllowRollupHdrs: js.AllowRollupHdrs,
		DenyDelete:      js.DenyDelete,
		DenyPurge:       js.DenyPurge,
		DuplicateWindow: js.DuplicateWindow,
	}
}

// convertStreamInfoToStreamConfig 将NATS StreamInfo转换为StreamConfig
func (s *JetStreamManageService) convertStreamInfoToStreamConfig(info *jsApi.StreamInfo) *models.StreamConfig {
	maxBytesValue, maxBytesUnit := models.ConvertFromBytes(info.Config.MaxBytes)

	// 转换存储类型
	var storage models.JetStreamStorageType
	switch info.Config.Storage {
	case jsApi.FileStorage:
		storage = models.JetStreamStorageFile
	case jsApi.MemoryStorage:
		storage = models.JetStreamStorageMemory
	default:
		storage = models.JetStreamStorageFile
	}

	// 转换保留策略
	var retention models.JetStreamRetentionPolicy
	switch info.Config.Retention {
	case jsApi.LimitsPolicy:
		retention = models.JetStreamRetentionLimits
	case jsApi.InterestPolicy:
		retention = models.JetStreamRetentionInterest
	case jsApi.WorkQueuePolicy:
		retention = models.JetStreamRetentionWorkQueue
	default:
		retention = models.JetStreamRetentionLimits
	}

	// 转换丢弃策略
	var discard models.JetStreamDiscardPolicy
	switch info.Config.Discard {
	case jsApi.DiscardOld:
		discard = models.JetStreamDiscardOld
	case jsApi.DiscardNew:
		discard = models.JetStreamDiscardNew
	default:
		discard = models.JetStreamDiscardOld
	}

	// 转换压缩类型
	var compression models.JetStreamCompressionType
	switch info.Config.Compression {
	case jsApi.NoCompression:
		compression = models.JetStreamCompressionNone
	case jsApi.S2Compression:
		compression = models.JetStreamCompressionS2
	default:
		compression = models.JetStreamCompressionNone
	}

	return &models.StreamConfig{
		Name:            info.Config.Name,
		Subjects:        info.Config.Subjects,
		Storage:         storage,
		Retention:       retention,
		Discard:         discard,
		Compression:     compression,
		MaxMsgs:         info.Config.MaxMsgs,
		MaxBytesValue:   maxBytesValue,
		MaxBytesUnit:    maxBytesUnit,
		MaxAge:          int64(info.Config.MaxAge.Seconds()),
		Replicas:        info.Config.Replicas,
		NoAck:           info.Config.NoAck,
		AllowDirect:     info.Config.AllowDirect,
		AllowRollupHdrs: info.Config.AllowRollup,
		DenyDelete:      info.Config.DenyDelete,
		DenyPurge:       info.Config.DenyPurge,
		DuplicateWindow: int64(info.Config.Duplicates.Seconds()),
	}
}

// compareStreamConfigs 对比两个StreamConfig并返回差异
func (s *JetStreamManageService) compareStreamConfigs(dbConfig, clusterConfig *models.StreamConfig) *models.JetStreamConfigDiff {
	var differences []models.ConfigDifference

	// 比较各个字段
	if dbConfig.Name != clusterConfig.Name {
		differences = append(differences, models.ConfigDifference{
			Field:         "name",
			DatabaseValue: dbConfig.Name,
			ClusterValue:  clusterConfig.Name,
		})
	}

	// 比较Subjects（切片比较）
	if !s.compareStringSlices(dbConfig.Subjects, clusterConfig.Subjects) {
		differences = append(differences, models.ConfigDifference{
			Field:         "subjects",
			DatabaseValue: dbConfig.Subjects,
			ClusterValue:  clusterConfig.Subjects,
		})
	}

	if dbConfig.Storage != clusterConfig.Storage {
		differences = append(differences, models.ConfigDifference{
			Field:         "storage",
			DatabaseValue: dbConfig.Storage,
			ClusterValue:  clusterConfig.Storage,
		})
	}

	if dbConfig.Retention != clusterConfig.Retention {
		differences = append(differences, models.ConfigDifference{
			Field:         "retention",
			DatabaseValue: dbConfig.Retention,
			ClusterValue:  clusterConfig.Retention,
		})
	}

	if dbConfig.Discard != clusterConfig.Discard {
		differences = append(differences, models.ConfigDifference{
			Field:         "discard",
			DatabaseValue: dbConfig.Discard,
			ClusterValue:  clusterConfig.Discard,
		})
	}

	if dbConfig.Compression != clusterConfig.Compression {
		differences = append(differences, models.ConfigDifference{
			Field:         "compression",
			DatabaseValue: dbConfig.Compression,
			ClusterValue:  clusterConfig.Compression,
		})
	}

	if dbConfig.MaxMsgs != clusterConfig.MaxMsgs {
		differences = append(differences, models.ConfigDifference{
			Field:         "max_msgs",
			DatabaseValue: dbConfig.MaxMsgs,
			ClusterValue:  clusterConfig.MaxMsgs,
		})
	}

	// 比较字节数（转换为相同单位进行比较）
	dbBytes := models.ConvertToBytes(dbConfig.MaxBytesValue, dbConfig.MaxBytesUnit)
	clusterBytes := models.ConvertToBytes(clusterConfig.MaxBytesValue, clusterConfig.MaxBytesUnit)
	if dbBytes != clusterBytes {
		differences = append(differences, models.ConfigDifference{
			Field:         "max_bytes",
			DatabaseValue: fmt.Sprintf("%.3f %s", dbConfig.MaxBytesValue, dbConfig.MaxBytesUnit),
			ClusterValue:  fmt.Sprintf("%.3f %s", clusterConfig.MaxBytesValue, clusterConfig.MaxBytesUnit),
		})
	}

	if dbConfig.MaxAge != clusterConfig.MaxAge {
		differences = append(differences, models.ConfigDifference{
			Field:         "max_age",
			DatabaseValue: dbConfig.MaxAge,
			ClusterValue:  clusterConfig.MaxAge,
		})
	}

	if dbConfig.Replicas != clusterConfig.Replicas {
		differences = append(differences, models.ConfigDifference{
			Field:         "replicas",
			DatabaseValue: dbConfig.Replicas,
			ClusterValue:  clusterConfig.Replicas,
		})
	}

	if dbConfig.NoAck != clusterConfig.NoAck {
		differences = append(differences, models.ConfigDifference{
			Field:         "no_ack",
			DatabaseValue: dbConfig.NoAck,
			ClusterValue:  clusterConfig.NoAck,
		})
	}

	if dbConfig.AllowDirect != clusterConfig.AllowDirect {
		differences = append(differences, models.ConfigDifference{
			Field:         "allow_direct",
			DatabaseValue: dbConfig.AllowDirect,
			ClusterValue:  clusterConfig.AllowDirect,
		})
	}

	if dbConfig.AllowRollupHdrs != clusterConfig.AllowRollupHdrs {
		differences = append(differences, models.ConfigDifference{
			Field:         "allow_rollup_hdrs",
			DatabaseValue: dbConfig.AllowRollupHdrs,
			ClusterValue:  clusterConfig.AllowRollupHdrs,
		})
	}

	if dbConfig.DenyDelete != clusterConfig.DenyDelete {
		differences = append(differences, models.ConfigDifference{
			Field:         "deny_delete",
			DatabaseValue: dbConfig.DenyDelete,
			ClusterValue:  clusterConfig.DenyDelete,
		})
	}

	if dbConfig.DenyPurge != clusterConfig.DenyPurge {
		differences = append(differences, models.ConfigDifference{
			Field:         "deny_purge",
			DatabaseValue: dbConfig.DenyPurge,
			ClusterValue:  clusterConfig.DenyPurge,
		})
	}

	if dbConfig.DuplicateWindow != clusterConfig.DuplicateWindow {
		differences = append(differences, models.ConfigDifference{
			Field:         "duplicate_window",
			DatabaseValue: dbConfig.DuplicateWindow,
			ClusterValue:  clusterConfig.DuplicateWindow,
		})
	}

	return &models.JetStreamConfigDiff{
		HasDifference:  len(differences) > 0,
		DatabaseConfig: dbConfig,
		ClusterConfig:  clusterConfig,
		Differences:    differences,
	}
}

// compareStringSlices 比较两个字符串切片是否相等
func (s *JetStreamManageService) compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// 创建map来比较内容
	aMap := make(map[string]bool)
	for _, v := range a {
		aMap[v] = true
	}

	for _, v := range b {
		if !aMap[v] {
			return false
		}
	}

	return true
}

// mergeStreamInfoToJetStream 将实时统计数据合并到 JetStream 对象中
func (s *JetStreamManageService) mergeStreamInfoToJetStream(jetStream *models.JetStream, streamInfo *jsApi.StreamInfo) {
	if streamInfo == nil {
		return
	}

	state := streamInfo.State

	// 更新统计信息
	jetStream.Messages = state.Msgs
	jetStream.FirstSeq = state.FirstSeq
	jetStream.LastSeq = state.LastSeq
	jetStream.NumConsumers = state.Consumers

	// 转换字节大小
	if state.Bytes > 0 {
		bytesValue, bytesUnit := models.ConvertFromBytes(int64(state.Bytes))
		jetStream.BytesValue = bytesValue
		jetStream.BytesUnit = bytesUnit
	} else {
		jetStream.BytesValue = 0
		jetStream.BytesUnit = models.StorageUnitBytes
	}

	log.WithContext(context.Background()).Debugf("Merged live statistics for JetStream %s: messages=%d, bytes=%.3f %s, consumers=%d, first_seq=%d, last_seq=%d",
		jetStream.ID, jetStream.Messages, jetStream.BytesValue, jetStream.BytesUnit, jetStream.NumConsumers, jetStream.FirstSeq, jetStream.LastSeq)
}
