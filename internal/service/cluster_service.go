package service

import (
	"context"
	"fmt"
	"time"

	"nats-control-api/internal/config"
	"nats-control-api/internal/db"
	"nats-control-api/pkg/models"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	jsApi "github.com/nats-io/nats.go/jetstream"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type ClusterService struct {
	repo   *db.Repository
	config *config.Config
	server *ClusterServer
}

func NewClusterService(repo *db.Repository, config *config.Config) *ClusterService {
	cs := &ClusterService{
		repo:   repo,
		config: config,
		server: NewClusterServer(),
	}
	return cs
}

// ========================================
// 数据库操作 - Cluster CRUD
// ========================================

func (s *ClusterService) CreateCluster(req *models.CreateClusterRequest) (*models.Cluster, error) {
	cluster := &models.Cluster{
		ID:          models.GenID(models.ClusterObjType),
		Name:        req.Name,
		Description: req.Description,
		Status:      models.ClusterStatusActive,

		Host:        req.Host,
		NATSPort:    s.GetPortOrDefault(req.NATSPort, 4222),
		GatewayPort: s.GetPortOrDefault(req.GatewayPort, 7222),
		MonitorPort: s.GetPortOrDefault(req.MonitorPort, 8222),
		ClusterPort: s.GetPortOrDefault(req.ClusterPort, 6222),

		SystemAccountID: req.SystemAccountID,
		SystemUserID:    req.SystemUserID,

		CreatedAt: models.CustomTime{Time: time.Now()},
		UpdatedAt: models.CustomTime{Time: time.Now()},
	}

	if err := s.repo.CreateCluster(cluster); err != nil {
		return nil, fmt.Errorf("failed to create cluster in database: %w", err)
	}

	log.WithContext(context.Background()).Infof("集群注册成功: cluster_id=%s, cluster_name=%s, host=%s, nats_port=%d, gateway_port=%d, monitor_port=%d, cluster_port=%d", cluster.ID, cluster.Name, cluster.Host, cluster.NATSPort, cluster.GatewayPort, cluster.MonitorPort, cluster.ClusterPort)

	return cluster, nil
}

func (s *ClusterService) GetClusterByID(id string) (*models.Cluster, error) {
	cluster, err := s.repo.GetClusterByID(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster: %w", err)
	}

	return cluster, nil
}

func (s *ClusterService) ListClusters(status string) ([]*models.Cluster, error) {
	clusters, err := s.repo.ListClusters(status)
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}

	return clusters, nil
}

func (s *ClusterService) GetActiveClusters() ([]*models.Cluster, error) {
	return s.ListClusters(string(models.ClusterStatusActive))
}

func (s *ClusterService) UpdateCluster(id string, req *models.UpdateClusterRequest) (*models.Cluster, error) {
	cluster, err := s.repo.GetClusterByID(id)
	if err != nil {
		return nil, fmt.Errorf("cluster not found: %w", err)
	}

	if req.Name != "" {
		cluster.Name = req.Name
	}
	if req.Description != "" {
		cluster.Description = req.Description
	}
	if req.Status != "" {
		cluster.Status = req.Status
	}

	if req.Host != "" {
		cluster.Host = req.Host
	}
	if req.NATSPort > 0 {
		cluster.NATSPort = req.NATSPort
	}
	if req.GatewayPort > 0 {
		cluster.GatewayPort = req.GatewayPort
	}
	if req.MonitorPort > 0 {
		cluster.MonitorPort = req.MonitorPort
	}
	if req.ClusterPort > 0 {
		cluster.ClusterPort = req.ClusterPort
	}

	if req.SystemAccountID != "" {
		cluster.SystemAccountID = req.SystemAccountID
	}
	if req.SystemUserID != "" {
		cluster.SystemUserID = req.SystemUserID
	}

	cluster.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateCluster(cluster); err != nil {
		return nil, fmt.Errorf("failed to update cluster: %w", err)
	}

	log.WithContext(context.Background()).Infof("集群更新成功: cluster_id=%s, cluster_name=%s, host=%s, nats_port=%d, gateway_port=%d, monitor_port=%d, cluster_port=%d", cluster.ID, cluster.Name, cluster.Host, cluster.NATSPort, cluster.GatewayPort, cluster.MonitorPort, cluster.ClusterPort)

	return cluster, nil
}

func (s *ClusterService) SetClusterStatus(id string, status models.ClusterStatus) error {
	cluster, err := s.repo.GetClusterByID(id)
	if err != nil {
		return fmt.Errorf("cluster not found: %w", err)
	}

	cluster.Status = status
	cluster.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateCluster(cluster); err != nil {
		return fmt.Errorf("failed to update cluster status: %w", err)
	}

	log.WithContext(context.Background()).Infof("集群状态已更新: cluster_id=%s, cluster_name=%s, status=%s", cluster.ID, cluster.Name, status)

	return nil
}

// ========================================
// 代理ClusterServer - NATS原始连接获取
// ========================================

func (s *ClusterService) GetConnection(clusterID string) (*nats.Conn, error) {
	cluster, err := s.repo.GetClusterByID(clusterID)
	if err != nil {
		return nil, fmt.Errorf("获取集群失败: %v", err)
	}
	if cluster == nil {
		return nil, fmt.Errorf("集群 %s 不存在", clusterID)
	}

	adminUser, err := s.getSystemAccountAdminUser(clusterID)
	if err != nil {
		return nil, err
	}

	return s.server.CreateConnectionWithUser(cluster, adminUser)
}

func (s *ClusterService) GetConnectionWithUser(clusterID, userID string) (*nats.Conn, error) {
	cluster, err := s.repo.GetClusterByID(clusterID)
	if err != nil {
		return nil, fmt.Errorf("获取集群失败: %v", err)
	}
	if cluster == nil {
		return nil, fmt.Errorf("集群 %s 不存在", clusterID)
	}

	user, err := s.repo.GetUser(userID)
	if err != nil {
		return nil, fmt.Errorf("获取用户失败: %v", err)
	}
	if user == nil {
		return nil, fmt.Errorf("用户 %s 不存在", userID)
	}

	return s.server.CreateConnectionWithUser(cluster, user)
}

func (s *ClusterService) GetConnectionWithAccount(clusterID, accountID string) (*nats.Conn, error) {
	cluster, err := s.repo.GetClusterByID(clusterID)
	if err != nil {
		return nil, fmt.Errorf("获取集群失败: %v", err)
	}
	if cluster == nil {
		return nil, fmt.Errorf("集群 %s 不存在", clusterID)
	}

	adminUser, err := s.repo.GetFirstAdminUserByAccountID(accountID)
	if err != nil {
		return nil, fmt.Errorf("账户 %s 下没有找到管理员用户: %v", accountID, err)
	}
	if adminUser == nil {
		return nil, fmt.Errorf("账户 %s 下没有管理员用户", accountID)
	}

	return s.server.CreateConnectionWithUser(cluster, adminUser)
}

func (s *ClusterService) CloseConnection(conn *nats.Conn) {
	s.server.CloseConnection(conn)
}

// ========================================
// 代理ClusterServer - JetStream对象获取
// ========================================

func (s *ClusterService) GetNatsJetStream(clusterID string) (*nats.Conn, jetstream.JetStream, error) {
	conn, err := s.GetConnection(clusterID)
	if err != nil {
		return nil, nil, err
	}

	js, err := s.server.CreateNatsJetStream(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, js, nil
}

func (s *ClusterService) GetNatsJetStreamWithUser(clusterID, userID string) (*nats.Conn, jetstream.JetStream, error) {
	conn, err := s.GetConnectionWithUser(clusterID, userID)
	if err != nil {
		return nil, nil, err
	}

	js, err := s.server.CreateNatsJetStream(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, js, nil
}

func (s *ClusterService) GetNatsJetStreamWithAccount(clusterID, accountID string) (*nats.Conn, jetstream.JetStream, error) {
	conn, err := s.GetConnectionWithAccount(clusterID, accountID)
	if err != nil {
		return nil, nil, err
	}

	js, err := s.server.CreateNatsJetStream(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, js, nil
}

// ========================================
// 代理ClusterServer - JetStreamContext对象获取
// ========================================

func (s *ClusterService) GetNatsJetStreamContextWithUsr(clusterID string, userID string) (*nats.Conn, JetStreamContext, error) {
	conn, err := s.GetConnectionWithUser(clusterID, userID)
	if err != nil {
		return nil, nil, err
	}
	jsc, err := s.server.CreateJetStreamContext(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, jsc, nil
}

func (s *ClusterService) GetNatsJetStreamContextWithAccount(clusterID string, accountID string) (*nats.Conn, JetStreamContext, error) {
	conn, err := s.GetConnectionWithAccount(clusterID, accountID)
	if err != nil {
		return nil, nil, err
	}
	jsc, err := s.server.CreateJetStreamContext(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, jsc, nil
}

// ========================================
// 代理ClusterServer - 其他功能方法
// ========================================

func (s *ClusterService) TestClusterConnection(id string) (map[string]interface{}, error) {
	cluster, err := s.repo.GetClusterByID(id)
	if err != nil {
		return nil, fmt.Errorf("cluster not found: %w", err)
	}

	result := map[string]interface{}{
		"cluster_id":   cluster.ID,
		"cluster_name": cluster.Name,
		"url":          cluster.GetNATSURL(),
		"tested_at":    time.Now(),
	}

	systemAccount, _ := s.repo.GetAccount(cluster.SystemAccountID)
	adminUser, _ := s.repo.GetFirstAdminUserByAccountID(cluster.SystemAccountID)
	if err := s.server.TestClusterNATSConnection(cluster, systemAccount, adminUser); err != nil {
		result["status"] = "failed"
		result["error"] = err.Error()
		return result, nil
	}

	result["status"] = "success"
	result["message"] = "Connection successful"

	return result, nil
}

func (s *ClusterService) GetStreamInfo(clusterID, userID, streamName string) (*jsApi.StreamInfo, error) {
	conn, js, err := s.GetNatsJetStreamWithUser(clusterID, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream: %w", err)
	}
	defer s.server.CloseConnection(conn)

	return s.server.GetStreamInfo(js, streamName)
}

func (s *ClusterService) GetJetStreamInfoByAccount(clusterID, accountID string, streamName string) (*jsApi.StreamInfo, error) {
	user, _ := s.repo.UserRepo.GetFirstAdminUserByAccountID(accountID)
	if user == nil {
		return nil, fmt.Errorf("user not found")
	}
	return s.GetStreamInfo(clusterID, user.ID, streamName)
}

// ========================================
// 内部辅助方法
// ========================================

func (s *ClusterService) GetPortOrDefault(port, defaultPort int) int {
	if port == 0 {
		return defaultPort
	}
	return port
}

func (s *ClusterService) getSystemAccountAdminUser(clusterID string) (*models.User, error) {
	cluster, err := s.repo.GetClusterByID(clusterID)
	if err != nil {
		return nil, fmt.Errorf("获取集群失败: %v", err)
	}

	if cluster.SystemAccountID == "" {
		return nil, fmt.Errorf("集群 %s 未配置系统账户", clusterID)
	}

	adminUser, err := s.repo.GetFirstAdminUserByAccountID(cluster.SystemAccountID)
	if err != nil {
		return nil, fmt.Errorf("系统账户 %s 下没有找到管理员用户: %v", cluster.SystemAccountID, err)
	}
	if adminUser == nil {
		return nil, fmt.Errorf("系统账户 %s 下没有管理员用户", cluster.SystemAccountID)
	}

	return adminUser, nil
}
