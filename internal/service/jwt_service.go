package service

import (
	"context"
	"fmt"
	"time"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/internal/db"
	"nats-control-api/internal/jwt"
	"nats-control-api/internal/nats"
	"nats-control-api/pkg/models"
)

// JWTService 使用新架构的JWT服务
type JWTService struct {
	repo           *db.Repository
	natsManager    *jwt.NATSManager
	clusterService *ClusterService
	natsService    *nats.Service
}

// NewJWTService 创建使用新架构的JWT服务
func NewJWTService(repo *db.Repository, natsManager *jwt.NATSManager, clusterService *ClusterService) *JWTService {
	return &JWTService{
		repo:           repo,
		natsManager:    natsManager,
		clusterService: clusterService,
		natsService:   nats.NewService(),
	}
}

// PushAccountJWTToAllClusters 将账户JWT推送到所有活跃集群（使用新架构）
func (s *JWTService) PushAccountJWTToAllClusters(publicKey, jwtToken string) (*models.JSONMap, error) {
	// 获取所有活跃集群
	clusters, err := s.repo.ListClusters("active")
	if err != nil {
		return nil, fmt.Errorf("failed to get active clusters: %w", err)
	}

	var errors []error
	successCount := 0
	syncResults := make(models.JSONMap)

	for _, cluster := range clusters {
		if err := s.pushAccountJWTToCluster(cluster.ID, publicKey, jwtToken); err != nil {
			log.WithContext(context.Background()).Errorf("Failed to push JWT to cluster %s (%s): %v", cluster.Name, cluster.ID, err)
			errors = append(errors, fmt.Errorf("cluster %s: %w", cluster.Name, err))
			syncResults[cluster.ID] = map[string]interface{}{
				"status":       "error",
				"message":      err.Error(),
				"cluster_name": cluster.Name,
				"timestamp":    time.Now().Format("2006-01-02 15:04:05"),
			}
		} else {
			log.WithContext(context.Background()).Infof("Successfully pushed JWT to cluster %s (%s)", cluster.Name, cluster.ID)
			successCount++
			syncResults[cluster.ID] = map[string]interface{}{
				"status":       "success",
				"message":      "JWT updated successfully",
				"cluster_name": cluster.Name,
				"timestamp":    time.Now().Format("2006-01-02 15:04:05"),
			}
		}
	}

	// 记录同步结果汇总
	syncResults["summary"] = map[string]interface{}{
		"total":     len(clusters),
		"success":   successCount,
		"failed":    len(clusters) - successCount,
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
	}

	var finalError error
	if len(errors) > 0 {
		finalError = fmt.Errorf("failed to push JWT to %d clusters: %v", len(errors), errors)
	}

	return &syncResults, finalError
}

// PushAccountJWTToSpecificClusters 将账户JWT推送到指定集群（使用新架构）
func (s *JWTService) PushAccountJWTToSpecificClusters(publicKey, jwtToken string, clusterIDs []string) (*models.JSONMap, error) {
	var errors []error
	successCount := 0
	syncResults := make(models.JSONMap)

	for _, clusterID := range clusterIDs {
		cluster, err := s.repo.GetClusterByID(clusterID)
		if err != nil {
			log.WithContext(context.Background()).Errorf("Failed to get cluster %s: %v", clusterID, err)
			errors = append(errors, fmt.Errorf("cluster %s: %w", clusterID, err))
			syncResults[clusterID] = map[string]interface{}{
				"status":    "error",
				"message":   err.Error(),
				"timestamp": time.Now().Format("2006-01-02 15:04:05"),
			}
			continue
		}
		if cluster == nil {
			err := fmt.Errorf("cluster not found: %s", clusterID)
			log.WithContext(context.Background()).Errorf("Cluster not found: %s", clusterID)
			errors = append(errors, err)
			syncResults[clusterID] = map[string]interface{}{
				"status":    "error",
				"message":   "cluster not found",
				"timestamp": time.Now().Format("2006-01-02 15:04:05"),
			}
			continue
		}

		if err := s.pushAccountJWTToCluster(clusterID, publicKey, jwtToken); err != nil {
			log.WithContext(context.Background()).Errorf("Failed to push JWT to cluster %s (%s): %v", cluster.Name, cluster.ID, err)
			errors = append(errors, fmt.Errorf("cluster %s: %w", cluster.Name, err))
			syncResults[clusterID] = map[string]interface{}{
				"status":       "error",
				"message":      err.Error(),
				"cluster_name": cluster.Name,
				"timestamp":    time.Now().Format("2006-01-02 15:04:05"),
			}
		} else {
			log.WithContext(context.Background()).Infof("Successfully pushed JWT to cluster %s (%s)", cluster.Name, cluster.ID)
			successCount++
			syncResults[clusterID] = map[string]interface{}{
				"status":       "success",
				"message":      "JWT updated successfully",
				"cluster_name": cluster.Name,
				"timestamp":    time.Now().Format("2006-01-02 15:04:05"),
			}
		}
	}

	// 记录同步结果汇总
	syncResults["summary"] = map[string]interface{}{
		"total":     len(clusterIDs),
		"success":   successCount,
		"failed":    len(clusterIDs) - successCount,
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
	}

	var finalError error
	if len(errors) > 0 {
		finalError = fmt.Errorf("failed to push JWT to %d clusters: %v", len(errors), errors)
	}

	return &syncResults, finalError
}

// pushAccountJWTToCluster 使用新架构推送JWT到单个集群
func (s *JWTService) pushAccountJWTToCluster(clusterID, publicKey, jwtToken string) error {
	// 1. 使用集群服务获取连接（自动使用系统账户管理员身份）
	conn, err := s.clusterService.GetConnection(clusterID)
	if err != nil {
		return fmt.Errorf("failed to get connection to cluster %s: %w", clusterID, err)
	}
	defer s.clusterService.CloseConnection(conn)

	// 2. 使用基础NATS服务推送JWT
	err = s.natsService.PushJWT(conn, publicKey, jwtToken)
	if err != nil {
		return fmt.Errorf("failed to push JWT: %v", err)
	}

	log.WithContext(context.Background()).Infof("Successfully pushed account JWT for %s to cluster %s", publicKey, clusterID)
	return nil
}

// ProcessManualSyncJWTTask handles manual JWT synchronization to specific clusters
func (s *JWTService) ProcessManualSyncJWTTask(account *models.Account, clusterIDs []string) (*models.JSONMap, error) {
	// Generate JWT for the account
	jwtToken, err := s.natsManager.CreateAccountJWT(account)
	if err != nil {
		return nil, fmt.Errorf("failed to create account JWT: %w", err)
	}

	// Push to specific clusters
	return s.PushAccountJWTToSpecificClusters(account.PublicKey, jwtToken, clusterIDs)
}

// ProcessAccountJWTTask handles standard account JWT operations with automatic cluster sync
func (s *JWTService) ProcessAccountJWTTask(account *models.Account, operation string) (*models.JSONMap, error) {
	// Generate JWT for the account
	jwtToken, err := s.natsManager.CreateAccountJWT(account)
	if err != nil {
		return nil, fmt.Errorf("failed to create account JWT: %w", err)
	}

	// For system accounts, don't push to all clusters to avoid circular updates
	if account.IsSystemAccount {
		// Return success status for system account without multi-cluster sync
		syncResults := make(models.JSONMap)
		syncResults["summary"] = map[string]interface{}{
			"total":     0,
			"success":   0,
			"failed":    0,
			"timestamp": time.Now().Format("2006-01-02 15:04:05"),
			"message":   "System account - skip multi-cluster sync",
		}
		return &syncResults, nil
	}

	// For non-system accounts, only push to origin cluster
	return s.PushAccountJWTToSpecificClusters(account.PublicKey, jwtToken, []string{account.OriginClusterID})
}

// ProcessUserJWTTask handles user JWT operations
func (s *JWTService) ProcessUserJWTTask(user *models.User, operation string) error {
	// Get the user's account
	account, err := s.repo.GetAccount(user.AccountID)
	if err != nil {
		return fmt.Errorf("failed to get user's account: %w", err)
	}
	if account == nil {
		return fmt.Errorf("user's account not found: %s", user.AccountID)
	}

	// Generate JWT for the user
	_, err = s.natsManager.CreateUserJWT(user, account.NKey)
	if err != nil {
		return fmt.Errorf("failed to create user JWT: %w", err)
	}

	log.WithContext(context.Background()).Infof("Successfully processed user JWT task for user %s (operation: %s)", user.ID, operation)
	return nil
}

// GetAccountJWT gets the JWT for an account and returns both token and claims
func (s *JWTService) GetAccountJWT(accountID string) (*models.JWTResponse, error) {
	// Get account from database
	account, err := s.repo.GetAccount(accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}

	// Generate JWT for the account
	jwtToken, err := s.natsManager.CreateAccountJWT(account)
	if err != nil {
		return nil, fmt.Errorf("failed to create account JWT: %w", err)
	}

	// Decode JWT to get claims
	claims, err := s.natsManager.DecodeAccountJWT(jwtToken)
	if err != nil {
		return nil, fmt.Errorf("failed to decode account JWT: %w", err)
	}

	return &models.JWTResponse{
		RawToken: jwtToken,
		Claims:   claims,
	}, nil
}

// GetUserJWT gets the JWT token for a user
func (s *JWTService) GetUserJWT(user *models.User) (string, error) {
	// Get the user's account to get the account's NKey
	account, err := s.repo.GetAccount(user.AccountID)
	if err != nil {
		return "", fmt.Errorf("failed to get user's account: %w", err)
	}
	if account == nil {
		return "", fmt.Errorf("user's account not found: %s", user.AccountID)
	}

	// Generate JWT for the user
	jwtToken, err := s.natsManager.CreateUserJWT(user, account.NKey)
	if err != nil {
		return "", fmt.Errorf("failed to create user JWT: %w", err)
	}

	return jwtToken, nil
}

// GenerateUserCredsFile generates the credentials file content for a user
func (s *JWTService) GenerateUserCredsFile(user *models.User) (string, error) {
	// Get the user's account to get the account's NKey
	account, err := s.repo.GetAccount(user.AccountID)
	if err != nil {
		return "", fmt.Errorf("failed to get user's account: %w", err)
	}
	if account == nil {
		return "", fmt.Errorf("user's account not found: %s", user.AccountID)
	}

	// Generate JWT for the user
	jwtToken, err := s.natsManager.CreateUserJWT(user, account.NKey)
	if err != nil {
		return "", fmt.Errorf("failed to create user JWT: %w", err)
	}

	// Generate the credentials file content
	credsContent := fmt.Sprintf(`-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------

*************************************************************
`, jwtToken, user.NKey)

	return credsContent, nil
}