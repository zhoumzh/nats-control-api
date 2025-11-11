package service

import (
	"encoding/json"
	"fmt"
	"time"

	"nats-control-api/internal/db"
	"nats-control-api/internal/jwt"
	"nats-control-api/pkg/models"

	"github.com/google/uuid"
)

type UserService struct {
	repo        *db.Repository
	natsManager *jwt.NATSManager
}

func NewUserService(repo *db.Repository, natsManager *jwt.NATSManager) *UserService {
	return &UserService{
		repo:        repo,
		natsManager: natsManager,
	}
}

func (s *UserService) CreateUser(accountID string, req *models.CreateUserRequest) (*models.User, error) {
	account, err := s.repo.GetAccount(accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}
	if account.Status != models.AccountStatusActive {
		return nil, fmt.Errorf("cannot create user in disabled account")
	}

	publicKey, nkey, err := s.natsManager.GenerateUserKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate user key pair: %w", err)
	}

	user := &models.User{
		ID:          models.GenID(models.UserObjType),
		AccountID:   accountID,
		Name:        req.Name,
		PublicKey:   publicKey,
		NKey:        nkey,
		Status:      models.UserStatusActive,
		Description: req.Description,
		Permissions: req.Permissions,
		Limits:      req.Limits,
		JWTClaims:   s.generateJWTClaims(req),
		CreatedAt:   models.CustomTime{Time: time.Now()},
		UpdatedAt:   models.CustomTime{Time: time.Now()},
	}

	if err := s.repo.CreateUser(user); err != nil {
		return nil, fmt.Errorf("failed to create user in database: %w", err)
	}

	// 注意：creds文件将在JWT任务处理后生成
	// 这里不生成creds文件，因为需要Account的NKey信息

	if err := s.createJWTTask(user.ID, models.EntityTypeUser, models.OperationCreate, user); err != nil {
		return nil, fmt.Errorf("failed to create JWT task: %w", err)
	}

	return user, nil
}

func (s *UserService) GetUser(id string) (*models.User, error) {
	return s.repo.GetUser(id)
}

func (s *UserService) UpdateUser(id string, req *models.UpdateUserRequest) (*models.User, error) {
	user, err := s.repo.GetUser(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	if req.Name != "" {
		user.Name = req.Name
	}
	if req.Description != "" {
		user.Description = req.Description
	}
	if req.Status != "" {
		user.Status = req.Status
	}
	if req.IsAdmin != nil {
		user.IsAdmin = *req.IsAdmin
	}
	if req.Permissions != nil {
		user.Permissions = req.Permissions
	}
	if req.Limits != nil {
		user.Limits = req.Limits
	}
	// 重新生成JWTClaims基于新的业务字段
	if req.Role != "" || req.Department != "" || req.Project != "" || req.IsAdmin != nil {
		user.JWTClaims = s.generateJWTClaimsFromUpdate(req, user)
	}
	user.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateUser(user); err != nil {
		return nil, fmt.Errorf("failed to update user in database: %w", err)
	}

	operation := models.OperationUpdate
	if req.Status == models.UserStatusDisabled {
		operation = models.OperationDisable
	} else if req.Status == models.UserStatusActive {
		operation = models.OperationEnable
	}

	if err := s.createJWTTask(user.ID, models.EntityTypeUser, operation, user); err != nil {
		return nil, fmt.Errorf("failed to create JWT task: %w", err)
	}

	return user, nil
}

func (s *UserService) ListUsers(accountID string, limit, offset int) ([]*models.User, error) {
	return s.repo.ListUsers(accountID, limit, offset)
}

// ListAdminUsersFromNonSystemAccounts 获取所有非系统账号的管理员用户
func (s *UserService) ListAdminUsersFromNonSystemAccounts() ([]*models.User, error) {
	return s.repo.ListAdminUsersFromNonSystemAccounts()
}

// GetAllUsers 获取所有用户，支持过滤
func (s *UserService) GetAllUsers(accountID, status string, limit, offset int) ([]*models.User, error) {
	return s.repo.GetAllUsers(accountID, status, limit, offset)
}

func (s *UserService) DisableUser(id string) error {
	user, err := s.repo.GetUser(id)
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return fmt.Errorf("user not found")
	}

	user.Status = models.UserStatusDisabled
	user.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateUser(user); err != nil {
		return fmt.Errorf("failed to update user status: %w", err)
	}

	if err := s.createJWTTask(user.ID, models.EntityTypeUser, models.OperationDisable, user); err != nil {
		return fmt.Errorf("failed to create JWT task: %w", err)
	}

	return nil
}

func (s *UserService) EnableUser(id string) error {
	user, err := s.repo.GetUser(id)
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return fmt.Errorf("user not found")
	}

	user.Status = models.UserStatusActive
	user.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateUser(user); err != nil {
		return fmt.Errorf("failed to update user status: %w", err)
	}

	if err := s.createJWTTask(user.ID, models.EntityTypeUser, models.OperationEnable, user); err != nil {
		return fmt.Errorf("failed to create JWT task: %w", err)
	}

	return nil
}

// DeleteUser deletes a user from the database
func (s *UserService) DeleteUser(id string) error {
	user, err := s.repo.GetUser(id)
	if err != nil {
		return fmt.Errorf("获取用户失败: %w", err)
	}
	if user == nil {
		return fmt.Errorf("用户未找到")
	}

	// Delete from database
	if err := s.repo.UserRepo.DeleteUser(id); err != nil {
		return fmt.Errorf("删除用户失败: %w", err)
	}

	return nil
}

func (s *UserService) createJWTTask(entityID string, entityType models.EntityType, operation models.Operation, payload interface{}) error {
	payloadMap := make(models.JSONMap)
	if payload != nil {
		payloadBytes, _ := json.Marshal(payload)
		json.Unmarshal(payloadBytes, &payloadMap)
	}

	// 获取账户公钥用于冗余存储
	var publicKey string
	if entityType == models.EntityTypeUser {
		user, err := s.repo.GetUser(entityID)
		if err != nil {
			return fmt.Errorf("获取用户失败: %w", err)
		}
		if user != nil {
			// 获取用户所属账户的公钥
			account, err := s.repo.GetAccount(user.AccountID)
			if err != nil {
				return fmt.Errorf("获取用户账户公钥失败: %w", err)
			}
			if account != nil {
				publicKey = account.PublicKey
			}
		}
	}

	task := &models.JWTTask{
		ID:         uuid.New().String(),
		Status:     models.TaskStatusPending,
		EntityType: entityType,
		EntityID:   entityID,
		PublicKey:  publicKey, // 冗余存储账户公钥
		Operation:  operation,
		Retries:    0,
		MaxRetries: 3,
		CreatedAt:  models.CustomTime{Time: time.Now()},
		UpdatedAt:  models.CustomTime{Time: time.Now()},
	}

	if operation == models.OperationDisable {
		// disable也是JWT更新操作，无需额外处理
	}

	return s.repo.CreateJWTTask(task)
}

// generateJWTClaims 基于业务字段生成JWT Claims
func (s *UserService) generateJWTClaims(req *models.CreateUserRequest) *models.JSONMap {
	claims := models.JSONMap{}

	if req.Role != "" {
		claims["role"] = req.Role
	}
	if req.Department != "" {
		claims["department"] = req.Department
	}
	if req.Project != "" {
		claims["project"] = req.Project
	}

	// 基于角色设置默认权限标签
	if req.Role == "admin" {
		claims["admin"] = true
	}

	if len(claims) == 0 {
		return nil
	}

	return &claims
}

// generateJWTClaimsFromUpdate 基于更新请求生成JWT Claims
func (s *UserService) generateJWTClaimsFromUpdate(req *models.UpdateUserRequest, user *models.User) *models.JSONMap {
	claims := models.JSONMap{}

	// 如果有现有的claims，先复制
	if user.JWTClaims != nil {
		for k, v := range *user.JWTClaims {
			claims[k] = v
		}
	}

	// 更新新的字段
	if req.Role != "" {
		claims["role"] = req.Role
		// 基于角色更新权限
		if req.Role == "admin" {
			claims["admin"] = true
		} else {
			delete(claims, "admin")
		}
	}
	if req.IsAdmin != nil {
		if *req.IsAdmin {
			claims["admin"] = true
		} else {
			delete(claims, "admin")
		}
	}
	if req.Department != "" {
		claims["department"] = req.Department
	}
	if req.Project != "" {
		claims["project"] = req.Project
	}

	if len(claims) == 0 {
		return nil
	}

	return &claims
}

func (s *UserService) UpdateUserCredsFile(userID string, credsContent string) error {
	user, err := s.repo.GetUser(userID)
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return fmt.Errorf("user not found")
	}

	user.CredsFile = &credsContent
	user.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateUser(user); err != nil {
		return fmt.Errorf("failed to update user creds file: %w", err)
	}

	return nil
}

func (s *UserService) GetCopyContextCommands(userID string) (string, error) {
	user, err := s.repo.GetUser(userID)
	if err != nil {
		return "", fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil {
		return "", fmt.Errorf("user not found")
	}

	if user.CredsFile == nil || *user.CredsFile == "" {
		return "", fmt.Errorf("user creds file not found, please generate it first")
	}

	credsFileName := fmt.Sprintf("%s.creds", user.Name)
	credsContent := *user.CredsFile

	commands := fmt.Sprintf(`cat > %s << 'EOF'
%s
EOF
nats context save %s --server=nats://nats-server.nats-dev.svc.cluster.local:4222 --creds=$(pwd)/%s
nats context select %s`, credsFileName, credsContent, user.Name, credsFileName, user.Name)

	return commands, nil
}
