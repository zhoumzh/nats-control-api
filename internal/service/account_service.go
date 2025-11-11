package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"nats-control-api/internal/db"
	"nats-control-api/internal/jwt"
	"nats-control-api/pkg/models"

	"github.com/google/uuid"
	nats_jwt "github.com/nats-io/jwt/v2"
)

type AccountService struct {
	repo        *db.Repository
	natsManager *jwt.NATSManager
}

func NewAccountService(repo *db.Repository, natsManager *jwt.NATSManager) *AccountService {
	return &AccountService{
		repo:        repo,
		natsManager: natsManager,
	}
}

func (s *AccountService) CreateAccount(req *models.CreateAccountRequest) (*models.Account, error) {
	// 验证 OriginClusterID 是否为空
	if req.OriginClusterID == "" {
		return nil, fmt.Errorf("origin_cluster_id 不能为空")
	}

	// 验证集群是否存在
	cluster, err := s.repo.GetClusterByID(req.OriginClusterID)
	if err != nil {
		return nil, fmt.Errorf("验证集群失败: %w", err)
	}
	if cluster == nil {
		return nil, fmt.Errorf("集群 %s 不存在", req.OriginClusterID)
	}

	publicKey, nkey, err := s.natsManager.GenerateAccountKeyPair()
	if err != nil {
		return nil, fmt.Errorf("生成账户密钥对失败: %w", err)
	}

	account := &models.Account{
		ID:              models.GenID(models.AccountObjType),
		Name:            req.Name,
		PublicKey:       publicKey,
		NKey:            nkey,
		Status:          models.AccountStatusActive,
		Description:     req.Description,
		OriginClusterID: req.OriginClusterID,
		Limits:          req.Limits,
		JWTClaims:       nil,
		CreatedAt:       models.CustomTime{Time: time.Now()},
		UpdatedAt:       models.CustomTime{Time: time.Now()},
	}

	if err := s.repo.CreateAccount(account); err != nil {
		return nil, fmt.Errorf("在数据库中创建账户失败: %w", err)
	}

	// Generate creds file content
	credsContent, err := s.natsManager.GenerateAccountCreds(account)
	if err != nil {
		// Don't fail the creation if creds generation fails, just log it
		fmt.Printf("警告: 为账户 %s 生成凭证文件失败: %v\n", account.ID, err)
	} else {
		account.CredsFile = &credsContent
		// Update account with creds file content
		if err := s.repo.UpdateAccount(account); err != nil {
			fmt.Printf("警告: 更新账户凭证文件失败: %v\n", err)
		}
	}

	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationCreate, account); err != nil {
		return nil, fmt.Errorf("创建JWT任务失败: %w", err)
	}

	return account, nil
}

func (s *AccountService) GetAccount(id string) (*models.Account, error) {
	return s.repo.GetAccount(id)
}

func (s *AccountService) UpdateAccount(id string, req *models.UpdateAccountRequest) (*models.Account, error) {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return nil, fmt.Errorf("获取账户失败: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("账户未找到")
	}

	if req.Name != "" {
		account.Name = req.Name
	}
	if req.Description != "" {
		account.Description = req.Description
	}
	if req.Status != "" {
		account.Status = req.Status
	}
	if req.Limits != nil {
		// 系统账户不允许设置任何limits
		if account.IsSystemAccount {
			// 系统账户直接清空limits字段，确保JWT生成时不包含任何限制
			account.Limits = nil
		} else {
			account.Limits = req.Limits
		}
	}
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	// 基于新的配置生成JWT和Claims
	jwtToken, err := s.natsManager.CreateAccountJWT(account)
	if err != nil {
		return nil, fmt.Errorf("生成JWT失败: %w", err)
	}

	// 解码JWT获取Claims用于存储
	decodedClaims, err := s.natsManager.DecodeAccountJWT(jwtToken)
	if err != nil {
		return nil, fmt.Errorf("解码JWT失败: %w", err)
	}

	// 将Claims转换为JSONMap格式存储
	if accountClaims, ok := decodedClaims.(*nats_jwt.AccountClaims); ok {
		claimsMap := make(models.JSONMap)
		claimsMap["sub"] = accountClaims.Subject
		claimsMap["name"] = accountClaims.Name
		claimsMap["iss"] = accountClaims.Issuer
		claimsMap["iat"] = accountClaims.IssuedAt
		claimsMap["exp"] = accountClaims.Expires

		// 添加NATS特定的claims
		natsMap := make(map[string]interface{})
		natsMap["limits"] = accountClaims.Limits
		if len(accountClaims.Imports) > 0 {
			natsMap["imports"] = accountClaims.Imports
		}
		if len(accountClaims.Exports) > 0 {
			natsMap["exports"] = accountClaims.Exports
		}
		claimsMap["nats"] = natsMap

		account.JWTClaims = &claimsMap
	}

	// 保存JWT文本
	account.JWTText = &jwtToken

	if err := s.repo.UpdateAccount(account); err != nil {
		return nil, fmt.Errorf("在数据库中更新账户失败: %w", err)
	}

	operation := models.OperationUpdate
	if req.Status == models.AccountStatusDisabled {
		operation = models.OperationDisable
	}

	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, operation, account); err != nil {
		return nil, fmt.Errorf("创建JWT任务失败: %w", err)
	}

	return account, nil
}

func (s *AccountService) ListAccounts(limit, offset int) ([]*models.Account, error) {
	return s.repo.ListAccounts(limit, offset)
}

// ListAccountsWithFilters returns accounts with filters applied
func (s *AccountService) ListAccountsWithFilters(req *models.AccountListRequest) (*models.AccountListResponse, error) {
	return s.repo.AccountRepo.GetAccounts(req)
}

func (s *AccountService) DisableAccount(id string) error {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return fmt.Errorf("获取账户失败: %w", err)
	}
	if account == nil {
		return fmt.Errorf("账户未找到")
	}

	account.Status = models.AccountStatusDisabled
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateAccount(account); err != nil {
		return fmt.Errorf("更新账户状态失败: %w", err)
	}

	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationDisable, account); err != nil {
		return fmt.Errorf("创建JWT任务失败: %w", err)
	}

	return nil
}

func (s *AccountService) EnableAccount(id string) error {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return fmt.Errorf("获取账户失败: %w", err)
	}
	if account == nil {
		return fmt.Errorf("账户未找到")
	}

	account.Status = models.AccountStatusActive
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateAccount(account); err != nil {
		return fmt.Errorf("更新账户状态失败: %w", err)
	}

	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationEnable, account); err != nil {
		return fmt.Errorf("创建JWT任务失败: %w", err)
	}

	return nil
}

func (s *AccountService) createJWTTask(entityID string, entityType models.EntityType, operation models.Operation, payload interface{}) error {
	payloadMap := make(models.JSONMap)
	if payload != nil {
		payloadBytes, _ := json.Marshal(payload)
		json.Unmarshal(payloadBytes, &payloadMap)
	}

	// 获取账户公钥用于冗余存储
	var publicKey string
	if entityType == models.EntityTypeAccount {
		// 如果payload是Account类型，直接使用
		if account, ok := payload.(*models.Account); ok && account != nil {
			publicKey = account.PublicKey
		} else {
			// 否则从数据库查询
			account, err := s.repo.GetAccount(entityID)
			if err != nil {
				return fmt.Errorf("获取账户公钥失败: %w", err)
			}
			if account != nil {
				publicKey = account.PublicKey
			}
		}
	}

	task := &models.JWTTask{
		ID:          uuid.New().String(),
		Status:      models.TaskStatusPending,
		EntityType:  entityType,
		EntityID:    entityID,
		PublicKey:   publicKey, // 冗余存储账户公钥
		Operation:   operation,
		TriggerType: models.TriggerTypeAuto, // 自动触发
		Retries:     0,
		MaxRetries:  3,
		CreatedAt:   models.CustomTime{Time: time.Now()},
		UpdatedAt:   models.CustomTime{Time: time.Now()},
	}

	if operation == models.OperationDisable {
		// disable也是JWT更新操作，无需额外处理
	}

	return s.repo.CreateJWTTask(task)
}

// AddAccountExport adds an export to the account
func (s *AccountService) AddAccountExport(id string, req *models.AddExportRequest) (*models.Account, error) {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}

	// Initialize limits if nil
	if account.Limits == nil {
		account.Limits = &models.AccountLimits{}
	}

	// Check if export name already exists
	for _, export := range account.Limits.Exports {
		if export.Name == req.Name {
			return nil, fmt.Errorf("export with name '%s' already exists", req.Name)
		}
	}

	// Create new export
	newExport := models.AccountExport{
		Name:                 req.Name,
		Subject:              req.Subject,
		Type:                 req.Type,
		TokenReq:             req.TokenReq,
		ResponseType:         req.ResponseType,
		AccountTokenPosition: req.AccountTokenPosition,
	}

	if req.Description != "" || req.InfoURL != "" {
		newExport.Info = &models.ExportInfo{
			Description: req.Description,
			InfoURL:     req.InfoURL,
		}
	}

	// Add export to account
	account.Limits.Exports = append(account.Limits.Exports, newExport)
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateAccount(account); err != nil {
		return nil, fmt.Errorf("failed to update account with export: %w", err)
	}

	// Create JWT update task
	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationUpdate, account); err != nil {
		return nil, fmt.Errorf("failed to create JWT task: %w", err)
	}

	return account, nil
}

// AddAccountImport adds an import to the account
func (s *AccountService) AddAccountImport(id string, req *models.AddImportRequest) (*models.Account, error) {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}

	// Initialize limits if nil
	if account.Limits == nil {
		account.Limits = &models.AccountLimits{}
	}

	// Check if import name already exists
	for _, imp := range account.Limits.Imports {
		if imp.Name == req.Name {
			return nil, fmt.Errorf("import with name '%s' already exists", req.Name)
		}
	}

	// Validate that the source account exists
	sourceAccount, err := s.repo.GetAccount(req.Account)
	if err != nil {
		return nil, fmt.Errorf("failed to validate source account: %w", err)
	}
	if sourceAccount == nil {
		return nil, fmt.Errorf("source account with ID '%s' not found", req.Account)
	}

	// Create new import
	newImport := models.AccountImport{
		Name:    req.Name,
		Subject: req.Subject,
		Account: sourceAccount.PublicKey,
		Token:   req.Token,
		To:      req.To,
		Type:    req.Type,
	}

	// Add import to account
	account.Limits.Imports = append(account.Limits.Imports, newImport)
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateAccount(account); err != nil {
		return nil, fmt.Errorf("failed to update account with import: %w", err)
	}

	// Create JWT update task
	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationUpdate, account); err != nil {
		return nil, fmt.Errorf("failed to create JWT task: %w", err)
	}

	return account, nil
}

// RemoveAccountExport removes an export from the account
func (s *AccountService) RemoveAccountExport(id, name string) (*models.Account, error) {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}

	if account.Limits == nil {
		return nil, fmt.Errorf("export not found")
	}

	// Find and remove the export
	found := false
	newExports := make([]models.AccountExport, 0)
	for _, export := range account.Limits.Exports {
		if export.Name != name {
			newExports = append(newExports, export)
		} else {
			found = true
		}
	}

	if !found {
		return nil, fmt.Errorf("export not found")
	}

	account.Limits.Exports = newExports
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateAccount(account); err != nil {
		return nil, fmt.Errorf("failed to update account: %w", err)
	}

	// Create JWT update task
	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationUpdate, account); err != nil {
		return nil, fmt.Errorf("failed to create JWT task: %w", err)
	}

	return account, nil
}

// RemoveAccountImport removes an import from the account
func (s *AccountService) RemoveAccountImport(id, name string) (*models.Account, error) {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}

	if account.Limits == nil {
		return nil, fmt.Errorf("import not found")
	}

	// Find and remove the import
	found := false
	newImports := make([]models.AccountImport, 0)
	for _, imp := range account.Limits.Imports {
		if imp.Name != name {
			newImports = append(newImports, imp)
		} else {
			found = true
		}
	}

	if !found {
		return nil, fmt.Errorf("import not found")
	}

	account.Limits.Imports = newImports
	account.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := s.repo.UpdateAccount(account); err != nil {
		return nil, fmt.Errorf("failed to update account: %w", err)
	}

	// Create JWT update task
	if err := s.createJWTTask(account.ID, models.EntityTypeAccount, models.OperationUpdate, account); err != nil {
		return nil, fmt.Errorf("failed to create JWT task: %w", err)
	}

	return account, nil
}

// CreateAccountAssociation creates a simplified cross-account association
func (s *AccountService) CreateAccountAssociation(sourceAccountID string, req *models.AccountAssociationRequest) error {
	// Validate source account exists
	sourceAccount, err := s.repo.GetAccount(sourceAccountID)
	if err != nil {
		return fmt.Errorf("failed to get source account: %w", err)
	}
	if sourceAccount == nil {
		return fmt.Errorf("source account not found")
	}

	// Validate target account exists
	targetAccount, err := s.repo.GetAccount(req.TargetAccountID)
	if err != nil {
		return fmt.Errorf("failed to get target account: %w", err)
	}
	if targetAccount == nil {
		return fmt.Errorf("target account not found")
	}

	// Create associations for each subject
	for _, subject := range req.Subjects {
		fullSubject := subject.BaseSubject + subject.Pattern
		associationName := fmt.Sprintf("%s_%s", sourceAccount.Name, subject.BaseSubject)

		// Add export to source account (what source account exports)
		exportReq := &models.AddExportRequest{
			Name:        associationName,
			Subject:     fullSubject,
			Type:        models.ExportType(subject.Type),
			TokenReq:    false,
			Description: fmt.Sprintf("Auto-generated export for association with %s", targetAccount.Name),
		}

		_, err := s.AddAccountExport(sourceAccountID, exportReq)
		if err != nil {
			// If export already exists, continue
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to add export to source account: %w", err)
			}
		}

		// Add import to target account (what target account imports from source)
		importReq := &models.AddImportRequest{
			Name:    associationName,
			Subject: fullSubject,
			Account: sourceAccountID,
			Type:    models.ImportType(subject.Type),
		}

		_, err = s.AddAccountImport(req.TargetAccountID, importReq)
		if err != nil {
			// If import already exists, continue
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to add import to target account: %w", err)
			}
		}

	}

	return nil
}

// DeleteAccount deletes an account after validating no associated users exist
func (s *AccountService) DeleteAccount(id string) error {
	account, err := s.repo.GetAccount(id)
	if err != nil {
		return fmt.Errorf("获取账户失败: %w", err)
	}
	if account == nil {
		return fmt.Errorf("账户未找到")
	}

	// Check if account has associated users
	users, err := s.repo.GetUsersByAccountID(id)
	if err != nil {
		return fmt.Errorf("检查关联用户失败: %w", err)
	}
	if len(users) > 0 {
		return fmt.Errorf("无法删除账户，存在 %d 个关联用户", len(users))
	}

	// Delete from database
	if err := s.repo.AccountRepo.DeleteAccount(id); err != nil {
		return fmt.Errorf("删除账户失败: %w", err)
	}

	return nil
}

// CreateManualSyncJWTTask 创建手动同步JWT任务
func (s *AccountService) CreateManualSyncJWTTask(accountID string, clusterIDs []string) error {
	// 验证集群ID是否有效
	for _, clusterID := range clusterIDs {
		cluster, err := s.repo.GetClusterByID(clusterID)
		if err != nil {
			return fmt.Errorf("failed to validate cluster %s: %w", clusterID, err)
		}
		if cluster == nil {
			return fmt.Errorf("cluster %s not found", clusterID)
		}
	}

	// 获取账户公钥用于冗余存储
	account, err := s.repo.GetAccount(accountID)
	if err != nil {
		return fmt.Errorf("获取账户公钥失败: %w", err)
	}
	if account == nil {
		return fmt.Errorf("账户未找到: %s", accountID)
	}

	// 构造集群ID列表
	clusterIDMap := make(models.JSONMap)
	clusterIDMap["cluster_ids"] = clusterIDs

	// 创建手动触发的JWT任务
	task := &models.JWTTask{
		ID:          uuid.New().String(),
		Status:      models.TaskStatusPending,
		EntityType:  models.EntityTypeAccount,
		EntityID:    accountID,
		PublicKey:   account.PublicKey, // 冗余存储账户公钥
		Operation:   models.OperationUpdate,
		TriggerType: models.TriggerTypeManual, // 标记为手动触发
		ClusterIDs:  &clusterIDMap,            // 指定要同步的集群
		Retries:     0,
		MaxRetries:  3,
		CreatedAt:   models.CustomTime{Time: time.Now()},
		UpdatedAt:   models.CustomTime{Time: time.Now()},
	}

	return s.repo.CreateJWTTask(task)
}

func (s *AccountService) GetUserCountByAccountID(accountID string) (int64, error) {
	return s.repo.CountUsersByAccountID(accountID)
}

func (s *AccountService) GetJWTTaskCountByAccountPublicKey(publicKey string) (int64, error) {
	return s.repo.CountJWTTasksByAccountPublicKey(publicKey)
}
