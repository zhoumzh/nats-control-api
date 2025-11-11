package db

import (
	"context"
	"fmt"
	"strings"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/pkg/models"

	"gorm.io/gorm"
)

type AccountRepository struct {
	db *gorm.DB
}

func NewAccountRepository(db *gorm.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

func (r *AccountRepository) CreateAccount(account *models.Account) error {
	log.WithContext(context.Background()).Infof("Creating account: %+v", account)
	return r.db.Create(account).Error
}

func (r *AccountRepository) GetAccount(id string) (*models.Account, error) {
	log.WithContext(context.Background()).Infof("Getting account with ID: %s", id)
	var account models.Account
	err := r.db.Where("id = ?", id).First(&account).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &account, nil
}

func (r *AccountRepository) GetAccountByName(name string) (*models.Account, error) {
	log.WithContext(context.Background()).Infof("Getting account with name: %s", name)
	var account models.Account
	err := r.db.Where("name = ?", name).First(&account).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &account, nil
}

func (r *AccountRepository) UpdateAccount(account *models.Account) error {
	log.WithContext(context.Background()).Infof("Updating account: %+v", account)
	return r.db.Save(account).Error
}

func (r *AccountRepository) UpdateAccountFields(accountID string, fields map[string]interface{}) error {
	log.WithContext(context.Background()).Infof("Updating account %s with fields: %+v", accountID, fields)
	return r.db.Model(&models.Account{}).Where("id = ?", accountID).Updates(fields).Error
}

func (r *AccountRepository) ListAccounts(limit, offset int) ([]*models.Account, error) {
	log.WithContext(context.Background()).Infof("Listing accounts with limit: %d, offset: %d", limit, offset)
	var accounts []*models.Account
	err := r.db.Limit(limit).Offset(offset).Find(&accounts).Error
	return accounts, err
}

func (r *AccountRepository) GetAccountByPublicKey(publicKey string) (*models.Account, error) {
	log.WithContext(context.Background()).Infof("Getting account with public key: %s", publicKey)
	
	// 增强参数验证
	if publicKey == "" {
		log.WithContext(context.Background()).Warn("Account public key is empty")
		return nil, fmt.Errorf("account public key cannot be empty")
	}
	
	// 验证公钥格式基本检查
	if len(publicKey) != 56 || !strings.HasPrefix(publicKey, "A") {
		log.WithContext(context.Background()).Warnf("Invalid NATS account public key format: %s", publicKey)
		return nil, fmt.Errorf("invalid NATS account public key format")
	}

	var account models.Account
	err := r.db.Where("public_key = ?", publicKey).First(&account).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.WithContext(context.Background()).Infof("Account with public key %s not found", publicKey)
			return nil, nil
		}
		log.WithContext(context.Background()).Errorf("Database error when getting account by public key %s: %v", publicKey, err)
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	
	// 额外的空值检查
	if account.ID == "" {
		log.WithContext(context.Background()).Warnf("Found account with empty ID for public key: %s", publicKey)
		return nil, fmt.Errorf("account data corruption: empty ID")
	}
	
	log.WithContext(context.Background()).Infof("Successfully found account: ID=%s, Name=%s, Status=%s", account.ID, account.Name, account.Status)
	return &account, nil
}

func (r *AccountRepository) DeleteAccount(id string) error {
	log.WithContext(context.Background()).Infof("Deleting account: %s", id)
	return r.db.Delete(&models.Account{}, "id = ?", id).Error
}

// GetAccounts returns paginated list of accounts with filters
func (r *AccountRepository) GetAccounts(req *models.AccountListRequest) (*models.AccountListResponse, error) {
	if req == nil {
		req = &models.AccountListRequest{}
	}
	
	if req.Limit <= 0 {
		req.Limit = 10
	}
	
	var accounts []*models.Account
	var total int64
	
	// Build query with filters
	query := r.db.Model(&models.Account{})
	
	// Apply search filter
	if req.Search != "" {
		query = query.Where("name ILIKE ? OR description ILIKE ?", "%"+req.Search+"%", "%"+req.Search+"%")
	}
	
	// Apply status filter
	if req.Status != "" {
		query = query.Where("status = ?", req.Status)
	}
	
	// Apply account type filter
	if req.AccountType != "" {
		if req.AccountType == "system" {
			query = query.Where("is_system_account = ?", true)
		} else if req.AccountType == "normal" {
			query = query.Where("is_system_account = ?", false)
		}
	}
	
	// Apply cluster ID filter
	if req.ClusterID != "" {
		query = query.Where("origin_cluster_id = ?", req.ClusterID)
	}
	
	// Apply legacy filter if provided
	if req.Filter != "" {
		query = query.Where("name ILIKE ? OR public_key ILIKE ?", "%"+req.Filter+"%", "%"+req.Filter+"%")
	}
	
	// Get total count
	query.Count(&total)
	
	// Apply sorting
	sortBy := "created_at"
	order := "DESC"
	
	if req.SortBy != "" {
		switch req.SortBy {
		case "name", "created_at", "updated_at", "status":
			sortBy = req.SortBy
		}
	}
	
	if req.Order != "" {
		if req.Order == "asc" || req.Order == "ASC" {
			order = "ASC"
		}
	}
	
	// Get paginated results
	resultQuery := r.db.Limit(req.Limit).Offset(req.Offset)
	
	// Apply same filters to result query
	if req.Search != "" {
		resultQuery = resultQuery.Where("name ILIKE ? OR description ILIKE ?", "%"+req.Search+"%", "%"+req.Search+"%")
	}
	if req.Status != "" {
		resultQuery = resultQuery.Where("status = ?", req.Status)
	}
	if req.AccountType != "" {
		if req.AccountType == "system" {
			resultQuery = resultQuery.Where("is_system_account = ?", true)
		} else if req.AccountType == "normal" {
			resultQuery = resultQuery.Where("is_system_account = ?", false)
		}
	}
	if req.ClusterID != "" {
		resultQuery = resultQuery.Where("origin_cluster_id = ?", req.ClusterID)
	}
	if req.Filter != "" {
		resultQuery = resultQuery.Where("name ILIKE ? OR public_key ILIKE ?", "%"+req.Filter+"%", "%"+req.Filter+"%")
	}
	
	// Apply sorting and execute
	err := resultQuery.Order(fmt.Sprintf("%s %s", sortBy, order)).Find(&accounts).Error
	
	return &models.AccountListResponse{
		Data:   accounts,
		Total:  total,
		Limit:  req.Limit,
		Offset: req.Offset,
	}, err
}