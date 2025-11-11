package db

import (
	"context"
	"fmt"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/pkg/models"

	"gorm.io/gorm"
)

type UserRepository struct {
	db *gorm.DB
}

func NewUserRepository(db *gorm.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) CreateUser(user *models.User) error {
	log.WithContext(context.Background()).Infof("Creating user: %+v", user)
	return r.db.Create(user).Error
}

func (r *UserRepository) GetUser(id string) (*models.User, error) {
	log.WithContext(context.Background()).Infof("Getting user with ID: %s", id)
	var user models.User
	err := r.db.Where("id = ?", id).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

func (r *UserRepository) GetUserByName(accountID, name string) (*models.User, error) {
	log.WithContext(context.Background()).Infof("Getting user with account ID: %s, name: %s", accountID, name)
	var user models.User
	err := r.db.Where("account_id = ? AND name = ?", accountID, name).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

func (r *UserRepository) UpdateUser(user *models.User) error {
	log.WithContext(context.Background()).Infof("Updating user: %+v", user)
	return r.db.Save(user).Error
}

func (r *UserRepository) UpdateUserFields(userID string, fields map[string]interface{}) error {
	log.WithContext(context.Background()).Infof("Updating user %s with fields: %+v", userID, fields)
	return r.db.Model(&models.User{}).Where("id = ?", userID).Updates(fields).Error
}

func (r *UserRepository) GetUsersByAccountID(accountID string) ([]*models.User, error) {
	log.WithContext(context.Background()).Infof("Getting all users for account ID: %s", accountID)
	
	// 增强参数验证
	if accountID == "" {
		log.WithContext(context.Background()).Warn("Account ID is empty")
		return nil, fmt.Errorf("account ID cannot be empty")
	}
	
	var users []*models.User
	err := r.db.Where("account_id = ?", accountID).Find(&users).Error
	if err != nil {
		log.WithContext(context.Background()).Errorf("Database error when getting users for account %s: %v", accountID, err)
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	
	// 统计和记录用户状态
	activeCount := 0
	jwtCount := 0
	for _, user := range users {
		if user.Status == models.UserStatusActive {
			activeCount++
		}
		if user.JWTText != nil && *user.JWTText != "" {
			jwtCount++
		}
	}
	
	log.WithContext(context.Background()).Infof("Found %d users for account %s (active: %d, with JWT: %d)", 
		len(users), accountID, activeCount, jwtCount)
	
	return users, err
}

func (r *UserRepository) ListUsers(accountID string, limit, offset int) ([]*models.User, error) {
	log.WithContext(context.Background()).Infof("Listing users for account: %s with limit: %d, offset: %d", accountID, limit, offset)
	var users []*models.User
	err := r.db.Where("account_id = ?", accountID).Limit(limit).Offset(offset).Find(&users).Error
	return users, err
}

func (r *UserRepository) GetFirstAdminUser() (*models.User, error) {
	log.WithContext(context.Background()).Infof("Getting first admin user")
	var user models.User
	err := r.db.Where("is_admin = ? AND status = ?", true, models.UserStatusActive).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

func (r *UserRepository) GetFirstAdminUserByAccountID(accountID string) (*models.User, error) {
	log.WithContext(context.Background()).Infof("获取系统账号: %s的管理员用户", accountID)
	
	// 增强参数验证
	if accountID == "" {
		log.WithContext(context.Background()).Warn("Account ID is empty when looking for admin user")
		return nil, fmt.Errorf("account ID cannot be empty")
	}
	
	var user models.User
	err := r.db.Where("account_id = ? AND is_admin = ? AND status = ?", accountID, true, models.UserStatusActive).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.WithContext(context.Background()).Infof("No active admin user found for account %s", accountID)
			return nil, nil
		}
		log.WithContext(context.Background()).Errorf("Database error when getting admin user for account %s: %v", accountID, err)
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	
	// 额外的数据完整性检查
	if user.ID == "" {
		log.WithContext(context.Background()).Warnf("Found admin user with empty ID for account: %s", accountID)
		return nil, fmt.Errorf("admin user data corruption: empty ID")
	}
	
	// 检查JWT状态
	hasJWT := user.JWTText != nil && *user.JWTText != ""
	log.WithContext(context.Background()).Infof("Found admin user: ID=%s, Name=%s, HasJWT=%v for account %s", 
		user.ID, user.Name, hasJWT, accountID)
	
	return &user, nil
}

// ListAdminUsersFromNonSystemAccounts 获取所有非系统账号的管理员用户
func (r *UserRepository) ListAdminUsersFromNonSystemAccounts() ([]*models.User, error) {
	log.WithContext(context.Background()).Info("Getting admin users from non-system accounts")
	
	var users []*models.User
	err := r.db.Joins("JOIN accounts ON users.account_id = accounts.id").
		Where("users.is_admin = ? AND users.status = ? AND accounts.is_system_account = ?", 
			true, models.UserStatusActive, false).
		Select("users.*").
		Find(&users).Error
	
	if err != nil {
		log.WithContext(context.Background()).Errorf("Database error when getting admin users from non-system accounts: %v", err)
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	
	log.WithContext(context.Background()).Infof("Found %d admin users from non-system accounts", len(users))
	
	return users, nil
}

func (r *UserRepository) CountUsersByAccountID(accountID string) (int64, error) {
	log.WithContext(context.Background()).Infof("Counting users for account ID: %s", accountID)
	var count int64
	err := r.db.Model(&models.User{}).Where("account_id = ?", accountID).Count(&count).Error
	return count, err
}

func (r *UserRepository) DeleteUser(id string) error {
	log.WithContext(context.Background()).Infof("Deleting user: %s", id)
	return r.db.Delete(&models.User{}, "id = ?", id).Error
}

func (r *UserRepository) GetAllUsers(accountID, status string, limit, offset int) ([]*models.User, error) {
	log.WithContext(context.Background()).Infof("Getting all users with accountID: %s, status: %s, limit: %d, offset: %d", accountID, status, limit, offset)
	
	var users []*models.User
	query := r.db.Model(&models.User{})
	
	if accountID != "" {
		query = query.Where("account_id = ?", accountID)
	}
	
	if status != "" {
		query = query.Where("status = ?", status)
	}
	
	err := query.Limit(limit).Offset(offset).Find(&users).Error
	if err != nil {
		log.WithContext(context.Background()).Errorf("Database error when getting all users: %v", err)
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	
	log.WithContext(context.Background()).Infof("Found %d users", len(users))
	
	return users, nil
}
