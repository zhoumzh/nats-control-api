package db

import (
	"fmt"

	"nats-control-api/pkg/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Repository struct {
	db              *gorm.DB
	ClusterRepo     *ClusterRepository
	AccountRepo     *AccountRepository
	UserRepo        *UserRepository
	JWTTaskRepo     *JWTTaskRepository
	JetStreamRepo   *JetStreamRepository
	ConsumerRepo    *ConsumerRepository
}

func NewRepository(driver, dsn string) (*Repository, error) {
	var dialector gorm.Dialector

	switch driver {
	case "sqlite3":
		dialector = sqlite.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.AutoMigrate(&models.Account{}, &models.User{}, &models.JWTTask{}, &models.Cluster{}, &models.ClusterHealth{}, &models.JetStream{}, &models.JetStreamMirror{}, &models.JetStreamSource{}, &models.Consumer{}); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	repo := &Repository{
		db:            db,
		ClusterRepo:   NewClusterRepository(db),
		AccountRepo:   NewAccountRepository(db),
		UserRepo:      NewUserRepository(db),
		JWTTaskRepo:   NewJWTTaskRepository(db),
		JetStreamRepo: NewJetStreamRepository(db),
		ConsumerRepo:  NewConsumerRepository(db),
	}

	return repo, nil
}

func (r *Repository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// GetDB returns the underlying GORM database instance
func (r *Repository) GetDB() *gorm.DB {
	return r.db
}

// Account operations - delegate to AccountRepository
func (r *Repository) CreateAccount(account *models.Account) error {
	return r.AccountRepo.CreateAccount(account)
}

func (r *Repository) GetAccount(id string) (*models.Account, error) {
	return r.AccountRepo.GetAccount(id)
}

func (r *Repository) GetAccountByName(name string) (*models.Account, error) {
	return r.AccountRepo.GetAccountByName(name)
}

func (r *Repository) GetAccountByPublicKey(publicKey string) (*models.Account, error) {
	return r.AccountRepo.GetAccountByPublicKey(publicKey)
}

func (r *Repository) UpdateAccount(account *models.Account) error {
	return r.AccountRepo.UpdateAccount(account)
}

func (r *Repository) UpdateAccountFields(accountID string, fields map[string]interface{}) error {
	return r.AccountRepo.UpdateAccountFields(accountID, fields)
}

func (r *Repository) ListAccounts(limit, offset int) ([]*models.Account, error) {
	return r.AccountRepo.ListAccounts(limit, offset)
}

// GetAccounts returns paginated list of accounts with filters
func (r *Repository) GetAccounts(req *models.AccountListRequest) (*models.AccountListResponse, error) {
	return r.AccountRepo.GetAccounts(req)
}

func (r *Repository) DeleteAccount(id string) error {
	return r.AccountRepo.DeleteAccount(id)
}

// User operations - delegate to UserRepository
func (r *Repository) CreateUser(user *models.User) error {
	return r.UserRepo.CreateUser(user)
}

func (r *Repository) GetUser(id string) (*models.User, error) {
	return r.UserRepo.GetUser(id)
}

func (r *Repository) GetUserByName(accountID, name string) (*models.User, error) {
	return r.UserRepo.GetUserByName(accountID, name)
}

func (r *Repository) UpdateUser(user *models.User) error {
	return r.UserRepo.UpdateUser(user)
}

func (r *Repository) UpdateUserFields(userID string, fields map[string]interface{}) error {
	return r.UserRepo.UpdateUserFields(userID, fields)
}

func (r *Repository) ListUsers(accountID string, limit, offset int) ([]*models.User, error) {
	return r.UserRepo.ListUsers(accountID, limit, offset)
}

func (r *Repository) GetUsersByAccountID(accountID string) ([]*models.User, error) {
	return r.UserRepo.GetUsersByAccountID(accountID)
}

func (r *Repository) CountUsersByAccountID(accountID string) (int64, error) {
	return r.UserRepo.CountUsersByAccountID(accountID)
}

func (r *Repository) GetFirstAdminUser() (*models.User, error) {
	return r.UserRepo.GetFirstAdminUser()
}

func (r *Repository) GetFirstAdminUserByAccountID(accountID string) (*models.User, error) {
	return r.UserRepo.GetFirstAdminUserByAccountID(accountID)
}

func (r *Repository) ListAdminUsersFromNonSystemAccounts() ([]*models.User, error) {
	return r.UserRepo.ListAdminUsersFromNonSystemAccounts()
}

func (r *Repository) GetAllUsers(accountID, status string, limit, offset int) ([]*models.User, error) {
	return r.UserRepo.GetAllUsers(accountID, status, limit, offset)
}

// JWT Task operations - delegate to JWTTaskRepository
func (r *Repository) CreateJWTTask(task *models.JWTTask) error {
	return r.JWTTaskRepo.CreateJWTTask(task)
}

func (r *Repository) GetJWTTask(id string) (*models.JWTTask, error) {
	return r.JWTTaskRepo.GetJWTTask(id)
}

func (r *Repository) UpdateJWTTask(task *models.JWTTask) error {
	return r.JWTTaskRepo.UpdateJWTTask(task)
}

func (r *Repository) ListPendingJWTTasks(limit int) ([]*models.JWTTask, error) {
	return r.JWTTaskRepo.ListPendingJWTTasks(limit)
}

func (r *Repository) ListJWTTasksByStatus(status models.TaskStatus, limit int) ([]*models.JWTTask, error) {
	return r.JWTTaskRepo.ListJWTTasksByStatus(status, limit)
}

func (r *Repository) RetryJWTTask(taskID string) error {
	return r.JWTTaskRepo.RetryJWTTask(taskID)
}

func (r *Repository) ListAllJWTTasks(limit, offset int) ([]*models.JWTTask, error) {
	return r.JWTTaskRepo.ListAllJWTTasks(limit, offset)
}

func (r *Repository) CountJWTTasksByAccountPublicKey(publicKey string) (int64, error) {
	return r.JWTTaskRepo.CountJWTTasksByAccountPublicKey(publicKey)
}

// JetStream operations - delegate to JetStreamRepository
func (r *Repository) CreateJetStream(js *models.JetStream) error {
	return r.JetStreamRepo.CreateJetStream(js)
}

func (r *Repository) GetJetStream(id string) (*models.JetStream, error) {
	return r.JetStreamRepo.GetJetStream(id)
}

func (r *Repository) GetJetStreamByNameAndUser(name, natsOperateUserID string) (*models.JetStream, error) {
	return r.JetStreamRepo.GetJetStreamByNameAndUser(name, natsOperateUserID)
}

func (r *Repository) GetJetStreamByNameAndCluster(name, clusterID string) (*models.JetStream, error) {
	return r.JetStreamRepo.GetJetStreamByNameAndCluster(name, clusterID)
}

func (r *Repository) ListJetStreams(natsOperateUserID, status, search, clusterID, syncStatus string, limit, offset int) ([]*models.JetStream, int64, error) {
	return r.JetStreamRepo.ListJetStreams(natsOperateUserID, status, search, clusterID, syncStatus, limit, offset)
}

func (r *Repository) UpdateJetStream(js *models.JetStream) error {
	return r.JetStreamRepo.UpdateJetStream(js)
}

func (r *Repository) UpdateJetStreamFields(id string, fields map[string]interface{}) error {
	return r.JetStreamRepo.UpdateJetStreamFields(id, fields)
}

func (r *Repository) DeleteJetStream(id string) error {
	return r.JetStreamRepo.DeleteJetStream(id)
}

func (r *Repository) GetJetStreamStats() (map[string]interface{}, error) {
	return r.JetStreamRepo.GetJetStreamStats()
}

// Health check
func (r *Repository) HealthCheck() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

// Cluster operations - delegate to ClusterRepository
func (r *Repository) CreateCluster(cluster *models.Cluster) error {
	return r.ClusterRepo.CreateCluster(cluster)
}

func (r *Repository) GetClusterByID(id string) (*models.Cluster, error) {
	return r.ClusterRepo.GetClusterByID(id)
}

func (r *Repository) ListClusters(status string) ([]*models.Cluster, error) {
	return r.ClusterRepo.ListClusters(status)
}

func (r *Repository) UpdateCluster(cluster *models.Cluster) error {
	return r.ClusterRepo.UpdateCluster(cluster)
}

func (r *Repository) DeleteCluster(id string) error {
	return r.ClusterRepo.DeleteCluster(id)
}

func (r *Repository) ListActiveClusters() ([]*models.Cluster, error) {
	return r.ClusterRepo.GetActiveClusters()
}

// Consumer operations - delegate to ConsumerRepository
func (r *Repository) CreateConsumer(consumer *models.Consumer) error {
	return r.ConsumerRepo.CreateConsumer(consumer)
}

func (r *Repository) GetConsumer(id string) (*models.Consumer, error) {
	return r.ConsumerRepo.GetConsumer(id)
}

func (r *Repository) GetConsumerByNameAndJetStream(name, jetstreamID string) (*models.Consumer, error) {
	return r.ConsumerRepo.GetConsumerByNameAndJetStream(name, jetstreamID)
}

func (r *Repository) ListConsumers(clusterID, jetstreamID, natsOperateUserID, consumerType, status, syncStatus, search string, isDurable *bool, limit, offset int) ([]*models.Consumer, int64, error) {
	return r.ConsumerRepo.ListConsumers(clusterID, jetstreamID, natsOperateUserID, consumerType, status, syncStatus, search, isDurable, limit, offset)
}

func (r *Repository) UpdateConsumer(consumer *models.Consumer) error {
	return r.ConsumerRepo.UpdateConsumer(consumer)
}

func (r *Repository) UpdateConsumerFields(id string, fields map[string]interface{}) error {
	return r.ConsumerRepo.UpdateConsumerFields(id, fields)
}

func (r *Repository) DeleteConsumer(id string) error {
	return r.ConsumerRepo.DeleteConsumer(id)
}

func (r *Repository) ListConsumersBySyncStatus(syncStatus models.ConsumerSyncStatus, limit, offset int) ([]*models.Consumer, int64, error) {
	return r.ConsumerRepo.ListConsumersBySyncStatus(syncStatus, limit, offset)
}

func (r *Repository) GetConsumerStats() (map[string]interface{}, error) {
	return r.ConsumerRepo.GetConsumerStats()
}

func (r *Repository) ListConsumersByJetStream(jetstreamID string) ([]*models.Consumer, error) {
	return r.ConsumerRepo.ListConsumersByJetStream(jetstreamID)
}
