package db

import (
	"context"
	"fmt"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/pkg/models"

	"gorm.io/gorm"
)

type JWTTaskRepository struct {
	db *gorm.DB
}

func NewJWTTaskRepository(db *gorm.DB) *JWTTaskRepository {
	return &JWTTaskRepository{db: db}
}

func (r *JWTTaskRepository) CreateJWTTask(task *models.JWTTask) error {
	log.WithContext(context.Background()).Infof("Creating JWT task: %+v", task)
	return r.db.Create(task).Error
}

func (r *JWTTaskRepository) GetJWTTask(id string) (*models.JWTTask, error) {
	log.WithContext(context.Background()).Infof("Getting JWT task with ID: %s", id)
	var task models.JWTTask
	err := r.db.Where("id = ?", id).First(&task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

func (r *JWTTaskRepository) UpdateJWTTask(task *models.JWTTask) error {
	log.WithContext(context.Background()).Infof("Updating JWT task: %+v", task)
	return r.db.Save(task).Error
}

func (r *JWTTaskRepository) ListPendingJWTTasks(limit int) ([]*models.JWTTask, error) {
	//log.WithContext(context.Background()).Infof("Listing pending JWT tasks with limit: %d", limit)
	var tasks []*models.JWTTask
	err := r.db.Where("status IN ?", []models.TaskStatus{
		models.TaskStatusPending,
		models.TaskStatusRetrying,
	}).Limit(limit).Find(&tasks).Error
	return tasks, err
}

// ListJWTTasksByStatus returns JWT tasks with specific status
func (r *JWTTaskRepository) ListJWTTasksByStatus(status models.TaskStatus, limit int) ([]*models.JWTTask, error) {
	log.WithContext(context.Background()).Infof("Listing JWT tasks with status: %s, limit: %d", status, limit)
	var tasks []*models.JWTTask
	err := r.db.Where("status = ?", status).Limit(limit).Order("updated_at DESC").Find(&tasks).Error
	return tasks, err
}

// RetryJWTTask resets a failed task to pending status for retry
func (r *JWTTaskRepository) RetryJWTTask(taskID string) error {
	log.WithContext(context.Background()).Infof("Retrying JWT task: %s", taskID)
	result := r.db.Model(&models.JWTTask{}).Where("id = ? AND status = ?", taskID, models.TaskStatusFailed).Updates(map[string]interface{}{
		"status":  models.TaskStatusPending,
		"retries": 0,
		"error":   "",
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("task not found or not in failed status")
	}
	return nil
}

// ListAllJWTTasks returns all JWT tasks with pagination
func (r *JWTTaskRepository) ListAllJWTTasks(limit, offset int) ([]*models.JWTTask, error) {
	log.WithContext(context.Background()).Infof("Listing all JWT tasks with limit: %d, offset: %d", limit, offset)
	var tasks []*models.JWTTask
	err := r.db.Limit(limit).Offset(offset).Order("updated_at DESC").Find(&tasks).Error
	return tasks, err
}

func (r *JWTTaskRepository) CountJWTTasksByAccountPublicKey(publicKey string) (int64, error) {
	log.WithContext(context.Background()).Infof("Counting JWT tasks for account public key: %s", publicKey)
	var count int64
	err := r.db.Model(&models.JWTTask{}).Where("public_key = ?", publicKey).Count(&count).Error
	return count, err
}
