package service

import (
	"fmt"
	"nats-control-api/internal/db"
	"nats-control-api/pkg/models"
)

type JWTTaskService struct {
	repository *db.Repository
}

func NewJWTTaskService(repository *db.Repository) *JWTTaskService {
	return &JWTTaskService{
		repository: repository,
	}
}

// GetJWTTask retrieves a JWT task by ID
func (s *JWTTaskService) GetJWTTask(id string) (*models.JWTTask, error) {
	return s.repository.GetJWTTask(id)
}

// ListJWTTasks lists JWT tasks with optional status filter
func (s *JWTTaskService) ListJWTTasks(status string, limit, offset int) ([]*models.JWTTask, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	if status != "" {
		taskStatus := models.TaskStatus(status)
		// Validate status
		switch taskStatus {
		case models.TaskStatusPending, models.TaskStatusProcessing, 
			 models.TaskStatusCompleted, models.TaskStatusFailed, models.TaskStatusRetrying:
			return s.repository.ListJWTTasksByStatus(taskStatus, limit)
		default:
			return nil, fmt.Errorf("invalid task status: %s", status)
		}
	}

	// Return all tasks if no status filter
	return s.repository.ListAllJWTTasks(limit, offset)
}

// ListFailedJWTTasks lists all failed JWT tasks
func (s *JWTTaskService) ListFailedJWTTasks(limit int) ([]*models.JWTTask, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.repository.ListJWTTasksByStatus(models.TaskStatusFailed, limit)
}

// RetryJWTTask retries a failed JWT task
func (s *JWTTaskService) RetryJWTTask(id string) error {
	// First check if task exists and is in failed status
	task, err := s.repository.GetJWTTask(id)
	if err != nil {
		return fmt.Errorf("task not found: %w", err)
	}

	if task.Status != models.TaskStatusFailed {
		return fmt.Errorf("task %s is not in failed status (current: %s)", id, task.Status)
	}

	// Reset task to pending status
	return s.repository.RetryJWTTask(id)
}

// GetJWTTaskStats returns task statistics
func (s *JWTTaskService) GetJWTTaskStats() (*JWTTaskStats, error) {
	stats := &JWTTaskStats{}
	
	// Count tasks by status
	pendingTasks, err := s.repository.ListJWTTasksByStatus(models.TaskStatusPending, 1000)
	if err != nil {
		return nil, err
	}
	stats.Pending = len(pendingTasks)

	processingTasks, err := s.repository.ListJWTTasksByStatus(models.TaskStatusProcessing, 1000)
	if err != nil {
		return nil, err
	}
	stats.Processing = len(processingTasks)

	completedTasks, err := s.repository.ListJWTTasksByStatus(models.TaskStatusCompleted, 1000)
	if err != nil {
		return nil, err
	}
	stats.Completed = len(completedTasks)

	failedTasks, err := s.repository.ListJWTTasksByStatus(models.TaskStatusFailed, 1000)
	if err != nil {
		return nil, err
	}
	stats.Failed = len(failedTasks)

	retryingTasks, err := s.repository.ListJWTTasksByStatus(models.TaskStatusRetrying, 1000)
	if err != nil {
		return nil, err
	}
	stats.Retrying = len(retryingTasks)

	stats.Total = stats.Pending + stats.Processing + stats.Completed + stats.Failed + stats.Retrying

	return stats, nil
}

// JWTTaskStats represents task statistics
type JWTTaskStats struct {
	Total      int `json:"total"`
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
	Retrying   int `json:"retrying"`
}