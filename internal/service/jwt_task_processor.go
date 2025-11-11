package service

import (
	"context"
	"fmt"
	"time"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/internal/db"
	"nats-control-api/pkg/models"
)

type JWTTaskProcessor struct {
	repo       *db.Repository
	jwtService *JWTService
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewJWTTaskProcessor(repo *db.Repository, jwtService *JWTService) *JWTTaskProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &JWTTaskProcessor{
		repo:       repo,
		jwtService: jwtService,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (p *JWTTaskProcessor) Start() {
	go p.run()
	log.Info("JWT任务处理器已启动")
}

func (p *JWTTaskProcessor) Stop() {
	p.cancel()
	log.Info("JWT任务处理器已停止")
}

func (p *JWTTaskProcessor) run() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.processPendingTasks()
		}
	}
}

func (p *JWTTaskProcessor) processPendingTasks() {
	//log.Printf("开始检查待处理的JWT任务...")
	tasks, err := p.repo.ListPendingJWTTasks(10)
	if err != nil {
		log.WithContext(p.ctx).Errorf("获取待处理JWT任务失败: %v", err)
		return
	}
	if len(tasks) == 0 {
		return
	}
	log.WithContext(p.ctx).Infof("找到 %d 个待处理的JWT任务", len(tasks))
	for _, task := range tasks {
		log.WithContext(p.ctx).Infof("处理任务: ID=%s, Status=%s, EntityType=%s, EntityID=%s, Operation=%s",
			task.ID, task.Status, task.EntityType, task.EntityID, task.Operation)
		p.processTask(task)
	}
}

func (p *JWTTaskProcessor) processTask(task *models.JWTTask) {
	task.Status = models.TaskStatusProcessing
	task.UpdatedAt = models.CustomTime{Time: time.Now()}

	if err := p.repo.UpdateJWTTask(task); err != nil {
		log.WithContext(p.ctx).Errorf("更新任务状态为处理中失败: %v", err)
		return
	}

	var err error
	switch task.EntityType {
	case models.EntityTypeAccount:
		err = p.processAccountTask(task)
	case models.EntityTypeUser:
		err = p.processUserTask(task)
	default:
		err = fmt.Errorf("未知的实体类型: %s", task.EntityType)
	}

	if err != nil {
		p.handleTaskError(task, err)
		return
	}

	task.Status = models.TaskStatusCompleted
	now := models.CustomTime{Time: time.Now()}
	task.CompletedAt = &now
	task.UpdatedAt = now
	task.Error = ""

	if err := p.repo.UpdateJWTTask(task); err != nil {
		log.WithContext(p.ctx).Errorf("更新任务状态为已完成失败: %v", err)
	} else {
		log.WithContext(p.ctx).Infof("任务 %s 成功完成", task.ID)
	}
}

func (p *JWTTaskProcessor) processAccountTask(task *models.JWTTask) error {
	// 直接从数据库获取最新的账户信息，而不是依赖可能不完整的payload
	// 这确保我们有完整的数据，包括NKey字段（payload中NKey会因json:"-"标签而丢失）
	account, err := p.repo.GetAccount(task.EntityID)
	if err != nil {
		return fmt.Errorf("从数据库获取账户失败: %w", err)
	}
	if account == nil {
		return fmt.Errorf("账户未找到: %s", task.EntityID)
	}

	// 检查是否为手动同步任务
	if task.TriggerType == models.TriggerTypeManual && task.ClusterIDs != nil {
		// 提取集群ID列表
		clusterIDInterface, exists := (*task.ClusterIDs)["cluster_ids"]
		if !exists {
			return fmt.Errorf("手动同步任务缺少集群ID列表")
		}

		clusterIDsSlice, ok := clusterIDInterface.([]interface{})
		if !ok {
			return fmt.Errorf("手动同步任务中集群ID格式无效")
		}

		clusterIDs := make([]string, len(clusterIDsSlice))
		for i, id := range clusterIDsSlice {
			clusterIDs[i] = id.(string)
		}

		log.WithContext(p.ctx).Infof("正在处理账户 %s 到集群的手动同步任务: %v", account.ID, clusterIDs)

		// 处理手动同步并获取结果
		syncResults, err := p.jwtService.ProcessManualSyncJWTTask(account, clusterIDs)
		if syncResults != nil {
			// 保存同步结果到任务记录
			task.SyncResults = syncResults
			task.UpdatedAt = models.CustomTime{Time: time.Now()}
			if updateErr := p.repo.UpdateJWTTask(task); updateErr != nil {
				log.WithContext(p.ctx).Errorf("更新任务同步结果失败: %v", updateErr)
			}

			// 检查同步结果，如果有集群失败则返回错误
			if summary, exists := (*syncResults)["summary"]; exists {
				if summaryMap, ok := summary.(map[string]interface{}); ok {
					if failed, exists := summaryMap["failed"]; exists {
						if failedCount, ok := failed.(int); ok && failedCount > 0 {
							return fmt.Errorf("手动同步部分失败: %d个集群同步失败", failedCount)
						}
					}
				}
			}
		}
		return err
	}

	// 标准的自动同步任务
	syncResults, err := p.jwtService.ProcessAccountJWTTask(account, string(task.Operation))
	if syncResults != nil {
		// 保存同步结果到任务记录
		task.SyncResults = syncResults
		task.UpdatedAt = models.CustomTime{Time: time.Now()}
		if updateErr := p.repo.UpdateJWTTask(task); updateErr != nil {
			log.WithContext(p.ctx).Errorf("更新任务同步结果失败: %v", updateErr)
		}

		// 检查同步结果，如果有集群失败则返回错误
		if summary, exists := (*syncResults)["summary"]; exists {
			if summaryMap, ok := summary.(map[string]interface{}); ok {
				if failed, exists := summaryMap["failed"]; exists {
					if failedCount, ok := failed.(int); ok && failedCount > 0 {
						return fmt.Errorf("JWT同步部分失败: %d个集群同步失败", failedCount)
					}
				}
			}
		}
	}
	return err
}

func (p *JWTTaskProcessor) processUserTask(task *models.JWTTask) error {
	// 直接从数据库获取最新的用户信息，而不是依赖可能不完整的payload
	// 这确保我们有完整的数据，包括NKey字段（payload中NKey会因json标签设置而可能丢失）
	user, err := p.repo.GetUser(task.EntityID)
	if err != nil {
		return fmt.Errorf("从数据库获取用户失败: %w", err)
	}
	if user == nil {
		return fmt.Errorf("用户未找到: %s", task.EntityID)
	}

	return p.jwtService.ProcessUserJWTTask(user, string(task.Operation))
}

func (p *JWTTaskProcessor) handleTaskError(task *models.JWTTask, taskErr error) {
	task.Retries++
	task.Error = taskErr.Error()
	task.UpdatedAt = models.CustomTime{Time: time.Now()}

	if task.Retries >= task.MaxRetries {
		task.Status = models.TaskStatusFailed
		log.WithContext(p.ctx).Errorf("任务 %s 在 %d 次重试后失败: %v", task.ID, task.Retries, taskErr)
	} else {
		task.Status = models.TaskStatusRetrying
		log.WithContext(p.ctx).Warnf("任务 %s 失败 (重试 %d/%d): %v", task.ID, task.Retries, task.MaxRetries, taskErr)
	}

	if err := p.repo.UpdateJWTTask(task); err != nil {
		log.WithContext(p.ctx).Errorf("更新任务错误状态失败: %v", err)
	}
}
