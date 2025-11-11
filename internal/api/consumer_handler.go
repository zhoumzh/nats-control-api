package api

import (
	"context"
	"nats-control-api/internal/service"
	"nats-control-api/pkg/models"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type ConsumerHandler struct {
	consumerService *service.ConsumerManageService
}

func NewConsumerHandler(consumerService *service.ConsumerManageService) *ConsumerHandler {
	return &ConsumerHandler{
		consumerService: consumerService,
	}
}

// CreateConsumer creates a new Consumer
// @Summary Create a new Consumer
// @Description Create a new Consumer on the specified JetStream
// @Tags Consumer
// @Accept json
// @Produce json
// @Param consumer body models.CreateConsumerRequest true "Consumer configuration"
// @Success 201 {object} models.Consumer
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers [post]
func (h *ConsumerHandler) CreateConsumer(c *gin.Context) {
	var req models.CreateConsumerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("请求绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid request format: " + err.Error(),
		})
		return
	}

	consumer, err := h.consumerService.CreateConsumer(&req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer创建失败: %v", err)

		if validationErr, ok := err.(*models.ValidationError); ok {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error: validationErr.Error(),
			})
			return
		}

		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to create Consumer: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer通过API创建成功: consumer_id=%s, consumer_name=%s, jetstream_id=%s", consumer.ID, consumer.Name, consumer.JetStreamID)

	c.JSON(http.StatusCreated, consumer)
}

// ListConsumers retrieves Consumers with optional filters
// @Summary List Consumers
// @Description Retrieve a list of Consumers with optional filtering and pagination
// @Tags Consumer
// @Accept json
// @Produce json
// @Param cluster_id query string false "Filter by Cluster ID"
// @Param jetstream_id query string false "Filter by JetStream ID"
// @Param nats_operate_user_id query string false "Filter by NATS operate user ID"
// @Param consumer_type query string false "Filter by consumer type (pull, push)"
// @Param status query string false "Filter by status (active, paused)"
// @Param sync_status query string false "Filter by sync status (pending, synced, failed)"
// @Param search query string false "Search by name or description"
// @Param is_durable query bool false "Filter by durability (true, false)"
// @Param page query int false "Page number (default: 1)"
// @Param page_size query int false "Page size (default: 20, max: 100)"
// @Success 200 {object} models.PaginatedResponse{data=[]models.Consumer}
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers [get]
func (h *ConsumerHandler) ListConsumers(c *gin.Context) {
	var req models.ConsumerListRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("查询参数绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid query parameters: " + err.Error(),
		})
		return
	}

	consumers, total, err := h.consumerService.ListConsumers(&req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer列表获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to list Consumers: " + err.Error(),
		})
		return
	}

	page := req.Page
	if page <= 0 {
		page = 1
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 20
	}

	c.JSON(http.StatusOK, models.PaginatedResponse{
		Data:       consumers,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: (total + int64(pageSize) - 1) / int64(pageSize),
	})
}

// GetConsumer retrieves a specific Consumer by ID
// @Summary Get a Consumer
// @Description Retrieve detailed information about a specific Consumer
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Success 200 {object} models.Consumer
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id} [get]
func (h *ConsumerHandler) GetConsumer(c *gin.Context) {
	id := c.Param("id")

	consumer, err := h.consumerService.GetConsumer(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer获取失败: consumer_id=%s, error=%v", id, err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to get Consumer: " + err.Error(),
		})
		return
	}

	if consumer == nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Consumer not found",
		})
		return
	}

	c.JSON(http.StatusOK, consumer)
}

// UpdateConsumer updates an existing Consumer
// @Summary Update a Consumer
// @Description Update configuration of an existing Consumer
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Param consumer body models.UpdateConsumerRequest true "Consumer update configuration"
// @Success 200 {object} models.Consumer
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id} [put]
func (h *ConsumerHandler) UpdateConsumer(c *gin.Context) {
	id := c.Param("id")

	var req models.UpdateConsumerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("请求绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid request format: " + err.Error(),
		})
		return
	}

	consumer, err := h.consumerService.UpdateConsumer(id, &req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer更新失败: consumer_id=%s, error=%v", id, err)

		if validationErr, ok := err.(*models.ValidationError); ok {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error: validationErr.Error(),
			})
			return
		}

		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to update Consumer: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer通过API更新成功: consumer_id=%s", id)

	c.JSON(http.StatusOK, consumer)
}

// DeleteConsumer deletes a Consumer
// @Summary Delete a Consumer
// @Description Delete a Consumer from the system and NATS cluster
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Success 204 "Consumer deleted successfully"
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id} [delete]
func (h *ConsumerHandler) DeleteConsumer(c *gin.Context) {
	id := c.Param("id")

	err := h.consumerService.DeleteConsumer(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer删除失败: consumer_id=%s, error=%v", id, err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to delete Consumer: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer通过API删除成功: consumer_id=%s", id)

	c.Status(http.StatusNoContent)
}

// RetryConsumerSync retries failed Consumer synchronization
// @Summary Retry Consumer sync
// @Description Retry synchronization for a Consumer that failed to sync to NATS
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Success 200 {object} map[string]string
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id}/retry [post]
func (h *ConsumerHandler) RetryConsumerSync(c *gin.Context) {
	id := c.Param("id")

	err := h.consumerService.RetryFailedSync(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer同步重试失败: consumer_id=%s, error=%v", id, err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to retry Consumer sync: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer同步重试成功启动: consumer_id=%s", id)

	c.JSON(http.StatusOK, gin.H{
		"message": "Consumer sync retry initiated successfully",
	})
}

// PauseConsumer pauses a Consumer
// @Summary Pause a Consumer
// @Description Pause message delivery for a Consumer
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Param pause_until body object false "Pause until timestamp (optional)"
// @Success 200 {object} map[string]string
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id}/pause [post]
func (h *ConsumerHandler) PauseConsumer(c *gin.Context) {
	id := c.Param("id")

	var req struct {
		PauseUntil *time.Time `json:"pause_until"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Warnf("请求绑定失败（使用默认暂停时间）: %v", err)
	}

	err := h.consumerService.PauseConsumer(id, req.PauseUntil)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer暂停失败: consumer_id=%s, error=%v", id, err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to pause Consumer: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer暂停成功: consumer_id=%s", id)

	c.JSON(http.StatusOK, gin.H{
		"message": "Consumer paused successfully",
	})
}

// ResumeConsumer resumes a paused Consumer
// @Summary Resume a Consumer
// @Description Resume message delivery for a paused Consumer
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Success 200 {object} map[string]string
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id}/resume [post]
func (h *ConsumerHandler) ResumeConsumer(c *gin.Context) {
	id := c.Param("id")

	err := h.consumerService.ResumeConsumer(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer恢复失败: consumer_id=%s, error=%v", id, err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to resume Consumer: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer恢复成功: consumer_id=%s", id)

	c.JSON(http.StatusOK, gin.H{
		"message": "Consumer resumed successfully",
	})
}

// GetConsumerStats retrieves Consumer statistics
// @Summary Get Consumer statistics
// @Description Get aggregated statistics for all Consumers
// @Tags Consumer
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/stats [get]
func (h *ConsumerHandler) GetConsumerStats(c *gin.Context) {
	_, total, err := h.consumerService.ListConsumers(&models.ConsumerListRequest{
		Page:     1,
		PageSize: 1,
	})
	if err != nil {
		log.WithContext(context.Background()).Errorf("Consumer统计信息获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to get Consumer stats: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total": total,
	})
}

// CompareConsumerConfig compares Consumer configuration between database and NATS cluster
// @Summary Compare Consumer configuration
// @Description Compare Consumer configuration between database and NATS cluster to detect differences
// @Tags Consumer
// @Accept json
// @Produce json
// @Param id path string true "Consumer ID"
// @Success 200 {object} models.ConsumerConfigDiff
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /consumers/{id}/diff [get]
func (h *ConsumerHandler) CompareConsumerConfig(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Consumer ID is required",
		})
		return
	}

	diff, err := h.consumerService.CompareConsumerConfig(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer配置对比失败: %v", err)
		if err.Error() == "Consumer not found" {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error: "Consumer not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to compare Consumer config: " + err.Error(),
		})
		return
	}

	if diff.HasDifference {
		log.WithContext(c.Request.Context()).Infof("Consumer配置差异检测完成: consumer_id=%s, 发现%d个差异", id, len(diff.Differences))
	} else {
		log.WithContext(c.Request.Context()).Infof("Consumer配置一致: consumer_id=%s", id)
	}

	c.JSON(http.StatusOK, diff)
}