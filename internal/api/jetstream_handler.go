package api

import (
	"context"
	"nats-control-api/internal/service"
	"nats-control-api/pkg/models"
	"net/http"

	"github.com/gin-gonic/gin"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type JetStreamHandler struct {
	jetStreamService *service.JetStreamManageService
}

func NewJetStreamHandler(jetStreamService *service.JetStreamManageService) *JetStreamHandler {
	return &JetStreamHandler{
		jetStreamService: jetStreamService,
	}
}

// CreateJetStream creates a new JetStream
// @Summary Create a new JetStream
// @Description Create a new JetStream on the specified NATS cluster
// @Tags JetStream
// @Accept json
// @Produce json
// @Param jetstream body models.CreateJetStreamRequest true "JetStream configuration"
// @Success 201 {object} models.JetStream
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams [post]
func (h *JetStreamHandler) CreateJetStream(c *gin.Context) {
	var req models.CreateJetStreamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("请求绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid request format",
		})
		return
	}

	jetStream, err := h.jetStreamService.CreateJetStream(&req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream创建失败: %v", err)

		// Check if it's a validation error
		if validationErr, ok := err.(*models.ValidationError); ok {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error: validationErr.Error(),
			})
			return
		}

		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to create JetStream: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("JetStream通过API创建成功: jetstream_id=%s, jetstream_name=%s, nats_operate_user_id=%s", jetStream.ID, jetStream.Name, jetStream.NatsOperateUserID)

	c.JSON(http.StatusCreated, jetStream)
}

// ListJetStreams retrieves JetStreams with optional filters
// @Summary List JetStreams
// @Description Retrieve a list of JetStreams with optional filtering and pagination
// @Tags JetStream
// @Accept json
// @Produce json
// @Param nats_operate_user_id query string false "Filter by NATS operate user ID"
// @Param status query string false "Filter by status (active, inactive, error)"
// @Param sync_status query string false "Filter by sync status (pending, synced, failed)"
// @Param search query string false "Search by name or description"
// @Param page query int false "Page number (default: 1)"
// @Param page_size query int false "Page size (default: 20, max: 100)"
// @Param sort_by query string false "Sort field (created_at, name)"
// @Param order query string false "Sort order (asc, desc)"
// @Success 200 {object} models.PaginatedResponse{data=[]models.JetStream}
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams [get]
func (h *JetStreamHandler) ListJetStreams(c *gin.Context) {
	var req models.JetStreamListRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("查询参数绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid query parameters",
		})
		return
	}

	jetStreams, total, err := h.jetStreamService.ListJetStreams(&req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream列表获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to list JetStreams: " + err.Error(),
		})
		return
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}

	totalPages := (total + int64(req.PageSize) - 1) / int64(req.PageSize)

	response := models.PaginatedResponse{
		Data:       jetStreams,
		Total:      total,
		Page:       req.Page,
		PageSize:   req.PageSize,
		TotalPages: totalPages,
	}

	c.JSON(http.StatusOK, response)
}

// GetJetStream retrieves a specific JetStream by ID
// @Summary Get a JetStream
// @Description Retrieve a specific JetStream by its ID, including real-time statistics
// @Tags JetStream
// @Accept json
// @Produce json
// @Param id path string true "JetStream ID"
// @Success 200 {object} models.JetStreamResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/{id} [get]
func (h *JetStreamHandler) GetJetStream(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "JetStream ID is required",
		})
		return
	}

	jetStream, err := h.jetStreamService.GetJetStream(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream获取失败: %v", err)
		if err.Error() == "JetStream not found" {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error: "JetStream not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to get JetStream: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, jetStream)
}

// UpdateJetStream updates an existing JetStream
// @Summary Update a JetStream
// @Description Update an existing JetStream configuration
// @Tags JetStream
// @Accept json
// @Produce json
// @Param id path string true "JetStream ID"
// @Param jetstream body models.UpdateJetStreamRequest true "Updated JetStream configuration"
// @Success 200 {object} models.JetStream
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/{id} [put]
func (h *JetStreamHandler) UpdateJetStream(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "JetStream ID is required",
		})
		return
	}

	var req models.UpdateJetStreamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("请求绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid request format",
		})
		return
	}

	// 使用新的异步更新服务方法
	jetStream, err := h.jetStreamService.UpdateJetStream(id, &req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream更新失败: %v", err)
		if err.Error() == "jetstream not found: "+id {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error: "JetStream not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to update JetStream: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("JetStream通过API更新成功，异步同步中: jetstream_id=%s, jetstream_name=%s, nats_operate_user_id=%s", jetStream.ID, jetStream.Name, req.NatsOperateUserID)

	c.JSON(http.StatusOK, jetStream)
}

// DeleteJetStream deletes a JetStream
// @Summary Delete a JetStream
// @Description Delete a JetStream from the cluster (not from database)
// @Tags JetStream
// @Accept json
// @Produce json
// @Param id path string true "JetStream ID"
// @Success 204
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/{id} [delete]
func (h *JetStreamHandler) DeleteJetStream(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "JetStream ID is required",
		})
		return
	}

	// Get existing JetStream to obtain accountID
	existingJetStream, err := h.jetStreamService.GetJetStream(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("获取JetStream失败: %v", err)
		if err.Error() == "JetStream not found" {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error: "JetStream not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to get JetStream: " + err.Error(),
		})
		return
	}

	// Delete from database only (not from cluster)
	err = h.jetStreamService.DeleteJetStream(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream从数据库删除失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to delete JetStream from database: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("JetStream通过API从数据库删除成功: jetstream_id=%s, stream_name=%s", id, existingJetStream.Name)

	c.Status(http.StatusNoContent)
}

// CompareJetStreamConfig compares database config with cluster config and returns differences
// @Summary Compare JetStream configuration
// @Description Compare database configuration with cluster configuration and return differences
// @Tags JetStream
// @Accept json
// @Produce json
// @Param id path string true "JetStream ID"
// @Success 200 {object} models.JetStreamConfigDiff
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/{id}/diff [get]
func (h *JetStreamHandler) CompareJetStreamConfig(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "JetStream ID is required",
		})
		return
	}

	diff, err := h.jetStreamService.CompareJetStreamConfig(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream配置对比失败: %v", err)
		if err.Error() == "JetStream not found" {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error: "JetStream not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to compare JetStream config: " + err.Error(),
		})
		return
	}

	if diff.HasDifference {
		log.WithContext(c.Request.Context()).Infof("JetStream配置差异检测完成: jetstream_id=%s, 发现%d个差异", id, len(diff.Differences))
	} else {
		log.WithContext(c.Request.Context()).Infof("JetStream配置一致: jetstream_id=%s", id)
	}

	c.JSON(http.StatusOK, diff)
}

// GetJetStreamStats retrieves statistics for all JetStreams
// @Summary Get JetStream statistics
// @Description Retrieve aggregated statistics for all JetStreams
// @Tags JetStream
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/stats [get]
func (h *JetStreamHandler) GetJetStreamStats(c *gin.Context) {
	stats, err := h.jetStreamService.GetJetStreamStats("", "")
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream统计数据获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to get JetStream statistics: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// RetryJetStreamCreation retries creating a JetStream that failed NATS creation
// @Summary Retry JetStream creation
// @Description Retry creating a JetStream on NATS that previously failed but was saved to database
// @Tags JetStream
// @Accept json
// @Produce json
// @Param id path string true "JetStream ID"
// @Success 200 {object} models.JetStream
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/{id}/retry [post]
func (h *JetStreamHandler) RetryJetStreamCreation(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "JetStream ID is required",
		})
		return
	}

	// Use the original nats operate user ID for authentication
	err := h.jetStreamService.RetryFailedSync(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream同步重试失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to retry sync: " + err.Error(),
		})
		return
	}

	log.WithContext(c.Request.Context()).Infof("JetStream同步重试成功启动: jetstream_id=%s", id)

	c.JSON(http.StatusOK, map[string]interface{}{
		"message": "Sync retry started successfully",
		"id":      id,
	})
}

// GetJetStreamsByUser retrieves all JetStreams for a specific user
// @Summary Get JetStreams by user
// @Description Retrieve all JetStreams created by a specific user
// @Tags JetStream
// @Accept json
// @Produce json
// @Param id path string true "User ID"
// @Success 200 {object} []models.JetStream
// @Failure 500 {object} models.ErrorResponse
// @Router /users/{id}/jetstreams [get]
func (h *JetStreamHandler) GetJetStreamsByUser(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "User ID is required",
		})
		return
	}

	req := &models.JetStreamListRequest{
		NatsOperateUserID: userID,
		Page:              1,
		PageSize:          100,
	}

	jetStreams, _, err := h.jetStreamService.ListJetStreams(req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("用户JetStream获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to get JetStreams: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, jetStreams)
}

// BatchDeleteJetStreams deletes multiple JetStreams
// @Summary Batch delete JetStreams
// @Description Delete multiple JetStreams in a single request
// @Tags JetStream
// @Accept json
// @Produce json
// @Param ids body []string true "Array of JetStream IDs to delete"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/batch-delete [post]
func (h *JetStreamHandler) BatchDeleteJetStreams(c *gin.Context) {
	var ids []string
	if err := c.ShouldBindJSON(&ids); err != nil {
		log.WithContext(c.Request.Context()).Errorf("请求绑定失败: %v", err)
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid request format, expected array of IDs",
		})
		return
	}

	if len(ids) == 0 {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "At least one ID is required",
		})
		return
	}

	if len(ids) > 50 {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Maximum 50 JetStreams can be deleted at once",
		})
		return
	}

	results := make(map[string]interface{})
	successCount := 0
	errors := make([]string, 0)

	for _, id := range ids {
		if err := h.jetStreamService.DeleteJetStream(id); err != nil {
			log.WithContext(context.Background()).Errorf("JetStream删除失败: id=%s, error=%v", id, err)
			errors = append(errors, "Failed to delete "+id+": "+err.Error())
		} else {
			successCount++
		}
	}

	results["total"] = len(ids)
	results["success"] = successCount
	results["failed"] = len(errors)
	if len(errors) > 0 {
		results["errors"] = errors
	}

	log.WithContext(c.Request.Context()).Infof("批量JetStream删除完成: total=%d, success=%d, failed=%d", len(ids), successCount, len(errors))

	if len(errors) > 0 {
		c.JSON(http.StatusPartialContent, results)
	} else {
		c.JSON(http.StatusOK, results)
	}
}

// ValidateJetStreamName validates if a JetStream name is available
// @Summary Validate JetStream name
// @Description Check if a JetStream name is available for a specific user
// @Tags JetStream
// @Accept json
// @Produce json
// @Param nats_operate_user_id query string true "NATS Operate User ID"
// @Param name query string true "JetStream name to validate"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /jetstreams/validate-name [get]
func (h *JetStreamHandler) ValidateJetStreamName(c *gin.Context) {
	natsOperateUserID := c.Query("nats_operate_user_id")
	name := c.Query("name")

	if natsOperateUserID == "" || name == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Both nats_operate_user_id and name are required",
		})
		return
	}

	req := &models.JetStreamListRequest{
		NatsOperateUserID: natsOperateUserID,
		Search:            name,
		Page:              1,
		PageSize:          1,
	}

	jetStreams, _, err := h.jetStreamService.ListJetStreams(req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream名称验证失败: %v", err)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error: "Failed to validate name: " + err.Error(),
		})
		return
	}

	available := true
	for _, js := range jetStreams {
		if js.Name == name {
			available = false
			break
		}
	}

	c.JSON(http.StatusOK, map[string]interface{}{
		"available":            available,
		"name":                 name,
		"nats_operate_user_id": natsOperateUserID,
	})
}
