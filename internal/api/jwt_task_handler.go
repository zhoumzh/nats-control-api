package api

import (
	"net/http"
	"strconv"

	"nats-control-api/internal/service"
	
	"github.com/gin-gonic/gin"
)

type JWTTaskHandler struct {
	jwtTaskService *service.JWTTaskService
}

func NewJWTTaskHandler(jwtTaskService *service.JWTTaskService) *JWTTaskHandler {
	return &JWTTaskHandler{
		jwtTaskService: jwtTaskService,
	}
}

// GetJWTTask godoc
//	@Summary		Get JWT task by ID
//	@Description	Retrieve a JWT task by its ID
//	@Tags			jwt-tasks
//	@Produce		json
//	@Param			id	path		string				true	"Task ID"
//	@Success		200	{object}	models.JWTTask		"Task details"
//	@Failure		404	{object}	map[string]string	"Task not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/jwt-tasks/{id} [get]
func (h *JWTTaskHandler) GetJWTTask(c *gin.Context) {
	id := c.Param("id")
	
	task, err := h.jwtTaskService.GetJWTTask(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}
	
	c.JSON(http.StatusOK, task)
}

// ListJWTTasks godoc
//	@Summary		List JWT tasks
//	@Description	Get a list of JWT tasks with optional status filter and pagination
//	@Tags			jwt-tasks
//	@Produce		json
//	@Param			status	query		string				false	"Task status filter (pending, processing, completed, failed, retrying)"
//	@Param			limit	query		int					false	"Number of tasks to return (default 20, max 100)"
//	@Param			offset	query		int					false	"Number of tasks to skip (default 0)"
//	@Success		200		{array}		models.JWTTask		"List of tasks"
//	@Failure		400		{object}	map[string]string	"Bad request"
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/jwt-tasks [get]
func (h *JWTTaskHandler) ListJWTTasks(c *gin.Context) {
	status := c.Query("status")
	limit := 20
	offset := 0
	
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	
	if o := c.Query("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	
	tasks, err := h.jwtTaskService.ListJWTTasks(status, limit, offset)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, tasks)
}

// ListFailedJWTTasks godoc
//	@Summary		List failed JWT tasks
//	@Description	Get a list of failed JWT tasks
//	@Tags			jwt-tasks
//	@Produce		json
//	@Param			limit	query		int					false	"Number of tasks to return (default 20, max 100)"
//	@Success		200		{array}		models.JWTTask		"List of failed tasks"
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/jwt-tasks/failed [get]
func (h *JWTTaskHandler) ListFailedJWTTasks(c *gin.Context) {
	limit := 20
	
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	
	tasks, err := h.jwtTaskService.ListFailedJWTTasks(limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, tasks)
}

// RetryJWTTask godoc
//	@Summary		Retry failed JWT task
//	@Description	Retry a failed JWT task by resetting it to pending status
//	@Tags			jwt-tasks
//	@Produce		json
//	@Param			id	path		string				true	"Task ID"
//	@Success		200	{object}	map[string]string	"Task retry initiated successfully"
//	@Failure		400	{object}	map[string]string	"Bad request"
//	@Failure		404	{object}	map[string]string	"Task not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/jwt-tasks/{id}/retry [post]
func (h *JWTTaskHandler) RetryJWTTask(c *gin.Context) {
	id := c.Param("id")
	
	err := h.jwtTaskService.RetryJWTTask(id)
	if err != nil {
		if err.Error() == "task not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Task has been reset to pending status and will be retried automatically",
		"task_id": id,
	})
}

// GetJWTTaskStats godoc
//	@Summary		Get JWT task statistics
//	@Description	Get statistics of JWT tasks by status
//	@Tags			jwt-tasks
//	@Produce		json
//	@Success		200	{object}	service.JWTTaskStats	"Task statistics"
//	@Failure		500	{object}	map[string]string		"Internal server error"
//	@Router			/jwt-tasks/stats [get]
func (h *JWTTaskHandler) GetJWTTaskStats(c *gin.Context) {
	stats, err := h.jwtTaskService.GetJWTTaskStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, stats)
}