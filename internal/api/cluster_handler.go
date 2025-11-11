package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"nats-control-api/internal/config"
	"nats-control-api/internal/service"
	"nats-control-api/pkg/models"

	"github.com/gin-gonic/gin"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type ClusterHandler struct {
	clusterService   *service.ClusterService
	monitorService   *service.ClusterMonitorService
	jetStreamManager *service.JetStreamManageService
	config           *config.Config
}

func NewClusterHandler(clusterService *service.ClusterService, monitorService *service.ClusterMonitorService, jms *service.JetStreamManageService, cfg *config.Config) *ClusterHandler {
	return &ClusterHandler{
		clusterService: clusterService,
		monitorService: monitorService,
		config:         cfg,
	}
}

// CreateCluster godoc
//
//	@Summary		Create a new cluster
//	@Description	Create a new NATS cluster with the provided configuration
//	@Tags			clusters
//	@Accept			json
//	@Produce		json
//	@Param			cluster	body		models.CreateClusterRequest	true	"Cluster data"
//	@Success		201		{object}	models.Cluster				"Created cluster"
//	@Failure		400		{object}	map[string]string			"Bad request"
//	@Failure		500		{object}	map[string]string			"Internal server error"
//	@Router			/clusters [post]
func (h *ClusterHandler) CreateCluster(c *gin.Context) {
	var req models.CreateClusterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群数据无效: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cluster, err := h.clusterService.CreateCluster(&req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群创建失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cluster)
}

// ListClusters godoc
//
//	@Summary		List all clusters
//	@Description	Get a list of all NATS clusters
//	@Tags			clusters
//	@Produce		json
//	@Param			status	query		string	false	"Filter by status"
//	@Success		200		{array}		models.Cluster
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/clusters [get]
func (h *ClusterHandler) ListClusters(c *gin.Context) {
	status := c.Query("status")

	clusters, err := h.clusterService.ListClusters(status)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群列表获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, clusters)
}

// GetCluster godoc
//
//	@Summary		Get cluster by ID
//	@Description	Get a specific cluster by its ID
//	@Tags			clusters
//	@Produce		json
//	@Param			id	path		string	true	"Cluster ID"
//	@Success		200	{object}	models.Cluster
//	@Failure		404	{object}	map[string]string	"Cluster not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id} [get]
func (h *ClusterHandler) GetCluster(c *gin.Context) {
	id := c.Param("id")

	cluster, err := h.clusterService.GetClusterByID(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群获取失败: %v", err)
		c.JSON(http.StatusNotFound, gin.H{"error": "Cluster not found"})
		return
	}

	c.JSON(http.StatusOK, cluster)
}

// UpdateCluster godoc
//
//	@Summary		Update cluster
//	@Description	Update a cluster's information
//	@Tags			clusters
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string						true	"Cluster ID"
//	@Param			cluster	body		models.UpdateClusterRequest	true	"Updated cluster data"
//	@Success		200		{object}	models.Cluster				"Updated cluster"
//	@Failure		400		{object}	map[string]string			"Bad request"
//	@Failure		404		{object}	map[string]string			"Cluster not found"
//	@Failure		500		{object}	map[string]string			"Internal server error"
//	@Router			/clusters/{id} [put]
func (h *ClusterHandler) UpdateCluster(c *gin.Context) {
	id := c.Param("id")

	var req models.UpdateClusterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群数据无效: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cluster, err := h.clusterService.UpdateCluster(id, &req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群更新失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cluster)
}

// EnableCluster godoc
//
//	@Summary		Enable cluster
//	@Description	Enable a cluster
//	@Tags			clusters
//	@Produce		json
//	@Param			id	path		string	true	"Cluster ID"
//	@Success		200	{object}	map[string]string	"Cluster enabled"
//	@Failure		404	{object}	map[string]string	"Cluster not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/enable [post]
func (h *ClusterHandler) EnableCluster(c *gin.Context) {
	id := c.Param("id")

	err := h.clusterService.SetClusterStatus(id, models.ClusterStatusActive)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群启用失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Cluster enabled successfully"})
}

// DisableCluster godoc
//
//	@Summary		Disable cluster
//	@Description	Disable a cluster
//	@Tags			clusters
//	@Produce		json
//	@Param			id	path		string	true	"Cluster ID"
//	@Success		200	{object}	map[string]string	"Cluster disabled"
//	@Failure		404	{object}	map[string]string	"Cluster not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/disable [post]
func (h *ClusterHandler) DisableCluster(c *gin.Context) {
	id := c.Param("id")

	err := h.clusterService.SetClusterStatus(id, models.ClusterStatusDisabled)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群禁用失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Cluster disabled successfully"})
}

// TestClusterConnection godoc
//
//	@Summary		Test cluster connection
//	@Description	Test connection to a NATS cluster
//	@Tags			clusters
//	@Produce		json
//	@Param			id	path		string	true	"Cluster ID"
//	@Success		200	{object}	map[string]interface{}	"Connection test result"
//	@Failure		404	{object}	map[string]string		"Cluster not found"
//	@Failure		500	{object}	map[string]string		"Internal server error"
//	@Router			/clusters/{id}/test [post]
func (h *ClusterHandler) TestClusterConnection(c *gin.Context) {
	id := c.Param("id")

	result, err := h.clusterService.TestClusterConnection(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群连接测试失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// Cluster monitoring APIs

// GetClusterHealthRecords godoc
//
//	@Summary		Get cluster health records
//	@Description	Get cluster health monitoring records with filters and pagination
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			cluster_id			query		string	false	"Filter by cluster ID"
//	@Param			status				query		string	false	"Filter by health status"
//	@Param			connection_status	query		string	false	"Filter by connection status"
//	@Param			start_date			query		string	false	"Start date (YYYY-MM-DD)"
//	@Param			end_date			query		string	false	"End date (YYYY-MM-DD)"
//	@Param			page				query		int		false	"Page number"
//	@Param			page_size			query		int		false	"Page size"
//	@Success		200					{object}	models.PaginatedResponse
//	@Failure		400					{object}	map[string]string	"Bad request"
//	@Failure		500					{object}	map[string]string	"Internal server error"
//	@Router			/clusters/health [get]
func (h *ClusterHandler) GetClusterHealthRecords(c *gin.Context) {
	var req models.ClusterHealthListRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		log.WithContext(c.Request.Context()).Errorf("查询参数无效: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Set defaults
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}

	records, total, err := h.monitorService.GetClusterHealthRecords(&req)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群健康记录获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	totalPages := (total + int64(req.PageSize) - 1) / int64(req.PageSize)

	response := models.PaginatedResponse{
		Data:       records,
		Total:      total,
		Page:       req.Page,
		PageSize:   req.PageSize,
		TotalPages: totalPages,
	}

	c.JSON(http.StatusOK, response)
}

// GetClusterMonitoringDashboard godoc
//
//	@Summary		Get cluster monitoring dashboard data
//	@Description	Get comprehensive cluster monitoring dashboard data including stats and recent failures
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Success		200	{object}	models.ClusterMonitoringDashboardResponse
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/monitoring/dashboard [get]
func (h *ClusterHandler) GetClusterMonitoringDashboard(c *gin.Context) {
	data, err := h.monitorService.GetMonitoringDashboardData()
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("监控面板数据获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, data)
}

// TriggerManualHealthCheck godoc
//
//	@Summary		Trigger manual health check
//	@Description	Trigger a manual health check for a specific cluster
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id	path		string	true	"Cluster ID"
//	@Success		200	{object}	models.ClusterHealthResponse
//	@Failure		404	{object}	map[string]string	"Cluster not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/health-check [post]
func (h *ClusterHandler) TriggerManualHealthCheck(c *gin.Context) {
	id := c.Param("id")

	result, err := h.monitorService.TriggerManualHealthCheck(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("手动健康检查触发失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// GetClusterTopology godoc
//
//	@Summary		Get cluster topology
//	@Description	Get super cluster topology showing gateway connections and groups
//	@Tags			cluster-topology
//	@Produce		json
//	@Success		200	{object}	models.ClusterTopologyResponse
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/topology [get]
func (h *ClusterHandler) GetClusterTopology(c *gin.Context) {
	topology, err := h.monitorService.GetClusterTopology()
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群拓扑获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, topology)
}

// GetClusterNodeCount godoc
//
//	@Summary		Get cluster node count
//	@Description	Get the number of nodes in a specific cluster
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id	path		string	true	"Cluster ID"
//	@Success		200	{object}	map[string]interface{}
//	@Failure		404	{object}	map[string]string	"Cluster not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/node-count [get]
func (h *ClusterHandler) GetClusterNodeCount(c *gin.Context) {
	id := c.Param("id")

	nodeCount, err := h.monitorService.GetClusterNodeCount(id)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群节点数获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"node_count": nodeCount})
}

// ListClusterServers godoc
//
//	@Summary		List cluster servers
//	@Description	Get information about all servers in a NATS cluster using system API
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id				path		string	true	"Cluster ID"
//	@Success		200				{object}	models.ServerListResponse
//	@Failure		400				{object}	map[string]string	"Bad request"
//	@Failure		404				{object}	map[string]string	"Cluster not found"
//	@Failure		500				{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/servers [get]
func (h *ClusterHandler) ListClusterServers(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster ID is required"})
		return
	}

	// 从配置获取超时时间，如果未配置则使用默认值5秒
	timeout := h.config.Monitor.ServerList.Timeout
	if timeout <= 0 {
		timeout = 5
	}
	timeoutDuration := time.Duration(timeout) * time.Second

	// 从服务获取期待节点数量
	expectedServers, err := h.monitorService.GetClusterNodeCount(clusterID)
	if err != nil {
		log.WithContext(c.Request.Context()).Warnf("获取集群节点数量失败，使用默认值0 - cluster: %s, error: %v", clusterID, err)
		expectedServers = 0
	}

	// 调用服务层方法
	response, err := h.monitorService.ListClusterServers(clusterID, timeoutDuration, expectedServers)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("获取集群服务器列表失败 - cluster: %s, error: %v", clusterID, err)

		// 根据错误类型返回适当的HTTP状态码
		if strings.Contains(err.Error(), "cluster not found") || strings.Contains(err.Error(), "failed to get cluster") {
			c.JSON(http.StatusNotFound, gin.H{"error": "cluster not found"})
			return
		}
		if strings.Contains(err.Error(), "no servers responded") || strings.Contains(err.Error(), "system privileges") {
			c.JSON(http.StatusForbidden, gin.H{"error": "insufficient system privileges or no servers available"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	log.WithContext(c.Request.Context()).Infof("成功获取集群服务器列表 - cluster: %s, servers: %d", clusterID, response.TotalServers)
	c.JSON(http.StatusOK, response)
}

// GetAccountJetStreamListDetection godoc
//
//	@Summary		Get cluster JetStream information
//	@Description	Get JetStream information for a specific cluster and account
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id		path		string	true	"Cluster ID"
//	@Param			acc		query		string	false	"Account public key"
//	@Success		200		{object}	map[string]interface{}
//	@Failure		404		{object}	map[string]string	"Cluster not found"
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/detection [get]
func (h *ClusterHandler) GetAccountJetStreamListDetection(c *gin.Context) {
	clusterID := c.Param("id")
	accountPublicKey := c.Query("acc")

	jetStreamInfo, err := h.monitorService.GetJetStreamDetectionInfo(clusterID, accountPublicKey)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream信息获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jetStreamInfo)
}

// GetAccountStreamNames godoc
//
//	@Summary		Get stream names for account
//	@Description	Get list of stream names for a specific account in a cluster
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id			path		string	true	"Cluster ID"
//	@Param			account_id	query		string	true	"Account ID"
//	@Success		200			{array}		string
//	@Failure		404			{object}	map[string]string	"Cluster not found"
//	@Failure		500			{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/actuality [get]
func (h *ClusterHandler) GetAccountStreamNames(c *gin.Context) {
	clusterID := c.Param("id")
	accountID := c.Query("account_id")

	streamNames, err := h.monitorService.GetStreamNamesByAccount(clusterID, accountID)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("获取流名称列表失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, streamNames)
}

// 辅助验证函数 - 已废弃，请使用 models.ValidateObjectID

// GetClusterJetStreamStreamDetail godoc
//
//	@Summary		Get detailed information for a specific stream
//	@Description	Get detailed information for a specific stream in a cluster and account
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id		path		string	true	"Cluster ID"
//	@Param			account_id		query		string	true	"Account ID"
//	@Param			stream	query		string	true	"Stream name"
//	@Success		200	{object}	map[string]interface{}
//	@Failure		400	{object}	map[string]string	"Bad request"
//	@Failure		404	{object}	map[string]string	"Cluster not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/info [get]
func (h *ClusterHandler) GetClusterJetStreamStreamDetail(c *gin.Context) {
	clusterID := c.Param("id")
	accountID := c.Query("account_id")
	streamName := c.Query("stream")

	// 增强参数验证
	if clusterID == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的集群ID参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "集群ID不能为空"})
		return
	}

	if accountID == "" {
		log.WithContext(c.Request.Context()).Errorf("缺少必要的账户ID参数: cluster_id=%s", clusterID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "账户ID不能为空"})
		return
	}

	if streamName == "" {
		log.WithContext(c.Request.Context()).Errorf("缺少必要的流名称参数: cluster_id=%s, account_key=%s", clusterID, accountID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "流名称不能为空"})
		return
	}

	// 验证集群ID格式
	if !models.ValidateObjectID(clusterID, models.ClusterObjType) {
		log.WithContext(c.Request.Context()).Errorf("集群ID格式不正确: cluster_id=%s", clusterID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "集群ID格式不正确，必须是 'cl_' + 15位十六进制字符"})
		return
	}

	// 验证流名称格式
	if !isValidStreamName(streamName) {
		log.WithContext(c.Request.Context()).Errorf("流名称格式错误: cluster_id=%s, account_key=%s, stream_name=%s", clusterID, accountID, streamName)
		c.JSON(http.StatusBadRequest, gin.H{"error": "流名称格式错误"})
		return
	}

	// 验证集群存在性
	cluster, err := h.clusterService.GetClusterByID(clusterID)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群查询失败: cluster_id=%s, error=%v", clusterID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "集群查询失败: " + err.Error()})
		return
	}
	if cluster == nil {
		log.WithContext(c.Request.Context()).Errorf("集群不存在: cluster_id=%s", clusterID)
		c.JSON(http.StatusNotFound, gin.H{"error": "指定的集群不存在"})
		return
	}

	// 验证集群状态
	if cluster.Status != "active" {
		log.WithContext(c.Request.Context()).Warnf("集群状态非活跃: cluster_id=%s, cluster_status=%s", clusterID, cluster.Status)
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("集群状态为'%s'，仅活跃集群支持此操作", cluster.Status)})
		return
	}

	log.WithContext(c.Request.Context()).Infof("处理流详情请求: cluster_id=%s, account_key=%s, stream_name=%s", clusterID, accountID, streamName)

	streamDetail, err := h.clusterService.GetJetStreamInfoByAccount(clusterID, accountID, streamName)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("流详情信息获取失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, streamDetail)
}

// DeleteClusterJetStreamStream godoc
//
//	@Summary		Delete a JetStream stream from cluster
//	@Description	Delete a JetStream stream directly from NATS cluster (not from database)
//	@Tags			clusters
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string	true	"Cluster ID"
//	@Param			account_id		query		string	true	"Account ID"
//	@Param			stream	query		string	true	"Stream name to delete"
//	@Success		204		{string}	string	"Stream deleted successfully"
//	@Failure		400		{object}	map[string]string	"Bad request"
//	@Failure		404		{object}	map[string]string	"Stream not found"
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/stream [delete]
func (h *ClusterHandler) DeleteClusterJetStreamStream(c *gin.Context) {
	clusterID := c.Param("id")
	accountID := c.Query("account_id")
	streamName := c.Query("stream")

	// 增强参数验证
	if clusterID == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的集群ID参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "集群ID不能为空"})
		return
	}

	if accountID == "" {
		log.WithContext(c.Request.Context()).Errorf("缺少必要的账户ID参数: cluster_id=%s", clusterID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "账户ID不能为空"})
		return
	}

	if streamName == "" {
		log.WithContext(c.Request.Context()).Errorf("缺少必要的流名称参数: cluster_id=%s, account_id=%s", clusterID, accountID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "流名称不能为空"})
		return
	}

	// 验证集群ID格式
	if !models.ValidateObjectID(clusterID, models.ClusterObjType) {
		log.WithContext(c.Request.Context()).Errorf("集群ID格式不正确: cluster_id=%s", clusterID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "集群ID格式不正确，必须是 'cl_' + 15位十六进制字符"})
		return
	}

	// 验证账户ID格式
	if !models.ValidateObjectID(accountID, models.AccountObjType) {
		log.WithContext(c.Request.Context()).Errorf("账户ID格式不正确: account_id=%s", accountID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "账户ID格式不正确，必须是 'ac_' + 15位十六进制字符"})
		return
	}

	// 验证流名称格式
	if !isValidStreamName(streamName) {
		log.WithContext(c.Request.Context()).Errorf("流名称格式错误: cluster_id=%s, account_id=%s, stream_name=%s", clusterID, accountID, streamName)
		c.JSON(http.StatusBadRequest, gin.H{"error": "流名称格式错误"})
		return
	}

	// 验证集群存在性
	cluster, err := h.clusterService.GetClusterByID(clusterID)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群不存在: cluster_id=%s, error=%v", clusterID, err)
		c.JSON(http.StatusNotFound, gin.H{"error": "集群不存在"})
		return
	}

	if cluster.Status != "active" {
		log.WithContext(c.Request.Context()).Errorf("集群状态异常，无法删除流: cluster_id=%s, status=%s", clusterID, cluster.Status)
		c.JSON(http.StatusBadRequest, gin.H{"error": "集群状态异常，无法删除流"})
		return
	}

	// 删除JetStream流
	err = h.monitorService.DeleteJetStreamFromCluster(clusterID, accountID, streamName)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("JetStream流删除失败: cluster_id=%s, account_id=%s, stream_name=%s, error=%v", clusterID, accountID, streamName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("删除流失败: %v", err)})
		return
	}

	log.WithContext(c.Request.Context()).Infof("JetStream流删除成功: cluster_id=%s, account_id=%s, stream_name=%s", clusterID, accountID, streamName)
	c.Status(http.StatusNoContent)
}

// isValidNATSAccountPublicKey 验证NATS账户公钥格式
func isValidNATSAccountPublicKey(publicKey string) bool {
	// NATS账户公钥以A开头，长度56个字符，由大写字母和数字组成
	if len(publicKey) != 56 {
		return false
	}
	if !strings.HasPrefix(publicKey, "A") {
		return false
	}
	// 验证只包含大写字母和数字
	for _, char := range publicKey {
		if !((char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9')) {
			return false
		}
	}
	return true
}

// isValidStreamName 验证流名称格式
func isValidStreamName(streamName string) bool {
	// JetStream流名称规则：
	// - 长度1-255个字符
	// - 只能包含字母、数字、点、下划线和横线
	// - 不能以点开头或结尾
	if len(streamName) < 1 || len(streamName) > 255 {
		return false
	}
	if strings.HasPrefix(streamName, ".") || strings.HasSuffix(streamName, ".") {
		return false
	}
	for _, char := range streamName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

// GetStreamConsumerList godoc
//
//	@Summary		Get consumer list for a specific stream
//	@Description	Get all consumers under a specific stream in a JetStream
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			name		path		string	true	"JetStream Name"
//	@Param			id		path		string	true	"Cluster ID"
//	@Success		200	{object}	map[string]interface{}
//	@Failure		400	{object}	map[string]string	"Bad request"
//	@Failure		404	{object}	map[string]string	"JetStream not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/{name}/consumers [get]
func (h *ClusterHandler) GetStreamConsumerList(c *gin.Context) {
	jetStreamName := c.Param("name")
	clusterID := c.Param("id")

	if jetStreamName == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的JetStream Name参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "JetStream Name不能为空"})
		return
	}

	log.WithContext(c.Request.Context()).Infof("处理Consumer列表请求: jetstream_name=%s", jetStreamName)

	consumers, err := h.monitorService.GetStreamConsumerList(clusterID, jetStreamName)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer列表获取失败: jetstream_name=%s, error=%v", jetStreamName, err)

		if strings.Contains(err.Error(), "not found") {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"jetstream_name": jetStreamName,
		"consumers":      consumers,
		"total":          len(consumers),
	})
}

// GetStreamConsumerInfo godoc
//
//	@Summary		Get consumer info for a specific consumer
//	@Description	Get detailed information for a specific consumer under a stream in a JetStream
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			name		path		string	true	"JetStream Name"
//	@Param			id		path		string	true	"Cluster ID"
//	@Param			consumer		path		string	true	"Consumer Name"
//	@Success		200	{object}	map[string]interface{}
//	@Failure		400	{object}	map[string]string	"Bad request"
//	@Failure		404	{object}	map[string]string	"Consumer not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/{name}/consumers/{consumer} [get]
func (h *ClusterHandler) GetStreamConsumerInfo(c *gin.Context) {
	jetStreamName := c.Param("name")
	clusterID := c.Param("id")
	consumerName := c.Param("consumer")

	if jetStreamName == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的JetStream Name参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "JetStream Name不能为空"})
		return
	}

	if consumerName == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的Consumer Name参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Consumer Name不能为空"})
		return
	}

	log.WithContext(c.Request.Context()).Infof("处理Consumer详情请求: jetstream_name=%s, consumer_name=%s", jetStreamName, consumerName)

	consumerInfo, err := h.monitorService.GetStreamConsumerInfo(clusterID, jetStreamName, consumerName)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer详情获取失败: jetstream_name=%s, consumer_name=%s, error=%v", jetStreamName, consumerName, err)

		if strings.Contains(err.Error(), "not found") {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"jetstream_name": jetStreamName,
		"consumer_name":  consumerName,
		"consumer_info":  consumerInfo,
	})
}

// DeleteClusterJetStreamConsumer godoc
//
//	@Summary		Delete a consumer from JetStream cluster
//	@Description	Delete a consumer from the specified JetStream stream on the cluster
//	@Tags			cluster-monitoring
//	@Produce		json
//	@Param			id		path		string	true	"Cluster ID"
//	@Param			account_id		query		string	true	"Account ID"
//	@Param			stream_name		query		string	true	"Stream Name"
//	@Param			consumer_name		query		string	true	"Consumer Name"
//	@Success		204	"Consumer deleted successfully"
//	@Failure		400	{object}	map[string]string	"Bad request"
//	@Failure		404	{object}	map[string]string	"Consumer not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/clusters/{id}/jetstream/consumer [delete]
func (h *ClusterHandler) DeleteClusterJetStreamConsumer(c *gin.Context) {
	clusterID := c.Param("id")
	accountID := c.Query("account_id")
	streamName := c.Query("stream_name")
	consumerName := c.Query("consumer_name")

	if clusterID == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的Cluster ID参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cluster ID不能为空"})
		return
	}

	if accountID == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的Account ID参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Account ID不能为空"})
		return
	}

	if streamName == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的Stream Name参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Stream Name不能为空"})
		return
	}

	if consumerName == "" {
		log.WithContext(c.Request.Context()).Error("缺少必要的Consumer Name参数")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Consumer Name不能为空"})
		return
	}

	if !models.ValidateObjectID(accountID, models.AccountObjType) {
		log.WithContext(c.Request.Context()).Errorf("账户ID格式不正确: account_id=%s", accountID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "账户ID格式不正确，必须是 'ac_' + 15位十六进制字符"})
		return
	}

	if !isValidStreamName(streamName) {
		log.WithContext(c.Request.Context()).Errorf("流名称格式错误: cluster_id=%s, account_id=%s, stream_name=%s", clusterID, accountID, streamName)
		c.JSON(http.StatusBadRequest, gin.H{"error": "流名称格式错误"})
		return
	}

	cluster, err := h.clusterService.GetClusterByID(clusterID)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("集群不存在: cluster_id=%s, error=%v", clusterID, err)
		c.JSON(http.StatusNotFound, gin.H{"error": "集群不存在"})
		return
	}

	if cluster.Status != "active" {
		log.WithContext(c.Request.Context()).Errorf("集群状态异常，无法删除消费者: cluster_id=%s, status=%s", clusterID, cluster.Status)
		c.JSON(http.StatusBadRequest, gin.H{"error": "集群状态异常，无法删除消费者"})
		return
	}

	err = h.monitorService.DeleteConsumerFromCluster(clusterID, accountID, streamName, consumerName)
	if err != nil {
		log.WithContext(c.Request.Context()).Errorf("Consumer删除失败: cluster_id=%s, account_id=%s, stream_name=%s, consumer_name=%s, error=%v", clusterID, accountID, streamName, consumerName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("删除消费者失败: %v", err)})
		return
	}

	log.WithContext(c.Request.Context()).Infof("Consumer删除成功: cluster_id=%s, account_id=%s, stream_name=%s, consumer_name=%s", clusterID, accountID, streamName, consumerName)
	c.Status(http.StatusNoContent)
}
