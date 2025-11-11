package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HealthHandler struct {
	repo *interface{} // DB repository for health checks
}

func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

// HealthCheck godoc
//
//	@Summary		Health check
//	@Description	Check the health status of the API and its dependencies
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	map[string]string	"Service is healthy"
//	@Failure		503	{object}	map[string]string	"Service is unhealthy"
//	@Router			/health [get]
func (h *HealthHandler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "nats-control-api",
	})
}

func SetupRoutes(
	router *gin.Engine,
	accountHandler *AccountHandler,
	userHandler *UserHandler,
	healthHandler *HealthHandler,
	jwtTaskHandler *JWTTaskHandler,
	clusterHandler *ClusterHandler,
	jetStreamHandler *JetStreamHandler,
	consumerHandler *ConsumerHandler,
) {
	api := router.Group("/api/v1")

	api.GET("/health", healthHandler.HealthCheck)

	// Cluster routes
	api.POST("/clusters", clusterHandler.CreateCluster)
	api.GET("/clusters", clusterHandler.ListClusters)
	api.GET("/clusters/:id", clusterHandler.GetCluster)
	api.PUT("/clusters/:id", clusterHandler.UpdateCluster)
	api.POST("/clusters/:id/enable", clusterHandler.EnableCluster)
	api.POST("/clusters/:id/disable", clusterHandler.DisableCluster)
	api.POST("/clusters/:id/test", clusterHandler.TestClusterConnection)

	// Cluster monitoring routes
	api.GET("/clusters/health", clusterHandler.GetClusterHealthRecords)
	api.GET("/clusters/monitoring/dashboard", clusterHandler.GetClusterMonitoringDashboard)
	api.POST("/clusters/:id/health-check", clusterHandler.TriggerManualHealthCheck)

	// Cluster topology routes
	api.GET("/clusters/topology", clusterHandler.GetClusterTopology)

	// Cluster node count route
	api.GET("/clusters/:id/node-count", clusterHandler.GetClusterNodeCount)

	// Cluster servers list route
	api.GET("/clusters/:id/servers", clusterHandler.ListClusterServers)

	// Cluster JetStream monitoring routes
	api.GET("/clusters/:id/jetstream/detection", clusterHandler.GetAccountJetStreamListDetection)
	api.GET("/clusters/:id/jetstream/actuality", clusterHandler.GetAccountStreamNames)
	api.GET("/clusters/:id/jetstream/info", clusterHandler.GetClusterJetStreamStreamDetail)
	api.DELETE("/clusters/:id/jetstream/stream", clusterHandler.DeleteClusterJetStreamStream)
	// Cluster JetStream Consumer monitoring routes
	api.GET("/clusters/:id/jetstream/:name/consumers", clusterHandler.GetStreamConsumerList)
	api.GET("/clusters/:id/jetstream/:name/consumers/:consumer", clusterHandler.GetStreamConsumerInfo)
	api.DELETE("/clusters/:id/jetstream/consumer", clusterHandler.DeleteClusterJetStreamConsumer)

	// User-specific JetStream routes
	api.GET("/users/:id/jetstreams", jetStreamHandler.GetJetStreamsByUser)

	// Account routes
	api.POST("/accounts", accountHandler.CreateAccount)
	api.GET("/accounts", accountHandler.ListAccounts)
	api.GET("/accounts/:id", accountHandler.GetAccount)
	api.PUT("/accounts/:id", accountHandler.UpdateAccount)
	api.DELETE("/accounts/:id", accountHandler.DeleteAccount)
	api.POST("/accounts/:id/disable", accountHandler.DisableAccount)
	api.POST("/accounts/:id/enable", accountHandler.EnableAccount)
	api.GET("/accounts/:id/jwt", accountHandler.GetAccountJWT)
	api.POST("/accounts/:id/sync", accountHandler.ManualSyncAccountJWT) // 手动同步JWT
	api.GET("/accounts/:id/user-count", accountHandler.GetAccountUserCount)
	api.GET("/accounts/public-key/:publicKey/jwt-task-count", accountHandler.GetAccountJWTTaskCount)

	// Account Export/Import routes
	api.POST("/accounts/:id/exports", accountHandler.AddAccountExport)
	api.DELETE("/accounts/:id/exports/:name", accountHandler.RemoveAccountExport)
	api.POST("/accounts/:id/imports", accountHandler.AddAccountImport)
	api.DELETE("/accounts/:id/imports/:name", accountHandler.RemoveAccountImport)

	// Simplified account association route
	api.POST("/accounts/:id/associate", accountHandler.CreateAccountAssociation)

	// Account users routes - use the same parameter name to avoid conflicts
	api.POST("/accounts/:id/users", userHandler.CreateUser)
	api.GET("/accounts/:id/users", userHandler.ListUsers)

	// User routes
	api.GET("/users/admin/non-system", userHandler.ListAdminUsersFromNonSystemAccounts)
	api.GET("/users", userHandler.GetAllUsers)
	api.GET("/users/:id", userHandler.GetUser)
	api.PUT("/users/:id", userHandler.UpdateUser)
	api.DELETE("/users/:id", userHandler.DeleteUser)
	api.POST("/users/:id/disable", userHandler.DisableUser)
	api.POST("/users/:id/enable", userHandler.EnableUser)
	api.GET("/users/:id/jwt", userHandler.GetUserJWT)
	api.GET("/users/:id/creds", userHandler.GetUserCreds)
	api.GET("/users/:id/creds/download", userHandler.DownloadUserCreds)
	api.GET("/users/:id/copy-context", userHandler.CopyUserContext)

	// JWT Task management routes
	api.GET("/jwt-tasks", jwtTaskHandler.ListJWTTasks)
	api.GET("/jwt-tasks/stats", jwtTaskHandler.GetJWTTaskStats)
	api.GET("/jwt-tasks/failed", jwtTaskHandler.ListFailedJWTTasks)
	api.GET("/jwt-tasks/:id", jwtTaskHandler.GetJWTTask)
	api.POST("/jwt-tasks/:id/retry", jwtTaskHandler.RetryJWTTask)

	// JetStream management routes
	api.POST("/jetstreams", jetStreamHandler.CreateJetStream)
	api.PUT("/jetstreams/:id", jetStreamHandler.UpdateJetStream)
	api.GET("/jetstreams", jetStreamHandler.ListJetStreams)
	api.GET("/jetstreams/stats", jetStreamHandler.GetJetStreamStats)
	api.GET("/jetstreams/validate-name", jetStreamHandler.ValidateJetStreamName)
	api.POST("/jetstreams/batch-delete", jetStreamHandler.BatchDeleteJetStreams)
	api.GET("/jetstreams/:id", jetStreamHandler.GetJetStream)
	api.DELETE("/jetstreams/:id", jetStreamHandler.DeleteJetStream)
	api.GET("/jetstreams/:id/diff", jetStreamHandler.CompareJetStreamConfig)
	api.POST("/jetstreams/:id/retry", jetStreamHandler.RetryJetStreamCreation)

	// Consumer management routes
	api.POST("/consumers", consumerHandler.CreateConsumer)
	api.GET("/consumers", consumerHandler.ListConsumers)
	api.GET("/consumers/stats", consumerHandler.GetConsumerStats)
	api.GET("/consumers/:id", consumerHandler.GetConsumer)
	api.PUT("/consumers/:id", consumerHandler.UpdateConsumer)
	api.DELETE("/consumers/:id", consumerHandler.DeleteConsumer)
	api.GET("/consumers/:id/diff", consumerHandler.CompareConsumerConfig)
	api.POST("/consumers/:id/retry", consumerHandler.RetryConsumerSync)
	api.POST("/consumers/:id/pause", consumerHandler.PauseConsumer)
	api.POST("/consumers/:id/resume", consumerHandler.ResumeConsumer)
}
