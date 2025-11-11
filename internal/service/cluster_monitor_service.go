package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"nats-control-api/internal/config"
	"nats-control-api/internal/db"
	mynats "nats-control-api/internal/nats"
	"nats-control-api/pkg/models"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
)

type ClusterMonitorService struct {
	repo       *db.Repository
	clusterSvc *ClusterService
	natsServer *mynats.Service
	config     *config.Config
	stopChan   chan struct{}
	isRunning  bool
}

func NewClusterMonitorService(repo *db.Repository, clusterSvc *ClusterService, config *config.Config) *ClusterMonitorService {
	return &ClusterMonitorService{
		repo:       repo,
		clusterSvc: clusterSvc,
		natsServer: mynats.NewService(),
		config:     config,
		stopChan:   make(chan struct{}),
	}
}

// StartMonitoring starts the background monitoring process
func (s *ClusterMonitorService) StartMonitoring() error {
	if s.isRunning {
		return fmt.Errorf("monitoring is already running")
	}

	s.isRunning = true

	go s.monitorLoop()

	log.WithContext(context.Background()).Info("Cluster monitoring started")
	return nil
}

// StopMonitoring stops the background monitoring process
func (s *ClusterMonitorService) StopMonitoring() {
	if !s.isRunning {
		return
	}

	close(s.stopChan)
	s.isRunning = false

	log.WithContext(context.Background()).Info("Cluster monitoring stopped")
}

// monitorLoop is the main monitoring loop
func (s *ClusterMonitorService) monitorLoop() {
	for {
		select {
		case <-s.stopChan:
			return
		default:
			s.performHealthChecks()

			// Get refresh interval from config, with fallback to database config
			var interval time.Duration
			if s.config.Monitor.HealthCheck.Interval > 0 {
				// Use global config interval
				interval = time.Duration(s.config.Monitor.HealthCheck.Interval) * time.Second
			} else {
				// Use default interval
				log.WithContext(context.Background()).Debug("Using default monitoring interval")
				interval = 30 * time.Second
			}

			time.Sleep(interval)
		}
	}
}

// performHealthChecks performs health checks on all active clusters
func (s *ClusterMonitorService) performHealthChecks() {
	// Get all active clusters for monitoring
	clusters, err := s.repo.ClusterRepo.GetActiveClusters()
	if err != nil {
		log.WithContext(context.Background()).Errorf("Failed to get active clusters: %v", err)
		return
	}

	log.WithContext(context.Background()).Debugf("Performing health checks on %d clusters", len(clusters))

	for _, cluster := range clusters {
		s.checkClusterHealth(cluster)
	}
}

// checkClusterHealth performs health check on a single cluster
func (s *ClusterMonitorService) checkClusterHealth(cluster *models.Cluster) {
	startTime := time.Now()

	// Perform the connection test
	result, err := s.clusterSvc.TestClusterConnection(cluster.ID)

	responseTime := time.Since(startTime).Milliseconds()

	// Create health record
	health := &models.ClusterHealth{
		ID:           uuid.New().String(),
		ClusterID:    cluster.ID,
		NATSURL:      cluster.GetNATSURL(),
		MonitorURL:   cluster.GetMonitorURL(),
		TestType:     "auto",
		TestedAt:     models.CustomTime{Time: time.Now()},
		ResponseTime: responseTime,
	}

	if err != nil {
		health.Status = models.ClusterHealthStatusUnhealthy
		health.ConnectionStatus = "failed"
		health.ErrorMessage = err.Error()

		log.WithContext(context.Background()).Warnf("Cluster health check failed for cluster %s (%s) - Error: %s, Response time: %dms", cluster.ID, cluster.Name, err.Error(), responseTime)
	} else {
		// Check if result indicates success
		if status, exists := result["status"]; exists && status == "success" {
			health.Status = models.ClusterHealthStatusHealthy
			health.ConnectionStatus = "success"
		} else {
			health.Status = models.ClusterHealthStatusUnhealthy
			health.ConnectionStatus = "failed"
			if errMsg, exists := result["error"]; exists {
				health.ErrorMessage = fmt.Sprintf("%v", errMsg)
			}
		}

		log.WithContext(context.Background()).Debugf("Cluster health check completed for cluster %s (%s) - Status: %s, Response time: %dms", cluster.ID, cluster.Name, health.Status, responseTime)
	}

	// Only save health record if cluster is not healthy (abnormal status)
	if health.Status != models.ClusterHealthStatusHealthy {
		if err := s.repo.ClusterRepo.CreateClusterHealth(health); err != nil {
			log.WithContext(context.Background()).Errorf("Failed to save cluster health record for cluster %s: %v", cluster.ID, err)
		}

		log.WithContext(context.Background()).Infof("Recorded abnormal cluster health status for cluster %s (%s) - Status: %s, Response time: %dms", cluster.ID, cluster.Name, health.Status, responseTime)
	} else {
		log.WithContext(context.Background()).Debugf("Cluster is healthy, skipping record creation for cluster %s (%s) - Status: %s, Response time: %dms", cluster.ID, cluster.Name, health.Status, responseTime)
	}
}

// GetClusterHealthRecords retrieves cluster health records with pagination
func (s *ClusterMonitorService) GetClusterHealthRecords(req *models.ClusterHealthListRequest) ([]*models.ClusterHealthResponse, int64, error) {
	return s.repo.ClusterRepo.GetClusterHealth(req)
}

// GetMonitoringDashboardData retrieves comprehensive monitoring dashboard data
func (s *ClusterMonitorService) GetMonitoringDashboardData() (*models.ClusterMonitoringDashboardResponse, error) {
	stats, err := s.repo.ClusterRepo.GetClusterHealthStats()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster stats: %w", err)
	}

	unhealthyClusters, err := s.repo.ClusterRepo.GetUnhealthyClusters()
	if err != nil {
		return nil, fmt.Errorf("failed to get unhealthy clusters: %w", err)
	}

	// Get recent failures (last 10)
	req := &models.ClusterHealthListRequest{
		ConnectionStatus: "failed",
		Page:             1,
		PageSize:         10,
	}
	recentFailures, _, err := s.GetClusterHealthRecords(req)
	if err != nil {
		log.WithContext(context.Background()).Errorf("Failed to get recent failures: %v", err)
		recentFailures = []*models.ClusterHealthResponse{}
	}

	response := &models.ClusterMonitoringDashboardResponse{
		Stats:          stats,
		UnhealthyCount: len(unhealthyClusters),
		RecentFailures: recentFailures,
		LastUpdateTime: models.CustomTime{Time: time.Now()},
	}

	return response, nil
}

// TriggerManualHealthCheck triggers a manual health check for a specific cluster
func (s *ClusterMonitorService) TriggerManualHealthCheck(clusterID string) (*models.ClusterHealthResponse, error) {
	cluster, err := s.repo.GetClusterByID(clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster: %w", err)
	}

	startTime := time.Now()

	// Perform the connection test
	result, err := s.clusterSvc.TestClusterConnection(cluster.ID)

	responseTime := time.Since(startTime).Milliseconds()

	// Create health record
	health := &models.ClusterHealth{
		ID:           uuid.New().String(),
		ClusterID:    cluster.ID,
		NATSURL:      cluster.GetNATSURL(),
		MonitorURL:   cluster.GetMonitorURL(),
		TestType:     "manual",
		TestedAt:     models.CustomTime{Time: time.Now()},
		ResponseTime: responseTime,
	}

	if err != nil {
		health.Status = models.ClusterHealthStatusUnhealthy
		health.ConnectionStatus = "failed"
		health.ErrorMessage = err.Error()
	} else {
		// Check if result indicates success
		if status, exists := result["status"]; exists && status == "success" {
			health.Status = models.ClusterHealthStatusHealthy
			health.ConnectionStatus = "success"
		} else {
			health.Status = models.ClusterHealthStatusUnhealthy
			health.ConnectionStatus = "failed"
			if errMsg, exists := result["error"]; exists {
				health.ErrorMessage = fmt.Sprintf("%v", errMsg)
			}
		}
	}

	// Only save health record if cluster is not healthy (abnormal status)
	// Even for manual checks, we only record abnormal status to keep database clean
	if health.Status != models.ClusterHealthStatusHealthy {
		if err := s.repo.ClusterRepo.CreateClusterHealth(health); err != nil {
			return nil, fmt.Errorf("failed to save health record: %w", err)
		}

		log.WithContext(context.Background()).Infof("Recorded manual cluster health check result (abnormal) for cluster %s (%s) - Status: %s, Test type: manual", cluster.ID, cluster.Name, health.Status)
	} else {
		log.WithContext(context.Background()).Infof("Manual health check shows cluster is healthy, no record saved for cluster %s (%s) - Status: %s, Test type: manual", cluster.ID, cluster.Name, health.Status)
	}

	response := &models.ClusterHealthResponse{
		ClusterHealth: health,
		ClusterName:   cluster.Name,
		HostInfo:      fmt.Sprintf("%s:%d", cluster.Host, cluster.NATSPort),
	}

	return response, nil
}

// GetClusterNodeCount retrieves the node count for a specific cluster
func (s *ClusterMonitorService) GetClusterNodeCount(clusterID string) (int, error) {
	cluster, err := s.repo.GetClusterByID(clusterID)
	if err != nil {
		return 0, fmt.Errorf("failed to get cluster: %w", err)
	}

	if cluster == nil {
		return 0, fmt.Errorf("cluster not found")
	}

	nodeCount, err := s.fetchClusterNodeCountHTTP(cluster)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch cluster node count: %w", err)
	}

	return nodeCount, nil
}

// GetClusterTopology retrieves the complete cluster topology by fetching gateway information
func (s *ClusterMonitorService) GetClusterTopology() (*models.ClusterTopologyResponse, error) {
	// Get all active clusters
	clusters, err := s.repo.ClusterRepo.GetActiveClusters()
	if err != nil {
		return nil, fmt.Errorf("failed to get active clusters: %w", err)
	}

	// Fetch gateway information for each cluster
	clusterNodes := make(map[string]*models.ClusterTopologyNode)
	allConnections := make([]*models.ClusterConnection, 0)

	for _, cluster := range clusters {
		node := &models.ClusterTopologyNode{
			ClusterID:           cluster.ID,
			ClusterName:         cluster.Name,
			Host:                cluster.Host,
			GatewayPort:         cluster.GatewayPort,
			MonitorPort:         cluster.MonitorPort,
			IncomingConnections: make([]*models.ClusterConnection, 0),
			OutgoingConnections: make([]*models.ClusterConnection, 0),
			GatewayUrls:         make([]string, 0),
		}

		// Fetch gateway info from the cluster's monitor endpoint
		gatewayInfo, err := s.fetchGatewayInfo(cluster)
		if err != nil {
			log.WithContext(context.Background()).Warnf("Failed to fetch gateway info for cluster %s: %v", cluster.ID, err)
			node.ConnectionStatus = "unknown"
			clusterNodes[cluster.ID] = node
			continue
		}

		log.WithContext(context.Background()).Debugf("Fetched gateway info for cluster %s (%s) - Outbound: %d, Inbound: %d", cluster.ID, cluster.Name, len(gatewayInfo.OutboundGateways), len(gatewayInfo.InboundGateways))

		// Process gateway connections
		for gatewayName, gatewayConn := range gatewayInfo.OutboundGateways {
			if gatewayConn.Connection != nil {
				log.WithContext(context.Background()).Debugf("Processing outbound gateway connection for cluster %s - Gateway: %s, IP: %s, Port: %d", cluster.ID, gatewayName, gatewayConn.Connection.IP, gatewayConn.Connection.Port)

				// Find target cluster by gateway name (which should match cluster name)
				targetCluster := s.findClusterByName(clusters, gatewayName)
				if targetCluster != nil {
					connection := &models.ClusterConnection{
						FromClusterID:   cluster.ID,
						FromClusterName: cluster.Name,
						ToClusterID:     targetCluster.ID,
						ToClusterName:   targetCluster.Name,
						ConnectionType:  "gateway",
						Status:          "active",
					}
					node.OutgoingConnections = append(node.OutgoingConnections, connection)
					allConnections = append(allConnections, connection)
					node.GatewayUrls = append(node.GatewayUrls, fmt.Sprintf("nats://%s:%d", gatewayConn.Connection.IP, gatewayConn.Connection.Port))

					log.WithContext(context.Background()).Debugf("Added outbound connection from %s to %s", cluster.Name, targetCluster.Name)
				} else {
					// External gateway connection (not in our cluster list)
					log.WithContext(context.Background()).Debugf("Found external gateway connection (not in our cluster list) for cluster %s - Gateway: %s, IP: %s, Port: %d", cluster.ID, gatewayName, gatewayConn.Connection.IP, gatewayConn.Connection.Port)
					node.GatewayUrls = append(node.GatewayUrls, fmt.Sprintf("nats://%s:%d", gatewayConn.Connection.IP, gatewayConn.Connection.Port))
				}
			}
		}

		// Determine connection status - simplified logic
		incomingCount := 0
		for _, conn := range allConnections {
			if conn.ToClusterID == cluster.ID {
				incomingCount++
			}
		}

		// Only two states: connected (has any connections) or isolated (no connections)
		if len(node.OutgoingConnections) > 0 || incomingCount > 0 {
			node.ConnectionStatus = "connected"
		} else {
			node.ConnectionStatus = "isolated"
		}

		clusterNodes[cluster.ID] = node
	}

	// Update incoming connections for all nodes
	for _, connection := range allConnections {
		if targetNode, exists := clusterNodes[connection.ToClusterID]; exists {
			targetNode.IncomingConnections = append(targetNode.IncomingConnections, connection)
		}
	}

	// Group clusters into super cluster groups using DFS
	visited := make(map[string]bool)
	superClusterGroups := make([]*models.SuperClusterGroup, 0)
	isolatedClusters := make([]*models.ClusterTopologyNode, 0)
	groupCounter := 1

	for clusterID, node := range clusterNodes {
		if visited[clusterID] {
			continue
		}

		if node.ConnectionStatus == "isolated" {
			isolatedClusters = append(isolatedClusters, node)
			visited[clusterID] = true
			continue
		}

		// Find all connected clusters using DFS
		groupClusters := make([]*models.ClusterTopologyNode, 0)
		groupConnections := make([]*models.ClusterConnection, 0)
		stack := []string{clusterID}

		for len(stack) > 0 {
			currentID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if visited[currentID] {
				continue
			}

			visited[currentID] = true
			currentNode := clusterNodes[currentID]
			groupClusters = append(groupClusters, currentNode)

			// Add all connections from this cluster
			for _, conn := range currentNode.OutgoingConnections {
				groupConnections = append(groupConnections, conn)
				if !visited[conn.ToClusterID] {
					stack = append(stack, conn.ToClusterID)
				}
			}

			for _, conn := range currentNode.IncomingConnections {
				// Check if we already added this connection
				found := false
				for _, existingConn := range groupConnections {
					if existingConn.FromClusterID == conn.FromClusterID && existingConn.ToClusterID == conn.ToClusterID {
						found = true
						break
					}
				}
				if !found {
					groupConnections = append(groupConnections, conn)
				}
				if !visited[conn.FromClusterID] {
					stack = append(stack, conn.FromClusterID)
				}
			}
		}

		if len(groupClusters) > 1 {
			group := &models.SuperClusterGroup{
				GroupID:     fmt.Sprintf("group-%d", groupCounter),
				GroupName:   fmt.Sprintf("超级集群组 %d", groupCounter),
				Clusters:    groupClusters,
				Connections: groupConnections,
			}
			superClusterGroups = append(superClusterGroups, group)
			groupCounter++
		} else if len(groupClusters) == 1 {
			// Single cluster with partial connections
			isolatedClusters = append(isolatedClusters, groupClusters[0])
		}
	}

	// Calculate statistics
	connectedClusters := 0
	for _, group := range superClusterGroups {
		connectedClusters += len(group.Clusters)
	}

	response := &models.ClusterTopologyResponse{
		SuperClusterGroups: superClusterGroups,
		IsolatedClusters:   isolatedClusters,
		TotalClusters:      len(clusters),
		ConnectedClusters:  connectedClusters,
		IsolatedCount:      len(isolatedClusters),
		SuperClusterCount:  len(superClusterGroups),
	}

	return response, nil
}

// fetchGatewayInfo fetches gateway information from cluster's monitor endpoint
func (s *ClusterMonitorService) fetchGatewayInfo(cluster *models.Cluster) (*models.GatewayInfo, error) {
	url := fmt.Sprintf("http://%s:%d/gatewayz", cluster.Host, cluster.MonitorPort)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch gateway info from %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway endpoint returned status %d", resp.StatusCode)
	}

	var gatewayInfo models.GatewayInfo
	if err := json.NewDecoder(resp.Body).Decode(&gatewayInfo); err != nil {
		return nil, fmt.Errorf("failed to decode gateway response: %w", err)
	}

	return &gatewayInfo, nil
}

// connectWithUserAuth connects to NATS cluster using user's existing JWT token
func (s *ClusterMonitorService) connectWithUserAuth(cluster *models.Cluster, user *models.User, account *models.Account) (*nats.Conn, error) {
	log.WithContext(context.Background()).Infof("Connecting to NATS using existing user JWT - Cluster: %s, User: %s (%s), Account: %s (%s)", cluster.GetNATSURL(), user.ID, user.Name, account.ID, account.Name)

	// Check if user has JWT token
	if user.JWTText == nil || *user.JWTText == "" {
		return nil, fmt.Errorf("user %s does not have JWT token", user.Name)
	}

	// Use the existing JWT token from the user record
	userJWT := *user.JWTText

	log.WithContext(context.Background()).Infof("Using existing JWT token for authentication - User: %s, JWT length: %d", user.ID, len(userJWT))

	// Connect with JWT authentication
	nc, err := nats.Connect(cluster.GetNATSURL(),
		nats.Name(fmt.Sprintf("monitor-user-%s", user.ID)),
		nats.Timeout(10*time.Second),
		nats.UserJWT(func() (string, error) {
			log.WithContext(context.Background()).Debug("Providing existing JWT for authentication")
			return userJWT, nil
		}, func(nonce []byte) ([]byte, error) {
			log.WithContext(context.Background()).Debugf("Signing nonce with user NKey - Nonce length: %d", len(nonce))
			// Use user's NKey to sign the nonce
			kp, err := nkeys.FromSeed([]byte(user.NKey))
			if err != nil {
				log.WithContext(context.Background()).Errorf("Failed to create key pair from user NKey: %v", err)
				return nil, fmt.Errorf("failed to create key pair from user NKey: %w", err)
			}
			signature, err := kp.Sign(nonce)
			if err != nil {
				log.WithContext(context.Background()).Errorf("Failed to sign nonce: %v", err)
				return nil, fmt.Errorf("failed to sign nonce: %w", err)
			}
			log.WithContext(context.Background()).Debug("Nonce signed successfully")
			return signature, nil
		}),
	)
	if err != nil {
		log.WithContext(context.Background()).Errorf("Failed to connect to NATS cluster %s (%s) for user %s: %v", cluster.GetNATSURL(), cluster.Name, user.ID, err)

		// Provide more specific connection error messages
		var detailedErr error
		switch {
		case strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host"):
			detailedErr = fmt.Errorf("无法连接到NATS服务器 %s，请检查集群配置", cluster.GetNATSURL())
		case strings.Contains(err.Error(), "authentication violation") || strings.Contains(err.Error(), "authorization violation"):
			detailedErr = fmt.Errorf("用户 '%s' 认证失败，请检查用户权限配置", user.Name)
		case strings.Contains(err.Error(), "timeout"):
			detailedErr = fmt.Errorf("连接NATS服务器超时，请检查网络连接")
		case strings.Contains(err.Error(), "jwt") || strings.Contains(err.Error(), "JWT"):
			detailedErr = fmt.Errorf("用户JWT认证失败")
		case strings.Contains(err.Error(), "nkey") || strings.Contains(err.Error(), "key"):
			detailedErr = fmt.Errorf("用户密钥验证失败")
		case strings.Contains(err.Error(), "account"):
			detailedErr = fmt.Errorf("账户 '%s' 配置错误或未启用", account.Name)
		default:
			detailedErr = fmt.Errorf("连接NATS集群失败: %s", err.Error())
		}

		return nil, detailedErr
	}

	log.WithContext(context.Background()).Infof("Successfully connected to NATS cluster %s using existing JWT - User: %s, Connected: %t", cluster.Name, user.ID, nc.IsConnected())

	return nc, nil
}

// fetchJetStreamInfoHTTP fetches JetStream information from cluster's HTTP monitor endpoint
func (s *ClusterMonitorService) fetchJetStreamInfoHTTP(cluster *models.Cluster, accountPublicKey string) (map[string]interface{}, error) {
	url := fmt.Sprintf("http://%s:%d/jsz", cluster.Host, cluster.MonitorPort)

	if accountPublicKey != "" {
		url += fmt.Sprintf("?acc=%s&config=true&streams=true&consumers=true", accountPublicKey)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch jetstream info from %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jetstream endpoint returned status %d", resp.StatusCode)
	}

	var jetStreamInfo map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&jetStreamInfo); err != nil {
		return nil, fmt.Errorf("failed to decode jetstream response: %w", err)
	}

	return jetStreamInfo, nil
}

// fetchClusterNodeCountHTTP fetches cluster node count from cluster's HTTP monitor endpoint
func (s *ClusterMonitorService) fetchClusterNodeCountHTTP(cluster *models.Cluster) (int, error) {
	url := fmt.Sprintf("http://%s:%d/routez", cluster.Host, cluster.MonitorPort)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch routes info from %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("routes endpoint returned status %d", resp.StatusCode)
	}

	var routesInfo map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&routesInfo); err != nil {
		return 0, fmt.Errorf("failed to decode routes response: %w", err)
	}

	routes, ok := routesInfo["routes"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid routes format in response")
	}

	// Use map to count unique remote nodes
	uniqueNodes := make(map[string]bool)
	for _, route := range routes {
		routeMap, ok := route.(map[string]interface{})
		if !ok {
			continue
		}
		if remoteID, exists := routeMap["remote_id"].(string); exists {
			uniqueNodes[remoteID] = true
		}
	}

	// Add 1 for current node
	nodeCount := len(uniqueNodes) + 1
	return nodeCount, nil
}

// GetJetStreamDetectionInfo returns standardized JetStream information for a specific cluster and account
func (s *ClusterMonitorService) GetJetStreamDetectionInfo(clusterID, accountPublicKey string) (*models.JetStreamResponse, error) {
	cluster, err := s.repo.ClusterRepo.GetClusterByID(clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster: %w", err)
	}

	if cluster == nil {
		return nil, fmt.Errorf("cluster not found")
	}

	rawInfo, err := s.fetchJetStreamInfoHTTP(cluster, accountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch jetstream info: %w", err)
	}

	jsonData, err := json.Marshal(rawInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal jetstream info: %w", err)
	}

	var jsInfo models.JetStreamResponse
	if err := json.Unmarshal(jsonData, &jsInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal jetstream info: %w", err)
	}

	return &jsInfo, nil
}

// GetStreamNamesByAccount returns stream names for a specific account in a cluster
func (s *ClusterMonitorService) GetStreamNamesByAccount(clusterID, accountID string) ([]string, error) {
	nc, js, err := s.clusterSvc.GetNatsJetStreamWithAccount(clusterID, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect with account: %w", err)
	}
	defer nc.Close()

	streamNames, err := s.natsServer.ListStreams(js)
	if err != nil {
		if strings.Contains(err.Error(), "JetStream not enabled") || strings.Contains(err.Error(), "code 503") {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to list streams: %w", err)
	}
	return streamNames, nil
}

// findClusterByName finds a cluster by name
func (s *ClusterMonitorService) findClusterByName(clusters []*models.Cluster, name string) *models.Cluster {
	for _, cluster := range clusters {
		if strings.EqualFold(cluster.Name, name) {
			return cluster
		}
	}

	log.WithContext(context.Background()).Debugf("No cluster found matching gateway name %s - Total clusters: %d", name, len(clusters))

	return nil
}

// determineJetStreamEnabledStatus implements the complex status detection logic from frontend
func (s *ClusterMonitorService) determineJetStreamEnabledStatus(data map[string]interface{}) bool {
	// Check for explicit jetstream_enabled flag first
	if jetStreamEnabled, exists := data["jetstream_enabled"]; exists {
		if enabled, ok := jetStreamEnabled.(bool); ok {
			return enabled
		}
	}

	// If there's an error or empty response, JetStream is not enabled
	if data == nil {
		return false
	}

	if errorMsg, exists := data["error"]; exists {
		if errStr, ok := errorMsg.(string); ok && errStr != "" {
			return false
		}
	}

	// Check account_details for account-specific JetStream information
	if accountDetails, exists := data["account_details"]; exists {
		if detailsArray, ok := accountDetails.([]interface{}); ok && len(detailsArray) > 0 {
			if accountDetail, ok := detailsArray[0].(map[string]interface{}); ok {
				// Check for explicit jetstream_enabled flag in account details
				if jetStreamEnabled, exists := accountDetail["jetstream_enabled"]; exists {
					if enabled, ok := jetStreamEnabled.(bool); ok {
						return enabled
					}
				}

				// Check if account has streams
				if streamDetail, exists := accountDetail["stream_detail"]; exists {
					if streams, ok := streamDetail.([]interface{}); ok && len(streams) > 0 {
						return true
					}
				}

				// Check if account has JetStream limits/quotas configured
				if reservedMemory, exists := accountDetail["reserved_memory"]; exists {
					if memory, ok := reservedMemory.(float64); ok && memory > 0 {
						return true
					}
				}

				if reservedStorage, exists := accountDetail["reserved_storage"]; exists {
					if storage, ok := reservedStorage.(float64); ok && storage > 0 {
						return true
					}
				}
			}
		}
	}

	// Legacy check: If we have streams property at top level (for older API versions)
	if streams, exists := data["streams"]; exists {
		if streamsArray, ok := streams.([]interface{}); ok && len(streamsArray) > 0 {
			return true
		}
	}

	// If no clear indication, assume not enabled
	return false
}

// extractAndStandardizeStreams extracts stream information and calculates stats
func (s *ClusterMonitorService) extractAndStandardizeStreams(data map[string]interface{}) ([]models.StreamInfo, *models.AccountStreamStats) {
	var streams []models.StreamInfo
	stats := &models.AccountStreamStats{}

	// Try to extract streams from account_details first
	if accountDetails, exists := data["account_details"]; exists {
		if detailsArray, ok := accountDetails.([]interface{}); ok && len(detailsArray) > 0 {
			if accountDetail, ok := detailsArray[0].(map[string]interface{}); ok {
				if streamDetail, exists := accountDetail["stream_detail"]; exists {
					if streamsArray, ok := streamDetail.([]interface{}); ok {
						streams = s.parseStreamsFromArray(streamsArray)
					}
				}
			}
		}
	}

	// Fallback to top-level streams for older API versions
	if len(streams) == 0 {
		if topLevelStreams, exists := data["streams"]; exists {
			if streamsArray, ok := topLevelStreams.([]interface{}); ok {
				streams = s.parseStreamsFromArray(streamsArray)
			}
		}
	}

	// Calculate stats
	stats.TotalStreams = len(streams)
	var totalBytesSum int64
	for _, stream := range streams {
		if stream.State != nil {
			stats.TotalMessages += stream.State.Messages
			// 累加字节数
			totalBytesSum += models.ConvertToBytes(stream.State.BytesValue, stream.State.BytesUnit)
			stats.TotalConsumers += stream.State.ConsumerCount
		}

		switch stream.Status {
		case "active":
			stats.ActiveStreams++
		case "inactive":
			stats.InactiveStreams++
		case "error":
			stats.ErrorStreams++
		}
	}

	// 设置最终的统计字段
	stats.TotalBytesValue, stats.TotalBytesUnit = models.ConvertFromBytes(totalBytesSum)

	return streams, stats
}

// parseStreamsFromArray parses stream array and determines status for each stream
func (s *ClusterMonitorService) parseStreamsFromArray(streamsArray []interface{}) []models.StreamInfo {
	var streams []models.StreamInfo

	for _, streamData := range streamsArray {
		if streamMap, ok := streamData.(map[string]interface{}); ok {
			stream := models.StreamInfo{
				Name:   s.extractStringValue(streamMap, "name"),
				Status: s.determineStreamStatus(streamMap),
			}

			// Parse config if available
			if configData, exists := streamMap["config"]; exists {
				if configMap, ok := configData.(map[string]interface{}); ok {
					stream.Config = s.parseStreamConfig(configMap)
				}
			}

			// Parse state if available
			if stateData, exists := streamMap["state"]; exists {
				if stateMap, ok := stateData.(map[string]interface{}); ok {
					stream.State = s.parseStreamState(stateMap)
				}
			}

			// Parse created timestamp if available
			if created, exists := streamMap["created"]; exists {
				if createdStr, ok := created.(string); ok {
					if createdTime, err := time.Parse(time.RFC3339, createdStr); err == nil {
						customTime := models.NewCustomTime(createdTime)
						stream.CreatedAt = &customTime
					}
				}
			}

			streams = append(streams, stream)
		}
	}

	return streams
}

// determineStreamStatus implements the stream status logic from frontend
func (s *ClusterMonitorService) determineStreamStatus(streamData map[string]interface{}) string {
	// Check if stream has recent activity
	if stateData, exists := streamData["state"]; exists {
		if stateMap, ok := stateData.(map[string]interface{}); ok {
			if messages, exists := stateMap["messages"]; exists {
				if msgCount, ok := messages.(float64); ok && msgCount > 0 {
					// Check if last message is recent (within last 30 days)
					if lastTs, exists := stateMap["last_ts"]; exists {
						if lastTsStr, ok := lastTs.(string); ok {
							if lastTime, err := time.Parse(time.RFC3339, lastTsStr); err == nil {
								thirtyDaysAgo := time.Now().AddDate(0, 0, -30)
								if lastTime.After(thirtyDaysAgo) {
									return "active"
								} else {
									return "inactive" // Has messages but old
								}
							}
						}
					}
					return "active" // Has messages, assume active
				}
				// No messages, but stream exists
				return "inactive"
			}
		}
	}

	// Check cluster status if available
	if clusterData, exists := streamData["cluster"]; exists {
		if clusterMap, ok := clusterData.(map[string]interface{}); ok {
			if leader, exists := clusterMap["leader"]; exists && leader != nil {
				return "active"
			}
		}
	}

	// Default to active if stream exists
	return "active"
}

// Helper methods for parsing stream config and state
func (s *ClusterMonitorService) parseStreamConfig(configMap map[string]interface{}) *models.StreamConfig {
	streamConfig := &models.StreamConfig{
		Name: s.extractStringValue(configMap, "name"),
	}

	// Parse subjects
	if subjects, exists := configMap["subjects"]; exists {
		if subjectsArray, ok := subjects.([]interface{}); ok {
			for _, subj := range subjectsArray {
				if subjStr, ok := subj.(string); ok {
					streamConfig.Subjects = append(streamConfig.Subjects, subjStr)
				}
			}
		}
	}

	// Parse storage type
	if storage, exists := configMap["storage"]; exists {
		if storageStr, ok := storage.(string); ok {
			streamConfig.Storage = models.JetStreamStorageType(storageStr)
		}
	}

	// Parse other config fields
	streamConfig.Retention = models.JetStreamRetentionPolicy(s.extractStringValue(configMap, "retention"))
	streamConfig.Discard = models.JetStreamDiscardPolicy(s.extractStringValue(configMap, "discard"))
	streamConfig.Compression = models.JetStreamCompressionType(s.extractStringValue(configMap, "compression"))
	streamConfig.MaxMsgs = s.extractInt64Value(configMap, "max_msgs")
	// 从原始字节数转换为带单位的格式
	maxBytes := s.extractInt64Value(configMap, "max_bytes")
	streamConfig.MaxBytesValue, streamConfig.MaxBytesUnit = models.ConvertFromBytes(maxBytes)
	streamConfig.MaxAge = s.extractInt64Value(configMap, "max_age")
	streamConfig.Replicas = s.extractIntValue(configMap, "num_replicas")
	streamConfig.NoAck = s.extractBoolValue(configMap, "no_ack")
	streamConfig.AllowDirect = s.extractBoolValue(configMap, "allow_direct")
	streamConfig.AllowRollupHdrs = s.extractBoolValue(configMap, "allow_rollup_hdrs")
	streamConfig.DenyDelete = s.extractBoolValue(configMap, "deny_delete")
	streamConfig.DenyPurge = s.extractBoolValue(configMap, "deny_purge")
	streamConfig.DuplicateWindow = s.extractInt64Value(configMap, "duplicate_window")

	return streamConfig
}

func (s *ClusterMonitorService) parseStreamState(stateMap map[string]interface{}) *models.StreamState {
	// 从原始字节数转换为带单位的格式
	bytes := s.extractUint64Value(stateMap, "bytes")
	bytesValue, bytesUnit := models.ConvertFromBytes(int64(bytes))

	state := &models.StreamState{
		Messages:      s.extractUint64Value(stateMap, "messages"),
		BytesValue:    bytesValue,
		BytesUnit:     bytesUnit,
		FirstSeq:      s.extractUint64Value(stateMap, "first_seq"),
		LastSeq:       s.extractUint64Value(stateMap, "last_seq"),
		ConsumerCount: s.extractIntValue(stateMap, "consumer_count"),
	}

	// Parse timestamp
	if lastTs, exists := stateMap["last_ts"]; exists {
		if lastTsStr, ok := lastTs.(string); ok {
			if lastTime, err := time.Parse(time.RFC3339, lastTsStr); err == nil {
				customTime := models.NewCustomTime(lastTime)
				state.LastTs = &customTime
			}
		}
	}

	return state
}

// Helper methods for type-safe value extraction
func (s *ClusterMonitorService) extractStringValue(data map[string]interface{}, key string) string {
	if value, exists := data[key]; exists {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

func (s *ClusterMonitorService) extractIntValue(data map[string]interface{}, key string) int {
	if value, exists := data[key]; exists {
		if num, ok := value.(float64); ok {
			return int(num)
		}
		if num, ok := value.(int); ok {
			return num
		}
	}
	return 0
}

func (s *ClusterMonitorService) extractInt64Value(data map[string]interface{}, key string) int64 {
	if value, exists := data[key]; exists {
		if num, ok := value.(float64); ok {
			return int64(num)
		}
		if num, ok := value.(int64); ok {
			return num
		}
		if num, ok := value.(int); ok {
			return int64(num)
		}
	}
	return 0
}

func (s *ClusterMonitorService) extractUint64Value(data map[string]interface{}, key string) uint64 {
	if value, exists := data[key]; exists {
		if num, ok := value.(float64); ok {
			return uint64(num)
		}
		if num, ok := value.(uint64); ok {
			return num
		}
		if num, ok := value.(int); ok {
			return uint64(num)
		}
	}
	return 0
}

func (s *ClusterMonitorService) extractBoolValue(data map[string]interface{}, key string) bool {
	if value, exists := data[key]; exists {
		if b, ok := value.(bool); ok {
			return b
		}
	}
	return false
}

// GetStreamDetailInfo returns detailed information for a specific stream in standardized format
func (s *ClusterMonitorService) GetStreamDetailInfo(clusterID, accountPublicKey, streamName string) (*models.StreamInfo, error) {
	// Get account info for authentication
	account, err := s.repo.GetAccountByPublicKey(accountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get account by public key: %w", err)
	}
	if account == nil {
		return nil, fmt.Errorf("account not found for public key: %s", accountPublicKey)
	}

	// Connect using account authentication directly
	nc, _, err := s.clusterSvc.GetNatsJetStreamWithAccount(clusterID, account.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect with account: %w", err)
	}
	defer s.clusterSvc.CloseConnection(nc)

	// Get stream info using JS API
	streamInfo, err := s.natsServer.GetStreamInfoWithJSAPI(nc, streamName)
	jstr, _ := json.Marshal(streamInfo)
	log.Info("info: %+v", string(jstr))
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info for %s: %w", streamName, err)
	}

	// Parse the stream info into standardized format
	streamData := s.parseStreamFromMap(streamInfo)
	return &streamData, nil
}

// parseStreamFromMap parses a single stream from map format
func (s *ClusterMonitorService) parseStreamFromMap(streamMap map[string]interface{}) models.StreamInfo {
	stream := models.StreamInfo{
		Name:   s.extractStringValue(streamMap, "name"),
		Status: s.determineStreamStatus(streamMap),
	}

	// Parse config if available
	if configData, exists := streamMap["config"]; exists {
		if configMap, ok := configData.(map[string]interface{}); ok {
			stream.Config = s.parseStreamConfig(configMap)
		}
	}

	// Parse state if available
	if stateData, exists := streamMap["state"]; exists {
		if stateMap, ok := stateData.(map[string]interface{}); ok {
			stream.State = s.parseStreamState(stateMap)
		}
	}

	// Parse created timestamp if available
	if created, exists := streamMap["created"]; exists {
		if createdStr, ok := created.(string); ok {
			if createdTime, err := time.Parse(time.RFC3339, createdStr); err == nil {
				customTime := models.NewCustomTime(createdTime)
				stream.CreatedAt = &customTime
			}
		}
	}

	return stream
}

// DeleteJetStreamFromCluster deletes a JetStream stream directly from NATS cluster
func (s *ClusterMonitorService) DeleteJetStreamFromCluster(clusterID, accountID, streamName string) error {
	log.WithContext(context.Background()).Infof("开始删除JetStream流: cluster_id=%s, account_id=%s, stream_name=%s", clusterID, accountID, streamName)

	// Use cluster service to get JetStream context with admin user
	conn, js, err := s.clusterSvc.GetNatsJetStreamWithAccount(clusterID, accountID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer s.clusterSvc.CloseConnection(conn)

	// Delete the stream
	err = s.natsServer.DeleteStream(js, streamName)
	if err != nil {
		// Check if the error is stream not found
		if strings.Contains(err.Error(), "stream not found") || strings.Contains(err.Error(), "10059") {
			log.WithContext(context.Background()).Warnf("JetStream流不存在，可能已被删除: stream_name=%s", streamName)
			return fmt.Errorf("stream not found: %s", streamName)
		}
		return fmt.Errorf("failed to delete stream: %w", err)
	}

	log.WithContext(context.Background()).Infof("JetStream流删除成功: cluster_id=%s, account_id=%s, stream_name=%s", clusterID, accountID, streamName)
	return nil
}

// ListClusterServers 获取集群中所有服务器信息
func (s *ClusterMonitorService) ListClusterServers(clusterID string, timeout time.Duration, expectedServers int) (*models.ServerListResponse, error) {
	log.WithContext(context.Background()).Infof("ListClusterServers - clusterID: %s, timeout: %v, expected: %d", clusterID, timeout, expectedServers)

	// 使用系统账户管理员身份获取连接
	conn, err := s.clusterSvc.GetConnection(clusterID)
	if err != nil {
		log.WithContext(context.Background()).Errorf("ListClusterServers failed to get connection: %v", err)
		return nil, fmt.Errorf("failed to get cluster connection: %w", err)
	}
	defer s.clusterSvc.CloseConnection(conn)

	// 使用NATS service的ListServers方法
	response, err := s.natsServer.ListServers(conn, timeout, expectedServers)
	if err != nil {
		log.WithContext(context.Background()).Errorf("ListClusterServers failed to list servers: %v", err)
		return nil, fmt.Errorf("failed to list servers: %w", err)
	}

	// 设置响应中的集群ID
	response.ClusterID = clusterID

	log.WithContext(context.Background()).Infof("ListClusterServers success - cluster: %s, found %d servers", clusterID, response.TotalServers)
	return response, nil
}

// GetStreamConsumerList 获取指定JetStream下的所有Consumer
func (s *ClusterMonitorService) GetStreamConsumerList(clusterID string, streamName string) ([]string, error) {
	log.WithContext(context.Background()).Infof("GetStreamConsumerList - stream_name: %s", streamName)

	jetstreamData, err := s.repo.GetJetStreamByNameAndCluster(streamName, clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get jetstream: %w", err)
	}

	conn, js, err := s.clusterSvc.GetNatsJetStreamWithUser(clusterID, jetstreamData.NatsOperateUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get system connection: %w", err)
	}
	defer s.clusterSvc.CloseConnection(conn)

	consumers, err := s.natsServer.ListConsumers(js, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumers: %w", err)
	}

	log.WithContext(context.Background()).Infof("GetStreamConsumerList success - stream_name: %s, found %d consumers", streamName, len(consumers))
	return consumers, nil
}

func (s *ClusterMonitorService) GetStreamConsumerInfo(clusterID string, streamName string, consumerName string) (*nats.ConsumerInfo, error) {
	log.WithContext(context.Background()).Infof("GetStreamConsumerInfo - stream_name: %s, consumer_name: %s", streamName, consumerName)

	jetstreamData, err := s.repo.GetJetStreamByNameAndCluster(streamName, clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get jetstream: %w", err)
	}

	conn, jsc, err := s.clusterSvc.GetNatsJetStreamContextWithUsr(clusterID, jetstreamData.NatsOperateUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get system connection: %w", err)
	}
	defer s.clusterSvc.CloseConnection(conn)

	consumerInfo, err := s.natsServer.GetConsumerInfo(jsc, streamName, consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info: %w", err)
	}

	log.WithContext(context.Background()).Infof("GetStreamConsumerInfo success - stream_name: %s, consumer_name: %s", streamName, consumerName)
	return consumerInfo, nil
}

func (s *ClusterMonitorService) DeleteConsumerFromCluster(clusterID, accountID, streamName, consumerName string) error {
	log.WithContext(context.Background()).Infof("开始删除Consumer: cluster_id=%s, account_id=%s, stream_name=%s, consumer_name=%s", clusterID, accountID, streamName, consumerName)

	conn, js, err := s.clusterSvc.GetNatsJetStreamWithAccount(clusterID, accountID)
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	defer s.clusterSvc.CloseConnection(conn)

	err = s.natsServer.DeleteConsumer(js, streamName, consumerName)
	if err != nil {
		if strings.Contains(err.Error(), "consumer not found") || strings.Contains(err.Error(), "10014") {
			log.WithContext(context.Background()).Warnf("Consumer不存在，可能已被删除: stream_name=%s, consumer_name=%s", streamName, consumerName)
			return fmt.Errorf("consumer not found: %s", consumerName)
		}
		return fmt.Errorf("failed to delete consumer: %w", err)
	}

	log.WithContext(context.Background()).Infof("Consumer删除成功: cluster_id=%s, account_id=%s, stream_name=%s, consumer_name=%s", clusterID, accountID, streamName, consumerName)
	return nil
}
