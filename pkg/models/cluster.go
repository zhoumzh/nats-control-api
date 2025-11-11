package models

import (
	"fmt"
)

// Cluster represents a NATS cluster in the super cluster
type Cluster struct {
	ID          string        `json:"id" gorm:"primaryKey;type:varchar(18)"`
	Name        string        `json:"name" gorm:"uniqueIndex;not null;type:varchar(255)" binding:"required"`
	Description string        `json:"description" gorm:"type:text"`
	Status      ClusterStatus `json:"status" gorm:"not null;type:varchar(50);default:'active'"`

	// NATS连接配置 - 拆分为具体字段
	Host        string `json:"host" gorm:"not null;type:varchar(255)" binding:"required"`
	NATSPort    int    `json:"nats_port" gorm:"not null;default:4222"`
	GatewayPort int    `json:"gateway_port" gorm:"not null;default:7222"`
	MonitorPort int    `json:"monitor_port" gorm:"not null;default:8222"`
	ClusterPort int    `json:"cluster_port" gorm:"not null;default:6222"`

	// 系统账户配置
	SystemAccountID string `json:"system_account_id" gorm:"type:varchar(18);index"`
	SystemUserID    string `json:"system_user_id" gorm:"type:varchar(18);index"`

	CreatedAt CustomTime `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt CustomTime `json:"updated_at" gorm:"autoUpdateTime"`
}

type ClusterStatus string

const (
	ClusterStatusActive   ClusterStatus = "active"
	ClusterStatusDisabled ClusterStatus = "disabled"
)

// GetNATSURL 构建完整的NATS连接URL
func (c *Cluster) GetNATSURL() string {
	return fmt.Sprintf("nats://%s:%d", c.Host, c.NATSPort)
}

// GetMonitorURL 构建监控URL
func (c *Cluster) GetMonitorURL() string {
	return fmt.Sprintf("http://%s:%d", c.Host, c.MonitorPort)
}

// Request/Response DTOs for Cluster APIs
type CreateClusterRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`

	// NATS连接配置
	Host        string `json:"host" binding:"required"`
	NATSPort    int    `json:"nats_port"`    // 默认4222
	GatewayPort int    `json:"gateway_port"` // 默认7222
	MonitorPort int    `json:"monitor_port"` // 默认8222
	ClusterPort int    `json:"cluster_port"` // 默认6222

	// 系统账户配置
	SystemAccountID string `json:"system_account_id,omitempty"`
	SystemUserID    string `json:"system_user_id,omitempty"`
}

type UpdateClusterRequest struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Status      ClusterStatus `json:"status"`

	// NATS连接配置
	Host        string `json:"host"`
	NATSPort    int    `json:"nats_port"`
	GatewayPort int    `json:"gateway_port"`
	MonitorPort int    `json:"monitor_port"`
	ClusterPort int    `json:"cluster_port"`

	// 系统账户配置
	SystemAccountID string `json:"system_account_id"`
	SystemUserID    string `json:"system_user_id"`
}

// Cluster monitoring models

// ClusterHealth represents cluster health monitoring record
type ClusterHealth struct {
	ID        string              `json:"id" gorm:"primaryKey;type:varchar(36)"`
	ClusterID string              `json:"cluster_id" gorm:"not null;type:varchar(18);index"`
	Status    ClusterHealthStatus `json:"status" gorm:"not null;type:varchar(50)"`
	
	// Connection test result
	ConnectionStatus string `json:"connection_status" gorm:"not null;type:varchar(50)"` // success, failed, timeout
	ResponseTime     int64  `json:"response_time" gorm:"not null;default:0"`            // milliseconds
	ErrorMessage     string `json:"error_message" gorm:"type:text"`                     // error details if failed
	
	// Additional monitoring data
	NATSURL     string `json:"nats_url" gorm:"type:varchar(500)"`                // tested URL
	MonitorURL  string `json:"monitor_url" gorm:"type:varchar(500)"`             // monitor endpoint URL
	TestType    string `json:"test_type" gorm:"not null;type:varchar(50)"`       // auto, manual
	TestedAt    CustomTime `json:"tested_at" gorm:"not null"`                    // when the test was performed
	
	// Cluster relation
	Cluster *Cluster `json:"cluster,omitempty" gorm:"foreignKey:ClusterID"`
	
	CreatedAt CustomTime `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt CustomTime `json:"updated_at" gorm:"autoUpdateTime"`
}

type ClusterHealthStatus string

const (
	ClusterHealthStatusHealthy   ClusterHealthStatus = "healthy"
	ClusterHealthStatusUnhealthy ClusterHealthStatus = "unhealthy"
	ClusterHealthStatusUnknown   ClusterHealthStatus = "unknown"
)


// ClusterStats represents cluster statistics for dashboard
type ClusterStats struct {
	TotalClusters     int `json:"total_clusters"`
	HealthyClusters   int `json:"healthy_clusters"`
	UnhealthyClusters int `json:"unhealthy_clusters"`
	UnknownClusters   int `json:"unknown_clusters"`
}

// Request/Response DTOs for monitoring APIs

// ClusterHealthListRequest represents query parameters for listing health records
type ClusterHealthListRequest struct {
	ClusterID        string `form:"cluster_id"`
	Status           string `form:"status"`
	ConnectionStatus string `form:"connection_status"`
	StartDate        string `form:"start_date"`    // YYYY-MM-DD format
	EndDate          string `form:"end_date"`      // YYYY-MM-DD format
	Page             int    `form:"page" binding:"min=1"`
	PageSize         int    `form:"page_size" binding:"min=1,max=100"`
}


// ClusterHealthResponse represents health check response with additional info
type ClusterHealthResponse struct {
	*ClusterHealth
	ClusterName string `json:"cluster_name,omitempty"`
	HostInfo    string `json:"host_info,omitempty"`
}

// ClusterMonitoringDashboardResponse represents dashboard monitoring data
type ClusterMonitoringDashboardResponse struct {
	Stats           *ClusterStats                `json:"stats"`
	UnhealthyCount  int                         `json:"unhealthy_count"`
	RecentFailures  []*ClusterHealthResponse    `json:"recent_failures"`
	LastUpdateTime  CustomTime                  `json:"last_update_time"`
}

// Super cluster topology models

// GatewayInfo represents gateway connection information from /gatewayz endpoint
type GatewayInfo struct {
	ServerID         string                 `json:"server_id"`
	Name             string                 `json:"name"`
	Host             string                 `json:"host"`
	Port             int                    `json:"port"`
	OutboundGateways map[string]GatewayConnection `json:"outbound_gateways"`
	InboundGateways  map[string][]GatewayConnection `json:"inbound_gateways"`
}

// GatewayConnection represents a single gateway connection
type GatewayConnection struct {
	Configured bool               `json:"configured"`
	Connection *ConnectionDetails `json:"connection"`
}

// ConnectionDetails represents the connection details
type ConnectionDetails struct {
	CID  int    `json:"cid"`
	Kind string `json:"kind"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
	Name string `json:"name"`
}

// ClusterTopologyNode represents a cluster node in the topology
type ClusterTopologyNode struct {
	ClusterID        string                   `json:"cluster_id"`
	ClusterName      string                   `json:"cluster_name"`
	Host             string                   `json:"host"`
	GatewayPort      int                      `json:"gateway_port"`
	MonitorPort      int                      `json:"monitor_port"`
	ConnectionStatus string                   `json:"connection_status"` // connected, isolated, partial
	IncomingConnections []*ClusterConnection `json:"incoming_connections"`
	OutgoingConnections []*ClusterConnection `json:"outgoing_connections"`
	GatewayUrls      []string                 `json:"gateway_urls"`
}

// ClusterConnection represents a connection between clusters
type ClusterConnection struct {
	FromClusterID   string `json:"from_cluster_id"`
	FromClusterName string `json:"from_cluster_name"`
	ToClusterID     string `json:"to_cluster_id"`
	ToClusterName   string `json:"to_cluster_name"`
	ConnectionType  string `json:"connection_type"` // gateway
	Status          string `json:"status"`          // active, inactive
}

// SuperClusterGroup represents a group of connected clusters
type SuperClusterGroup struct {
	GroupID    string                 `json:"group_id"`
	GroupName  string                 `json:"group_name"`
	Clusters   []*ClusterTopologyNode `json:"clusters"`
	Connections []*ClusterConnection  `json:"connections"`
}

// ClusterTopologyResponse represents the complete cluster topology
type ClusterTopologyResponse struct {
	SuperClusterGroups []*SuperClusterGroup `json:"super_cluster_groups"`
	IsolatedClusters   []*ClusterTopologyNode `json:"isolated_clusters"`
	TotalClusters      int                    `json:"total_clusters"`
	ConnectedClusters  int                    `json:"connected_clusters"`
	IsolatedCount      int                    `json:"isolated_count"`
	SuperClusterCount  int                    `json:"super_cluster_count"`
}