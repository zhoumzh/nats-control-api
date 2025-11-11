package models

import (
	"time"
)

// ServerStatsMsg NATS服务器统计信息结构体
type ServerStatsMsg struct {
	Server ServerInfo  `json:"server"`
	Stats  ServerStats `json:"statsz"`
}

// ServerInfo NATS服务器基本信息
type ServerInfo struct {
	Name              string   `json:"name"`
	ID                string   `json:"id"`
	Cluster           string   `json:"cluster,omitempty"`
	Version           string   `json:"ver"`
	GoVersion         string   `json:"go"`
	Host              string   `json:"host"`
	Port              int      `json:"port"`
	AuthRequired      bool     `json:"auth_required,omitempty"`
	TLSRequired       bool     `json:"tls_required,omitempty"`
	TLSVerify         bool     `json:"tls_verify,omitempty"`
	IP                string   `json:"ip,omitempty"`
	ClientConnectURLs []string `json:"connect_urls,omitempty"`
	WSConnectURLs     []string `json:"ws_connect_urls,omitempty"`
	JetStream         bool     `json:"jetstream"`
	XKey              string   `json:"xkey,omitempty"`
	Domain            string   `json:"domain,omitempty"`
	Flags             int      `json:"flags,omitempty"`
	Seq               int      `json:"seq,omitempty"`
	Time              string   `json:"time,omitempty"`
}

// TrafficStats 流量统计信息
type TrafficStats struct {
	Msgs  uint64 `json:"msgs"`
	Bytes uint64 `json:"bytes"`
}

// RouteStats 路由统计信息
type RouteStats struct {
	RID      int           `json:"rid"`
	Name     string        `json:"name"`
	Sent     TrafficStats  `json:"sent"`
	Received TrafficStats  `json:"received"`
	Pending  int           `json:"pending"`
}

// GatewayStatsDetail 网关统计详细信息
type GatewayStatsDetail struct {
	GWID                int          `json:"gwid"`
	Name                string       `json:"name"`
	Sent                TrafficStats `json:"sent"`
	Received            TrafficStats `json:"received"`
	InboundConnections  int          `json:"inbound_connections"`
}

// ServerStats NATS服务器统计信息
type ServerStats struct {
	Start            time.Time              `json:"start"`
	Now              time.Time              `json:"now"`
	Uptime           string                 `json:"uptime"`
	Mem              int64                  `json:"mem"`
	Cores            int                    `json:"cores"`
	CPU              float64                `json:"cpu"`
	Connections      int                    `json:"connections"`
	TotalConnections uint64                 `json:"total_connections"`
	ActiveAccounts   int                    `json:"active_accounts"`
	Subscriptions    uint32                 `json:"subscriptions"`
	Sent             TrafficStats           `json:"sent"`
	Received         TrafficStats           `json:"received"`
	SlowConsumers    uint64                 `json:"slow_consumers"`
	Routes           []RouteStats           `json:"routes"`
	Gateways         []GatewayStatsDetail   `json:"gateways"`
	ActiveServers    int                    `json:"active_servers"`
	JetStream        *JetStreamStatsDetail  `json:"jetstream,omitempty"`
	GoMaxProcs       int                    `json:"gomaxprocs"`
	
	// 向后兼容的字段
	HTTPReqStats     map[string]uint64 `json:"http_req_stats"`
	ConfigLoadTime   time.Time         `json:"config_load_time"`
	Routes_Count     int               `json:"routes"` // for backward compatibility
	Remotes          int               `json:"remotes"`
	Leafnodes        int               `json:"leafnodes"`
	InMsgs           uint64            `json:"in_msgs"`
	OutMsgs          uint64            `json:"out_msgs"`
	InBytes          uint64            `json:"in_bytes"`
	OutBytes         uint64            `json:"out_bytes"`
}

// JetStreamStatsDetail JetStream详细统计信息
type JetStreamStatsDetail struct {
	Config JetStreamConfig `json:"config"`
	Stats  JetStreamStats  `json:"stats"`
	Meta   JetStreamMeta   `json:"meta"`
	Limits interface{}     `json:"limits"`
}

// JetStreamConfig JetStream配置信息
type JetStreamConfig struct {
	MaxMemory    int64  `json:"max_memory"`
	MaxStorage   int64  `json:"max_storage"`
	SyncInterval int64  `json:"sync_interval"`
	Domain       string `json:"domain"`
	Strict       bool   `json:"strict"`
}

// JetStreamMeta JetStream元数据信息
type JetStreamMeta struct {
	Name        string `json:"name"`
	Leader      string `json:"leader"`
	Peer        string `json:"peer"`
	ClusterSize int    `json:"cluster_size"`
	Pending     int    `json:"pending"`
}

// JetStreamStats JetStream统计信息
type JetStreamStats struct {
	Memory         uint64                `json:"memory"`
	Storage        uint64                `json:"storage"`
	ReservedMemory uint64                `json:"reserved_memory,omitempty"`
	ReservedStorage uint64               `json:"reserved_storage,omitempty"`
	Accounts       int                   `json:"accounts"`
	HAAssets       int                   `json:"ha_assets"`
	API            JetStreamAPIStats     `json:"api"`
}

// JetStreamAPIStats JetStream API统计信息
type JetStreamAPIStats struct {
	Level  int    `json:"level"`
	Total  uint64 `json:"total"`
	Errors uint64 `json:"errors"`
}

// GatewayStats 网关统计信息
type GatewayStats struct {
	Name     string `json:"name,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	InMsgs   uint64 `json:"in_msgs"`
	OutMsgs  uint64 `json:"out_msgs"`
	InBytes  uint64 `json:"in_bytes"`
	OutBytes uint64 `json:"out_bytes"`
}

// ServerListResponse 服务器列表响应结构体
type ServerListResponse struct {
	Servers      []ServerStatsMsg `json:"servers"`
	TotalServers int              `json:"total_servers"`
	RequestedAt  CustomTime       `json:"requested_at"`
	Timeout      string           `json:"timeout"`
	ClusterID    string           `json:"cluster_id"`
}

// ServerListRequest 服务器列表请求结构体
type ServerListRequest struct {
	ClusterID string        `json:"cluster_id" binding:"required"`
	Timeout   time.Duration `json:"timeout,omitempty"`
	Expected  int           `json:"expected,omitempty"`
}