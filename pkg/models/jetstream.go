package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MetadataMap 自定义类型用于处理metadata的JSON序列化
type MetadataMap map[string]string

// StringSlice 自定义类型用于处理字符串切片的JSON序列化
type StringSlice []string

// Scan 实现sql.Scanner接口，用于从数据库读取
func (s *StringSlice) Scan(value interface{}) error {
	if value == nil {
		*s = make(StringSlice, 0)
		return nil
	}

	switch v := value.(type) {
	case []byte:
		if len(v) == 0 {
			*s = make(StringSlice, 0)
			return nil
		}
		return json.Unmarshal(v, s)
	case string:
		if v == "" {
			*s = make(StringSlice, 0)
			return nil
		}
		// 尝试作为JSON解析
		if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
			return json.Unmarshal([]byte(v), s)
		}
		// 否则按逗号分割
		*s = StringSlice(strings.Split(v, ","))
		return nil
	default:
		return fmt.Errorf("cannot scan %T into StringSlice", value)
	}
}

// Value 实现driver.Valuer接口，用于写入数据库
func (s StringSlice) Value() (driver.Value, error) {
	if len(s) == 0 {
		return "[]", nil
	}
	return json.Marshal(s)
}

// Scan 实现sql.Scanner接口，用于从数据库读取
func (m *MetadataMap) Scan(value interface{}) error {
	if value == nil {
		*m = make(MetadataMap)
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into MetadataMap", value)
	}

	if len(bytes) == 0 {
		*m = make(MetadataMap)
		return nil
	}

	return json.Unmarshal(bytes, m)
}

// Value 实现driver.Valuer接口，用于写入数据库
func (m MetadataMap) Value() (driver.Value, error) {
	if len(m) == 0 {
		return nil, nil
	}
	return json.Marshal(m)
}

// JetStreamStorageType JetStream存储类型
type JetStreamStorageType string

const (
	JetStreamStorageFile   JetStreamStorageType = "file"
	JetStreamStorageMemory JetStreamStorageType = "memory"
)

// JetStreamRetentionPolicy JetStream保留策略
type JetStreamRetentionPolicy string

const (
	JetStreamRetentionLimits    JetStreamRetentionPolicy = "limits"
	JetStreamRetentionInterest  JetStreamRetentionPolicy = "interest"
	JetStreamRetentionWorkQueue JetStreamRetentionPolicy = "workqueue"
)

// JetStreamDiscardPolicy JetStream丢弃策略
type JetStreamDiscardPolicy string

const (
	JetStreamDiscardOld JetStreamDiscardPolicy = "old"
	JetStreamDiscardNew JetStreamDiscardPolicy = "new"
)

// JetStreamCompressionType JetStream压缩类型
type JetStreamCompressionType string

const (
	JetStreamCompressionNone JetStreamCompressionType = "none"
	JetStreamCompressionS2   JetStreamCompressionType = "s2"
)

// JetStreamSyncStatus 同步状态枚举
type JetStreamSyncStatus string

const (
	JetStreamSyncPending JetStreamSyncStatus = "pending" // 待同步
	JetStreamSyncSyncing JetStreamSyncStatus = "syncing" // 同步中
	JetStreamSyncSynced  JetStreamSyncStatus = "synced"  // 已同步
	JetStreamSyncFailed  JetStreamSyncStatus = "failed"  // 同步失败
)

// JetStream 流配置模型
type JetStream struct {
	ID          string `json:"id" gorm:"primaryKey;type:varchar(18)"`
	Name        string `json:"name" gorm:"type:varchar(255);not null;index"`
	Description string `json:"description" gorm:"type:text"`
	// 重构字段：操作NATS的用户身份
	NatsOperateUserID string `json:"nats_operate_user_id" gorm:"type:varchar(18);not null;index"`
	ClusterID         string `json:"cluster_id" gorm:"type:varchar(18);not null;index"`
	Status            string `json:"status" gorm:"type:varchar(20);default:'active'"` // active, inactive, error

	// 新增：同步状态管理
	SyncStatus  JetStreamSyncStatus `json:"sync_status" gorm:"type:varchar(20);default:'pending';index"`
	SyncMessage string              `json:"sync_message" gorm:"type:text"`

	// 流配置
	Subjects    StringSlice              `json:"subjects" gorm:"type:json"`
	Storage     JetStreamStorageType     `json:"storage" gorm:"type:varchar(20);default:'file'"`
	Retention   JetStreamRetentionPolicy `json:"retention" gorm:"type:varchar(20);default:'limits'"`
	Discard     JetStreamDiscardPolicy   `json:"discard" gorm:"type:varchar(20);default:'old'"`
	Compression JetStreamCompressionType `json:"compression" gorm:"type:varchar(20);default:'none'"`

	// 限制配置
	MaxMsgs           int64       `json:"max_msgs" gorm:"default:-1"`                              // 最大消息数量，-1表示无限制
	MaxBytesValue     float64     `json:"max_bytes_value" gorm:"type:decimal(15,3);default:-1"`    // 最大字节数值
	MaxBytesUnit      StorageUnit `json:"max_bytes_unit" gorm:"type:varchar(10);default:'B'"`      // 最大字节数单位
	MaxAge            int64       `json:"max_age" gorm:"type:bigint"`                              // 消息最大存活时间（秒）
	MaxMsgSizeValue   float64     `json:"max_msg_size_value" gorm:"type:decimal(15,3);default:-1"` // 最大单条消息大小数值
	MaxMsgSizeUnit    StorageUnit `json:"max_msg_size_unit" gorm:"type:varchar(10);default:'B'"`   // 最大单条消息大小单位
	MaxConsumers      int         `json:"max_consumers" gorm:"default:-1"`                         // 最大消费者数量
	MaxMsgsPerSubject int64       `json:"max_msgs_per_subject" gorm:"default:-1"`                  // 每个主题的最大消息数量

	// 高级配置
	DiscardNewPerSubject bool `json:"discard_new_per_subject" gorm:"default:false"` // 按主题丢弃新消息
	Sealed               bool `json:"sealed" gorm:"default:false"`                  // 是否密封流

	// 副本和持久化配置
	Replicas        int   `json:"replicas" gorm:"default:1"`           // 副本数量
	NoAck           bool  `json:"no_ack" gorm:"default:false"`         // 是否禁用确认
	DuplicateWindow int64 `json:"duplicate_window" gorm:"type:bigint"` // 重复检测窗口（秒）
	AllowRollupHdrs bool  `json:"allow_rollup_hdrs" gorm:"default:false"`
	AllowDirect     bool  `json:"allow_direct" gorm:"default:false"`
	MirrorDirect    bool  `json:"mirror_direct" gorm:"default:false"`
	DenyDelete      bool  `json:"deny_delete" gorm:"default:false"`
	DenyPurge       bool  `json:"deny_purge" gorm:"default:false"`

	// 放置标签和元数据
	PlacementCluster string      `json:"placement_cluster" gorm:"type:varchar(255)"`
	PlacementTags    StringSlice `json:"placement_tags" gorm:"type:json"`
	Metadata         MetadataMap `json:"metadata" gorm:"type:json"`

	// 统计信息
	Messages     uint64      `json:"messages" gorm:"default:0"`                       // 当前消息数量
	BytesValue   float64     `json:"bytes_value" gorm:"type:decimal(15,3);default:0"` // 当前字节数值
	BytesUnit    StorageUnit `json:"bytes_unit" gorm:"type:varchar(10);default:'B'"`  // 当前字节数单位
	FirstSeq     uint64      `json:"first_seq" gorm:"default:0"`                      // 第一个序列号
	LastSeq      uint64      `json:"last_seq" gorm:"default:0"`                       // 最后一个序列号
	NumConsumers int         `json:"num_consumers" gorm:"default:0"`                  // 当前消费者数量

	// 时间戳
	CreatedAt CustomTime `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt CustomTime `json:"updated_at" gorm:"autoUpdateTime"`

	// 关联的NATS操作用户信息（不存储在数据库中）
	NatsOperateUser *User    `json:"nats_operate_user,omitempty" gorm:"-"`
	Cluster         *Cluster `json:"cluster,omitempty" gorm:"-"`
}

// CreateJetStreamRequest 创建JetStream请求
type CreateJetStreamRequest struct {
	Name              string `json:"name" binding:"required" example:"EVENTS"`
	Description       string `json:"description" example:"Event logging stream"`
	NatsOperateUserID string `json:"nats_operate_user_id" binding:"required" example:"user-123"`
	ClusterID         string `json:"cluster_id" binding:"required" example:"cluster-123"`

	// 流配置
	Subjects    StringSlice              `json:"subjects" binding:"required" example:"events.*,logs.*"`
	Storage     JetStreamStorageType     `json:"storage" example:"file"`
	Retention   JetStreamRetentionPolicy `json:"retention" example:"limits"`
	Discard     JetStreamDiscardPolicy   `json:"discard" example:"old"`
	Compression JetStreamCompressionType `json:"compression" example:"none"`

	// 限制配置 - 带单位的字节数
	MaxMsgs         int64       `json:"max_msgs" example:"1000000"`
	MaxBytesValue   float64     `json:"max_bytes_value" example:"1"`    // 最大字节数值
	MaxBytesUnit    StorageUnit `json:"max_bytes_unit" example:"GB"`    // 最大字节数单位
	MaxAge          int64       `json:"max_age" example:"86400"`        // 24小时（秒）
	MaxMsgSizeValue float64     `json:"max_msg_size_value" example:"1"` // 最大单条消息大小数值
	MaxMsgSizeUnit  StorageUnit `json:"max_msg_size_unit" example:"MB"` // 最大单条消息大小单位
	MaxConsumers    int         `json:"max_consumers" example:"100"`

	// 副本和持久化配置
	Replicas        int   `json:"replicas" example:"1"`
	NoAck           bool  `json:"no_ack" example:"false"`
	DuplicateWindow int64 `json:"duplicate_window" example:"120"` // 2分钟（秒）
	AllowRollupHdrs bool  `json:"allow_rollup_hdrs" example:"false"`
	AllowDirect     bool  `json:"allow_direct" example:"false"`
	MirrorDirect    bool  `json:"mirror_direct" example:"false"`
	DenyDelete      bool  `json:"deny_delete" example:"false"`
	DenyPurge       bool  `json:"deny_purge" example:"false"`

	// 放置配置
	PlacementCluster string      `json:"placement_cluster"`
	PlacementTags    StringSlice `json:"placement_tags"`
	Metadata         MetadataMap `json:"metadata"`
}

// UpdateJetStreamRequest 更新JetStream请求
type UpdateJetStreamRequest struct {
	NatsOperateUserID string `json:"nats_operate_user_id" binding:"required" example:"user-123"`
	Description       string `json:"description"`

	// 流配置（某些字段创建后不能修改）
	Subjects  StringSlice              `json:"subjects"`
	Storage   JetStreamStorageType     `json:"storage"`
	Retention JetStreamRetentionPolicy `json:"retention"`
	Discard   JetStreamDiscardPolicy   `json:"discard"`

	// 限制配置 - 带单位的字节数
	MaxMsgs         *int64       `json:"max_msgs"`
	MaxBytesValue   *float64     `json:"max_bytes_value"` // 最大字节数值
	MaxBytesUnit    *StorageUnit `json:"max_bytes_unit"`  // 最大字节数单位
	MaxAge          *int64       `json:"max_age"`
	MaxMsgSizeValue *float64     `json:"max_msg_size_value"` // 最大单条消息大小数值
	MaxMsgSizeUnit  *StorageUnit `json:"max_msg_size_unit"`  // 最大单条消息大小单位
	MaxConsumers    *int         `json:"max_consumers"`

	// 副本和持久化配置
	Replicas        *int  `json:"replicas"`
	NoAck           *bool `json:"no_ack"`
	DuplicateWindow int64 `json:"duplicate_window"`
	AllowRollupHdrs *bool `json:"allow_rollup_hdrs"`
	AllowDirect     *bool `json:"allow_direct"`
	MirrorDirect    *bool `json:"mirror_direct"`
	DenyDelete      *bool `json:"deny_delete"`
	DenyPurge       *bool `json:"deny_purge"`

	// 放置配置
	PlacementTags StringSlice `json:"placement_tags"`

	// 元数据
	Metadata MetadataMap `json:"metadata"`
}

// JetStreamListRequest 列表查询请求
type JetStreamListRequest struct {
	NatsOperateUserID string `form:"nats_operate_user_id" example:"user-123"`
	Status            string `form:"status" example:"active"`
	SyncStatus        string `form:"sync_status" example:"synced"`
	ClusterID         string `form:"cluster_id" example:"cluster-123"`
	Search            string `form:"search" example:"events"`
	Page              int    `form:"page" example:"1"`
	PageSize          int    `form:"page_size" example:"20"`
	SortBy            string `form:"sort_by" example:"created_at"`
	Order             string `form:"order" example:"desc"`
}

// JetStreamStatsResponse JetStream统计信息响应
type JetStreamStatsResponse struct {
	StreamName          string      `json:"stream_name"`
	NatsOperateUserID   string      `json:"nats_operate_user_id"`
	NatsOperateUserName string      `json:"nats_operate_user_name"`
	Messages            uint64      `json:"messages"`
	BytesValue          float64     `json:"bytes_value"` // 字节数值
	BytesUnit           StorageUnit `json:"bytes_unit"`  // 字节数单位
	FirstSeq            uint64      `json:"first_seq"`
	LastSeq             uint64      `json:"last_seq"`
	NumConsumers        int         `json:"num_consumers"`
	State               string      `json:"state"`
	SyncStatus          string      `json:"sync_status"`
	LastUpdate          time.Time   `json:"last_update"`
}

// TableName 指定表名
func (JetStream) TableName() string {
	return "jetstreams"
}

// GetMaxAgeDuration 获取MaxAge的Duration表示（用于与NATS API交互）
func (js *JetStream) GetMaxAgeDuration() time.Duration {
	return time.Duration(js.MaxAge) * time.Second
}

// SetMaxAgeFromDuration 从Duration设置MaxAge
func (js *JetStream) SetMaxAgeFromDuration(d time.Duration) {
	js.MaxAge = int64(d.Seconds())
}

// GetDuplicateWindowDuration 获取DuplicateWindow的Duration表示
func (js *JetStream) GetDuplicateWindowDuration() time.Duration {
	return time.Duration(js.DuplicateWindow) * time.Second
}

// SetDuplicateWindowFromDuration 从Duration设置DuplicateWindow
func (js *JetStream) SetDuplicateWindowFromDuration(d time.Duration) {
	js.DuplicateWindow = int64(d.Seconds())
}

// JetStreamMirror 流镜像配置模型
type JetStreamMirror struct {
	ID                string     `json:"id" gorm:"primaryKey;type:varchar(36)"`
	JetStreamID       string     `json:"jetstream_id" gorm:"type:varchar(36);not null;index"`
	SourceJetStreamID string     `json:"source_jetstream_id" gorm:"type:varchar(36);not null;index"`
	SourceClusterID   string     `json:"source_cluster_id" gorm:"type:varchar(36);not null;index"`
	FilterSubject     string     `json:"filter_subject" gorm:"type:varchar(255)"`
	OptStartSeq       uint64     `json:"opt_start_seq" gorm:"default:0"`
	OptStartTime      *time.Time `json:"opt_start_time"`
	Status            string     `json:"status" gorm:"type:varchar(20);default:'active'"` // active, inactive, error

	// 同步状态管理
	SyncStatus  JetStreamSyncStatus `json:"sync_status" gorm:"type:varchar(20);default:'pending';index"`
	SyncMessage string              `json:"sync_message" gorm:"type:text"`

	CreatedAt CustomTime `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt CustomTime `json:"updated_at" gorm:"autoUpdateTime"`

	// 关联信息（不存储在数据库中）
	JetStream       *JetStream `json:"jetstream,omitempty" gorm:"-"`
	SourceJetStream *JetStream `json:"source_jetstream,omitempty" gorm:"-"`
	SourceCluster   *Cluster   `json:"source_cluster,omitempty" gorm:"-"`
}

// JetStreamSource 流源配置模型
type JetStreamSource struct {
	ID                string     `json:"id" gorm:"primaryKey;type:varchar(36)"`
	JetStreamID       string     `json:"jetstream_id" gorm:"type:varchar(36);not null;index"`
	SourceJetStreamID string     `json:"source_jetstream_id" gorm:"type:varchar(36);not null;index"`
	SourceClusterID   string     `json:"source_cluster_id" gorm:"type:varchar(36);not null;index"`
	FilterSubject     string     `json:"filter_subject" gorm:"type:varchar(255)"`
	OptStartSeq       uint64     `json:"opt_start_seq" gorm:"default:0"`
	OptStartTime      *time.Time `json:"opt_start_time"`
	Status            string     `json:"status" gorm:"type:varchar(20);default:'active'"` // active, inactive, error

	// 同步状态管理
	SyncStatus  JetStreamSyncStatus `json:"sync_status" gorm:"type:varchar(20);default:'pending';index"`
	SyncMessage string              `json:"sync_message" gorm:"type:text"`

	CreatedAt CustomTime `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt CustomTime `json:"updated_at" gorm:"autoUpdateTime"`

	// 关联信息（不存储在数据库中）
	JetStream       *JetStream `json:"jetstream,omitempty" gorm:"-"`
	SourceJetStream *JetStream `json:"source_jetstream,omitempty" gorm:"-"`
	SourceCluster   *Cluster   `json:"source_cluster,omitempty" gorm:"-"`
}

// CreateJetStreamMirrorRequest 创建流镜像请求
type CreateJetStreamMirrorRequest struct {
	SourceJetStreamID string     `json:"source_jetstream_id" binding:"required" example:"jetstream-456"`
	SourceClusterID   string     `json:"source_cluster_id" binding:"required" example:"cluster-789"`
	FilterSubject     string     `json:"filter_subject" example:"events.*"`
	OptStartSeq       uint64     `json:"opt_start_seq" example:"0"`
	OptStartTime      *time.Time `json:"opt_start_time"`
}

// CreateJetStreamSourceRequest 创建流源请求
type CreateJetStreamSourceRequest struct {
	SourceJetStreamID string     `json:"source_jetstream_id" binding:"required" example:"jetstream-456"`
	SourceClusterID   string     `json:"source_cluster_id" binding:"required" example:"cluster-789"`
	FilterSubject     string     `json:"filter_subject" example:"events.*"`
	OptStartSeq       uint64     `json:"opt_start_seq" example:"0"`
	OptStartTime      *time.Time `json:"opt_start_time"`
}

// UpdateJetStreamMirrorRequest 更新流镜像请求
type UpdateJetStreamMirrorRequest struct {
	FilterSubject string     `json:"filter_subject"`
	OptStartSeq   *uint64    `json:"opt_start_seq"`
	OptStartTime  *time.Time `json:"opt_start_time"`
	Status        string     `json:"status"`
}

// UpdateJetStreamSourceRequest 更新流源请求
type UpdateJetStreamSourceRequest struct {
	FilterSubject string     `json:"filter_subject"`
	OptStartSeq   *uint64    `json:"opt_start_seq"`
	OptStartTime  *time.Time `json:"opt_start_time"`
	Status        string     `json:"status"`
}

// JetStreamMirrorResponse 流镜像响应
type JetStreamMirrorResponse struct {
	*JetStreamMirror
	Stats *JetStreamMirrorStats `json:"stats,omitempty"`
}

// JetStreamSourceResponse 流源响应
type JetStreamSourceResponse struct {
	*JetStreamSource
	Stats *JetStreamSourceStats `json:"stats,omitempty"`
}

// JetStreamMirrorStats 流镜像统计信息
type JetStreamMirrorStats struct {
	Lag    uint64        `json:"lag"`
	Active time.Duration `json:"active"`
}

// JetStreamSourceStats 流源统计信息
type JetStreamSourceStats struct {
	Lag    uint64        `json:"lag"`
	Active time.Duration `json:"active"`
}

// Validate 验证JetStream配置
func (req *CreateJetStreamRequest) Validate() error {
	if req.Name == "" {
		return NewValidationError("name", "name is required")
	}

	if req.NatsOperateUserID == "" {
		return NewValidationError("nats_operate_user_id", "nats_operate_user_id is required")
	}

	if req.ClusterID == "" {
		return NewValidationError("cluster_id", "cluster_id is required")
	}

	if len(req.Subjects) == 0 {
		return NewValidationError("subjects", "at least one subject is required")
	}

	if req.Replicas < 1 {
		return NewValidationError("replicas", "replicas must be at least 1")
	}

	if req.MaxMsgs < -1 {
		return NewValidationError("max_msgs", "max_msgs must be -1 (unlimited) or positive")
	}

	if req.MaxBytesValue < -1 {
		return NewValidationError("max_bytes", "max_bytes must be -1 (unlimited) or positive")
	}

	return nil
}

// TableName 指定表名
func (JetStreamMirror) TableName() string {
	return "jetstream_mirrors"
}

// TableName 指定表名
func (JetStreamSource) TableName() string {
	return "jetstream_sources"
}

// JetStreamConfigDiff 配置差异对比结果
type JetStreamConfigDiff struct {
	HasDifference  bool               `json:"has_difference"`
	DatabaseConfig *StreamConfig      `json:"database_config"`
	ClusterConfig  *StreamConfig      `json:"cluster_config"`
	Differences    []ConfigDifference `json:"differences"`
}

// ConfigDifference 单个配置差异
type ConfigDifference struct {
	Field         string      `json:"field"`
	DatabaseValue interface{} `json:"database_value"`
	ClusterValue  interface{} `json:"cluster_value"`
}

// StreamInfo 流信息
type StreamInfo struct {
	Name      string        `json:"name"`
	Config    *StreamConfig `json:"config,omitempty"`
	State     *StreamState  `json:"state,omitempty"`
	Status    string        `json:"status"` // active, inactive, error
	Error     string        `json:"error,omitempty"`
	CreatedAt *CustomTime   `json:"created_at,omitempty"`
}

// StreamConfig 流配置信息
type StreamConfig struct {
	Name          string                   `json:"name"`
	Subjects      []string                 `json:"subjects"`
	Storage       JetStreamStorageType     `json:"storage"`
	Retention     JetStreamRetentionPolicy `json:"retention"`
	Discard       JetStreamDiscardPolicy   `json:"discard"`
	Compression   JetStreamCompressionType `json:"compression"`
	MaxMsgs       int64                    `json:"max_msgs"`
	MaxBytesValue float64                  `json:"max_bytes_value"` // 字节数值
	MaxBytesUnit  StorageUnit              `json:"max_bytes_unit"`  // 字节数单位
	MaxAge        int64                    `json:"max_age"`
	Replicas      int                      `json:"num_replicas"`

	// 高级配置
	NoAck           bool  `json:"no_ack"`
	AllowDirect     bool  `json:"allow_direct"`
	AllowRollupHdrs bool  `json:"allow_rollup_hdrs"`
	DenyDelete      bool  `json:"deny_delete"`
	DenyPurge       bool  `json:"deny_purge"`
	DuplicateWindow int64 `json:"duplicate_window"`
}

// StreamState 流状态信息
type StreamState struct {
	Messages      uint64      `json:"messages"`
	BytesValue    float64     `json:"bytes_value"` // 字节数值
	BytesUnit     StorageUnit `json:"bytes_unit"`  // 字节数单位
	FirstSeq      uint64      `json:"first_seq"`
	LastSeq       uint64      `json:"last_seq"`
	LastTs        *CustomTime `json:"last_ts,omitempty"`
	ConsumerCount int         `json:"consumer_count"`
}

// AccountStreamStats 账户流统计信息
type AccountStreamStats struct {
	TotalStreams    int         `json:"total_streams"`
	TotalMessages   uint64      `json:"total_messages"`
	TotalBytesValue float64     `json:"total_bytes_value"` // 字节数值
	TotalBytesUnit  StorageUnit `json:"total_bytes_unit"`  // 字节数单位
	TotalConsumers  int         `json:"total_consumers"`
	ActiveStreams   int         `json:"active_streams"`
	InactiveStreams int         `json:"inactive_streams"`
	ErrorStreams    int         `json:"error_streams"`
}

// BatchStats 批量请求统计
type BatchStats struct {
	TotalRequested int `json:"total_requested"`
	Successful     int `json:"successful"`
	Failed         int `json:"failed"`
	Skipped        int `json:"skipped"`
}
