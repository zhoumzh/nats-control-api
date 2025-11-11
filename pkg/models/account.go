package models

// Account represents a NATS account
type Account struct {
	ID                string         `json:"id" gorm:"primaryKey;type:varchar(18)"`
	Name              string         `json:"name" gorm:"uniqueIndex;not null;type:varchar(255)" binding:"required"`
	PublicKey         string         `json:"public_key" gorm:"uniqueIndex;not null;type:varchar(255)"`
	NKey              string         `json:"n_key,omitempty" gorm:"not null;type:text"`
	Status            AccountStatus  `json:"status" gorm:"not null;type:varchar(50);default:'active'"`
	Description       string         `json:"description" gorm:"type:text"`
	Limits            *AccountLimits `json:"limits,omitempty" gorm:"type:text"`
	JWTClaims         *JSONMap       `json:"jwt_claims,omitempty" gorm:"type:text"`
	JWTText           *string        `json:"jwt_text,omitempty" gorm:"type:text"` // 存储实际的JWT密文
	CredsFile         *string        `json:"creds_file,omitempty" gorm:"type:text"`
	// 新增字段
	IsSystemAccount   bool           `json:"is_system_account" gorm:"not null;default:false"` // 是否为系统账户
	OriginClusterID   string         `json:"origin_cluster_id" gorm:"not null;type:varchar(18);index"` // 创建来源集群ID
	CreatedAt         CustomTime     `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt         CustomTime     `json:"updated_at" gorm:"autoUpdateTime"`
}

type AccountStatus string

const (
	AccountStatusActive   AccountStatus = "active"
	AccountStatusDisabled AccountStatus = "disabled"
)

type AccountLimits struct {
	MaxConnections   int64 `json:"max_connections,omitempty"`
	MaxLeafNodes     int64 `json:"max_leaf_nodes,omitempty"`
	MaxPayloadValue  float64 `json:"max_payload_value,omitempty"` // 数值
	MaxPayloadUnit   StorageUnit `json:"max_payload_unit,omitempty"` // 单位
	MaxDataValue     float64 `json:"max_data_value,omitempty"`    // 数值
	MaxDataUnit      StorageUnit `json:"max_data_unit,omitempty"`     // 单位
	MaxSubscriptions int64 `json:"max_subscriptions,omitempty"`
	// JetStream limits
	JetStreamLimits *JetStreamAccountLimits `json:"jetstream_limits,omitempty"`
	// Import/Export configuration
	Imports []AccountImport `json:"imports,omitempty"`
	Exports []AccountExport `json:"exports,omitempty"`
	// Default permissions for all users in this account
	DefaultPermissions *UserPermissions `json:"default_permissions,omitempty"`
}

// JetStream account-level limits
type JetStreamAccountLimits struct {
	// Core JetStream settings
	Enabled                   bool        `json:"enabled"`                              // Enable JetStream for this account
	MemoryStorageValue        float64     `json:"memory_storage_value,omitempty"`       // 数值
	MemoryStorageUnit         StorageUnit `json:"memory_storage_unit,omitempty"`        // 单位
	DiskStorageValue          float64     `json:"disk_storage_value,omitempty"`         // 数值
	DiskStorageUnit           StorageUnit `json:"disk_storage_unit,omitempty"`          // 单位
	Streams                   int64       `json:"streams,omitempty"`                    // max number of streams
	Consumers                 int64       `json:"consumers,omitempty"`                  // max number of consumers
	MaxAckPending             int64       `json:"max_ack_pending,omitempty"`            // max ack pending
	MemoryMaxStreamBytesValue float64     `json:"memory_max_stream_bytes_value,omitempty"` // 数值
	MemoryMaxStreamBytesUnit  StorageUnit `json:"memory_max_stream_bytes_unit,omitempty"`  // 单位
	DiskMaxStreamBytesValue   float64     `json:"disk_max_stream_bytes_value,omitempty"`   // 数值
	DiskMaxStreamBytesUnit    StorageUnit `json:"disk_max_stream_bytes_unit,omitempty"`    // 单位
	MaxBytesRequired          bool        `json:"max_bytes_required,omitempty"`         // require max_bytes to be set
}

// Account import configuration
type AccountImport struct {
	Name    string     `json:"name"`            // import name
	Subject string     `json:"subject"`         // subject to import
	Account string     `json:"account"`         // account public key (resolved from account ID on creation)
	Token   string     `json:"token,omitempty"` // activation token
	To      string     `json:"to,omitempty"`    // local subject mapping
	Type    ImportType `json:"type"`            // stream or service
}

// Account export configuration
type AccountExport struct {
	Name                 string      `json:"name"`                             // export name
	Subject              string      `json:"subject"`                          // subject to export
	Type                 ExportType  `json:"type"`                             // stream or service
	TokenReq             bool        `json:"token_req,omitempty"`              // require activation token
	Revocations          []string    `json:"revocations,omitempty"`            // revoked public keys
	ResponseType         string      `json:"response_type,omitempty"`          // response type for services
	AccountTokenPosition uint        `json:"account_token_position,omitempty"` // position of account in subject
	Info                 *ExportInfo `json:"info,omitempty"`                   // additional export info
}

type ExportInfo struct {
	Description string `json:"description,omitempty"`
	InfoURL     string `json:"info_url,omitempty"`
}

type ImportType string
type ExportType string

const (
	ImportTypeStream  ImportType = "stream"
	ImportTypeService ImportType = "service"

	ExportTypeStream  ExportType = "stream"
	ExportTypeService ExportType = "service"
)

// Request/Response DTOs for APIs
type CreateAccountRequest struct {
	Name            string         `json:"name" binding:"required"`
	Description     string         `json:"description"`
	OriginClusterID string         `json:"origin_cluster_id" binding:"required"` // 创建来源集群ID（必填）
	Limits          *AccountLimits `json:"limits,omitempty"`
}

type UpdateAccountRequest struct {
	Name            string         `json:"name"`
	Description     string         `json:"description"`
	Status          AccountStatus  `json:"status"`
	Limits          *AccountLimits `json:"limits,omitempty"`
	// Note: OriginClusterID is immutable and cannot be updated
}

// 手动同步JWT请求
type ManualSyncJWTRequest struct {
	ClusterIDs []string `json:"cluster_ids" binding:"required"` // 要同步到的集群ID列表
}

// JWT response structures
type JWTResponse struct {
	RawToken string      `json:"raw_token"`
	Claims   interface{} `json:"claims"`
}

// Export/Import request DTOs
type AddExportRequest struct {
	Name                 string     `json:"name" binding:"required"`          // export name
	Subject              string     `json:"subject" binding:"required"`       // subject to export
	Type                 ExportType `json:"type" binding:"required"`          // stream or service
	TokenReq             bool       `json:"token_req,omitempty"`              // require activation token
	ResponseType         string     `json:"response_type,omitempty"`          // response type for services
	AccountTokenPosition uint       `json:"account_token_position,omitempty"` // position of account in subject
	Description          string     `json:"description,omitempty"`            // export description
	InfoURL              string     `json:"info_url,omitempty"`               // additional info URL
}

type AddImportRequest struct {
	Name    string     `json:"name" binding:"required"`    // import name
	Subject string     `json:"subject" binding:"required"` // subject to import
	Account string     `json:"account" binding:"required"` // account ID to import from
	Token   string     `json:"token,omitempty"`            // activation token
	To      string     `json:"to,omitempty"`               // local subject mapping
	Type    ImportType `json:"type" binding:"required"`    // stream or service
}

// Account association request for simplified cross-account setup
type AccountAssociationRequest struct {
	TargetAccountID string                      `json:"target_account_id" binding:"required"` // target account to associate with
	Subjects        []AccountAssociationSubject `json:"subjects" binding:"required"`          // subjects to share
	Description     string                      `json:"description,omitempty"`                // association description
}

type AccountAssociationSubject struct {
	BaseSubject string     `json:"base_subject" binding:"required"` // base subject (e.g., "orders", "events")
	Pattern     string     `json:"pattern" binding:"required"`      // pattern: ".*" or ".>"
	Type        ImportType `json:"type" binding:"required"`         // stream or service
	Description string     `json:"description,omitempty"`           // subject description
}

// AccountListRequest for pagination and filtering
type AccountListRequest struct {
	Limit          int    `json:"limit,omitempty"`
	Offset         int    `json:"offset,omitempty"`
	Filter         string `json:"filter,omitempty"`
	Search         string `json:"search,omitempty"`
	Status         string `json:"status,omitempty"`
	AccountType    string `json:"account_type,omitempty"`
	ClusterID      string `json:"cluster_id,omitempty"`
	SortBy         string `json:"sort_by,omitempty"`
	Order          string `json:"order,omitempty"`
}

// AccountListResponse for paginated account responses
type AccountListResponse struct {
	Data  []*Account `json:"data"`
	Total int64      `json:"total"`
	Limit int        `json:"limit"`
	Offset int       `json:"offset"`
}

type AccountWithCluster struct {
	*Account
	ClusterName string `json:"cluster_name"`
}