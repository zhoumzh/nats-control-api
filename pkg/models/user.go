package models

// User represents a NATS user within an account
type User struct {
	ID          string           `json:"id" gorm:"primaryKey;type:varchar(18)"`
	AccountID   string           `json:"account_id" gorm:"not null;type:varchar(18);index" binding:"required"`
	Name        string           `json:"name" gorm:"not null;type:varchar(255)" binding:"required"`
	PublicKey   string           `json:"public_key" gorm:"uniqueIndex;not null;type:varchar(255)"`
	NKey        string           `json:"n_key,omitempty" gorm:"not null;type:text"`
	Status      UserStatus       `json:"status" gorm:"not null;type:varchar(50);default:'active'"`
	IsAdmin     bool             `json:"is_admin" gorm:"not null;default:false"`
	Description string           `json:"description" gorm:"type:text"`
	Permissions *UserPermissions `json:"permissions,omitempty" gorm:"type:text"`
	Limits      *UserLimits      `json:"limits,omitempty" gorm:"type:text"`
	JWTClaims   *JSONMap         `json:"jwt_claims,omitempty" gorm:"type:text"`
	JWTText     *string          `json:"jwt_text,omitempty" gorm:"type:text"` // 存储实际的JWT密文
	CredsFile   *string          `json:"creds_file,omitempty" gorm:"type:text"`
	CreatedAt   CustomTime       `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt   CustomTime       `json:"updated_at" gorm:"autoUpdateTime"`
}

type UserStatus string

const (
	UserStatusActive   UserStatus = "active"
	UserStatusDisabled UserStatus = "disabled"
)

type UserPermissions struct {
	Publish   *PermissionRules     `json:"publish,omitempty"`
	Subscribe *PermissionRules     `json:"subscribe,omitempty"`
	Response  *ResponsePermissions `json:"response,omitempty"`
	// JetStream permissions
	JetStream *JetStreamPermissions `json:"jetstream,omitempty"`
}

type PermissionRules struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

type ResponsePermissions struct {
	MaxMessages int `json:"max_messages,omitempty"`
	Expires     int `json:"expires,omitempty"` // Duration in seconds
}

type UserLimits struct {
	MaxPayloadValue     float64     `json:"max_payload_value,omitempty"`     // 数值
	MaxPayloadUnit      StorageUnit `json:"max_payload_unit,omitempty"`      // 单位
	MaxDataValue        float64     `json:"max_data_value,omitempty"`        // 数值
	MaxDataUnit         StorageUnit `json:"max_data_unit,omitempty"`         // 单位
	MaxSubscriptions    int64       `json:"max_subscriptions,omitempty"`
	// Access control limits (optional, defaults allow all)
	AccessControls      *AccessControls `json:"access_controls,omitempty"`
	// JetStream user limits
	JetStreamLimits     *JetStreamUserLimits `json:"jetstream_limits,omitempty"`
	// Connection controls
	ConnectionTypes     []string    `json:"connection_types,omitempty"` // allowed connection types
}

// JetStream user permissions
type JetStreamPermissions struct {
	Publish   *JetStreamPublishPermissions   `json:"publish,omitempty"`
	Subscribe *JetStreamSubscribePermissions `json:"subscribe,omitempty"`
}

type JetStreamPublishPermissions struct {
	Allow []string `json:"allow,omitempty"` // allowed stream subjects
	Deny  []string `json:"deny,omitempty"`  // denied stream subjects
}

type JetStreamSubscribePermissions struct {
	Allow []string `json:"allow,omitempty"` // allowed stream subjects
	Deny  []string `json:"deny,omitempty"`  // denied stream subjects
}

// JetStream user-level limits
type JetStreamUserLimits struct {
	MemoryStorageValue float64     `json:"memory_storage_value,omitempty"` // 数值
	MemoryStorageUnit  StorageUnit `json:"memory_storage_unit,omitempty"`  // 单位
	DiskStorageValue   float64     `json:"disk_storage_value,omitempty"`   // 数值
	DiskStorageUnit    StorageUnit `json:"disk_storage_unit,omitempty"`    // 单位
	Streams            int64       `json:"streams,omitempty"`              // max streams user can create
	Consumers          int64       `json:"consumers,omitempty"`            // max consumers user can create
	MaxAckPending      int64       `json:"max_ack_pending,omitempty"`      // max ack pending
}

// Access controls for IP/time restrictions (optional)
type AccessControls struct {
	// IP restrictions (CIDR format), empty means no restriction
	SourceIPs []string `json:"source_ips,omitempty"`
	// Time-based access (optional), empty means no restriction
	TimeRestrictions *TimeRestrictions `json:"time_restrictions,omitempty"`
}

// Time-based access controls (optional)
type TimeRestrictions struct {
	// Start time in RFC3339 format, empty means no start restriction
	Start string `json:"start,omitempty"`
	// End time in RFC3339 format, empty means no end restriction
	End string `json:"end,omitempty"`
	// Timezone for time calculations, defaults to UTC
	Timezone string `json:"timezone,omitempty"`
	// Days of week (0=Sunday, 6=Saturday), empty means all days
	DaysOfWeek []int `json:"days_of_week,omitempty"`
	// Hours of day (0-23), empty means all hours
	HoursOfDay []int `json:"hours_of_day,omitempty"`
}

type CreateUserRequest struct {
	Name        string           `json:"name" binding:"required"`
	Description string           `json:"description"`
	IsAdmin     bool             `json:"is_admin"`
	Role        string           `json:"role,omitempty"`       // 用户角色
	Department  string           `json:"department,omitempty"` // 部门
	Project     string           `json:"project,omitempty"`    // 项目
	Permissions *UserPermissions `json:"permissions,omitempty"`
	Limits      *UserLimits      `json:"limits,omitempty"`
}

type UpdateUserRequest struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Status      UserStatus       `json:"status"`
	IsAdmin     *bool            `json:"is_admin,omitempty"`   // 管理员权限，使用指针类型以支持可选更新
	Role        string           `json:"role,omitempty"`       // 用户角色
	Department  string           `json:"department,omitempty"` // 部门
	Project     string           `json:"project,omitempty"`    // 项目
	Permissions *UserPermissions `json:"permissions,omitempty"`
	Limits      *UserLimits      `json:"limits,omitempty"`
}

// SimplifiedUserResponse 简化的用户响应结构，仅包含基本信息
type SimplifiedUserResponse struct {
	ID        string `json:"id"`         // 用户ID
	Name      string `json:"name"`       // 用户名
	AccountID string `json:"account_id"` // 所属账号ID
}