package models

// JWTTask represents an async task for JWT operations
type JWTTask struct {
	ID          string      `json:"id" gorm:"primaryKey;type:varchar(36)"`
	Status      TaskStatus  `json:"status" gorm:"not null;type:varchar(50);default:'pending'"`
	EntityType  EntityType  `json:"entity_type" gorm:"not null;type:varchar(50)"`
	EntityID    string      `json:"entity_id" gorm:"not null;type:varchar(36);index"`
	PublicKey   string      `json:"public_key" gorm:"type:varchar(100);index"` // 冗余存储账户公钥，用于快照和追溯
	Operation   Operation   `json:"operation" gorm:"not null;type:varchar(50)"`
	TriggerType TriggerType `json:"trigger_type" gorm:"not null;type:varchar(50);default:'auto'"` // 触发方式：自动或手动
	Error       string      `json:"error,omitempty" gorm:"type:text"`
	Retries     int         `json:"retries" gorm:"not null;default:0"`
	MaxRetries  int         `json:"max_retries" gorm:"not null;default:3"`
	// Multi-cluster support
	ClusterIDs  *JSONMap    `json:"cluster_ids,omitempty" gorm:"type:text"`  // 需要同步的集群ID列表
	SyncResults *JSONMap    `json:"sync_results,omitempty" gorm:"type:text"` // 各集群同步结果
	CreatedAt   CustomTime  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt   CustomTime  `json:"updated_at" gorm:"autoUpdateTime"`
	CompletedAt *CustomTime `json:"completed_at,omitempty"`
}

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
	TaskStatusRetrying   TaskStatus = "retrying"
)

type EntityType string

const (
	EntityTypeAccount EntityType = "account"
	EntityTypeUser    EntityType = "user"
)

type TriggerType string

const (
	TriggerTypeAuto   TriggerType = "auto"   // 自动触发（账户/用户创建/更新时）
	TriggerTypeManual TriggerType = "manual" // 手动触发（用户点击同步按钮）
)

type Operation string

const (
	OperationCreate  Operation = "create"
	OperationUpdate  Operation = "update"
	OperationDisable Operation = "disable"
	OperationEnable  Operation = "enable"
)