package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type ConsumerType string

const (
	ConsumerTypePull ConsumerType = "pull"
	ConsumerTypePush ConsumerType = "push"
)

type ConsumerSyncStatus string

const (
	ConsumerSyncPending ConsumerSyncStatus = "pending"
	ConsumerSyncSyncing ConsumerSyncStatus = "syncing"
	ConsumerSyncSynced  ConsumerSyncStatus = "synced"
	ConsumerSyncFailed  ConsumerSyncStatus = "failed"
)

type ConsumerDeliverPolicy string

const (
	DeliverAll            ConsumerDeliverPolicy = "all"
	DeliverLast           ConsumerDeliverPolicy = "last"
	DeliverNew            ConsumerDeliverPolicy = "new"
	DeliverByStartSeq     ConsumerDeliverPolicy = "by_start_sequence"
	DeliverByStartTime    ConsumerDeliverPolicy = "by_start_time"
	DeliverLastPerSubject ConsumerDeliverPolicy = "last_per_subject"
)

type ConsumerAckPolicy string

const (
	AckExplicit ConsumerAckPolicy = "explicit"
	AckAll      ConsumerAckPolicy = "all"
	AckNone     ConsumerAckPolicy = "none"
)

type ConsumerRetryPolicy string

const (
	RetryExponential ConsumerRetryPolicy = "exponential"
	RetryUniform     ConsumerRetryPolicy = "uniform"
)

type ConsumerReplayPolicy string

const (
	ReplayInstant  ConsumerReplayPolicy = "instant"
	ReplayOriginal ConsumerReplayPolicy = "original"
)

type ConsumerPriorityPolicy string

const (
	PriorityNone   ConsumerPriorityPolicy = ""
	PriorityPinned ConsumerPriorityPolicy = "pinned"
)

type DurationSlice []int64

func (d *DurationSlice) Scan(value interface{}) error {
	if value == nil {
		*d = make(DurationSlice, 0)
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into DurationSlice", value)
	}

	if len(bytes) == 0 {
		*d = make(DurationSlice, 0)
		return nil
	}

	return json.Unmarshal(bytes, d)
}

func (d DurationSlice) Value() (driver.Value, error) {
	if len(d) == 0 {
		return "[]", nil
	}
	return json.Marshal(d)
}

type Consumer struct {
	ID                 string                 `json:"id" gorm:"primaryKey;type:varchar(18)"`
	Name               string                 `json:"name" gorm:"type:varchar(255);not null;index"`
	Durable            string                 `json:"durable" gorm:"type:varchar(255);index"`
	Description        string                 `json:"description" gorm:"type:text"`
	JetStreamID        string                 `json:"jetstream_id" gorm:"column:jetstream_id;type:varchar(18);not null;index"`
	NatsOperateUserID  string                 `json:"nats_operate_user_id" gorm:"type:varchar(18);not null;index"`
	SyncStatus         ConsumerSyncStatus     `json:"sync_status" gorm:"type:varchar(20);default:'pending';index"`
	SyncMessage        string                 `json:"sync_message" gorm:"type:text"`
	Status             string                 `json:"status" gorm:"type:varchar(20);default:'active'"`
	Mode               string                 `json:"mode" gorm:"type:varchar(10);not null;index"`
	ConsumerType       ConsumerType           `json:"consumer_type" gorm:"type:varchar(10);not null;index"`
	DeliverPolicy      ConsumerDeliverPolicy  `json:"deliver_policy" gorm:"type:varchar(30);default:'all'"`
	StartSeq           uint64                 `json:"start_seq" gorm:"default:0"`
	StartTime          *time.Time             `json:"start_time"`
	OptStartSeq        uint64                 `json:"opt_start_seq" gorm:"default:0"`
	OptStartTime       *time.Time             `json:"opt_start_time"`
	AckPolicy          ConsumerAckPolicy      `json:"ack_policy" gorm:"type:varchar(20);default:'explicit'"`
	AckWait            int64                  `json:"ack_wait" gorm:"type:bigint;default:30"`
	MaxDeliver         int                    `json:"max_deliver" gorm:"default:-1"`
	BackOff            DurationSlice          `json:"backoff" gorm:"type:json"`
	RetryPolicy        ConsumerRetryPolicy    `json:"retry_policy" gorm:"type:varchar(20)"`
	FilterSubject      string                 `json:"filter_subject" gorm:"type:varchar(255)"`
	FilterSubjects     StringSlice            `json:"filter_subjects" gorm:"type:json"`
	ReplayPolicy       ConsumerReplayPolicy   `json:"replay_policy" gorm:"type:varchar(20);default:'instant'"`
	RateLimitBps       uint64                 `json:"rate_limit_bps" gorm:"default:0"`
	RateLimit          uint64                 `json:"rate_limit" gorm:"default:0"`
	SampleFrequency    string                 `json:"sample_frequency" gorm:"type:varchar(20)"`
	MaxWaiting         int                    `json:"max_waiting"`
	MaxAckPending      int                    `json:"max_ack_pending" gorm:"default:1000"`
	MaxBatch           int                    `json:"max_batch" gorm:"default:0"`
	MaxBytes           int                    `json:"max_bytes" gorm:"default:0"`
	MaxExpires         int64                  `json:"max_expires" gorm:"type:bigint;default:0"`
	MaxRequestBatch    int                    `json:"max_request_batch" gorm:"default:0"`
	MaxRequestExpires  int64                  `json:"max_request_expires" gorm:"type:bigint;default:0"`
	MaxRequestMaxBytes int                    `json:"max_request_max_bytes" gorm:"default:0"`
	DeliverSubject     string                 `json:"deliver_subject" gorm:"type:varchar(255)"`
	DeliverGroup       string                 `json:"deliver_group" gorm:"type:varchar(255)"`
	FlowControl        bool                   `json:"flow_control" gorm:"default:false"`
	IdleHeartbeat      int64                  `json:"idle_heartbeat" gorm:"type:bigint;default:0"`
	HeadersOnly        bool                   `json:"headers_only" gorm:"default:false"`
	InactiveThreshold  int64                  `json:"inactive_threshold" gorm:"type:bigint;default:0"`
	Replicas           int                    `json:"replicas" gorm:"default:0"`
	MemoryStorage      bool                   `json:"memory_storage" gorm:"default:false"`
	Metadata           MetadataMap            `json:"metadata" gorm:"type:json"`
	PauseUntil         *time.Time             `json:"pause_until"`
	PriorityPolicy     ConsumerPriorityPolicy `json:"priority_policy" gorm:"type:varchar(20)"`
	PinnedTTL          int64                  `json:"pinned_ttl" gorm:"type:bigint;default:0"`
	PriorityGroups     StringSlice            `json:"priority_groups" gorm:"type:json"`
	CreatedAt          CustomTime             `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt          CustomTime             `json:"updated_at" gorm:"autoUpdateTime"`
	JetStream          *JetStream             `json:"jetstream,omitempty" gorm:"-"`
	NatsOperateUser    *User                  `json:"nats_operate_user,omitempty" gorm:"-"`
	Stats              *ConsumerStats         `json:"stats,omitempty" gorm:"-"`
}

func (Consumer) TableName() string {
	return "consumers"
}

type ConsumerStats struct {
	StreamName      string       `json:"stream_name"`
	ConsumerName    string       `json:"consumer_name"`
	Created         CustomTime   `json:"created"`
	DeliveredSeq    uint64       `json:"delivered_seq"`
	DeliveredStream uint64       `json:"delivered_stream"`
	DeliveredTime   *CustomTime  `json:"delivered_time,omitempty"`
	AckFloorSeq     uint64       `json:"ack_floor_seq"`
	AckFloorStream  uint64       `json:"ack_floor_stream"`
	NumAckPending   int          `json:"num_ack_pending"`
	NumRedelivered  int          `json:"num_redelivered"`
	NumWaiting      int          `json:"num_waiting"`
	NumPending      uint64       `json:"num_pending"`
	PushBound       bool         `json:"push_bound,omitempty"`
	Cluster         *ClusterInfo `json:"cluster,omitempty"`
	Paused          bool         `json:"paused"`
	PauseRemaining  int64        `json:"pause_remaining,omitempty" swaggertype:"integer" example:"0"`
	Timestamp       CustomTime   `json:"timestamp"`
}

type SequenceInfo struct {
	ConsumerSeq uint64      `json:"consumer_seq"`
	StreamSeq   uint64      `json:"stream_seq"`
	LastActive  *CustomTime `json:"last_active,omitempty"`
}

type ClusterInfo struct {
	Name     string   `json:"name,omitempty"`
	Leader   string   `json:"leader,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
}

type CreateConsumerRequest struct {
	ClusterID          string                `json:"cluster_id"`
	StreamName         string                `json:"stream_name" binding:"required"`
	ConsumerName       string                `json:"consumer_name" binding:"required"`
	Mode               string                `json:"mode" binding:"required,oneof=push pull"`
	AckPolicy          ConsumerAckPolicy     `json:"ack_policy" binding:"required"`
	Name               string                `json:"name"`
	Durable            string                `json:"durable"`
	Description        string                `json:"description"`
	NatsOperateUserID  string                `json:"nats_operate_user_id"`
	ConsumerType       ConsumerType          `json:"consumer_type"`
	DeliverPolicy      ConsumerDeliverPolicy `json:"deliver_policy"`
	StartSeq           *uint64               `json:"start_seq"`
	StartTime          *time.Time            `json:"start_time"`
	OptStartSeq        uint64                `json:"opt_start_seq"`
	OptStartTime       *time.Time            `json:"opt_start_time"`
	AckWait            string                `json:"ack_wait"`
	MaxDeliver         *int                  `json:"max_deliver"`
	BackOff            []string              `json:"backoff"`
	RetryPolicy        ConsumerRetryPolicy   `json:"retry_policy"`
	FilterSubject      string                `json:"filter_subject"`
	FilterSubjects     []string              `json:"filter_subjects"`
	ReplayPolicy       ConsumerReplayPolicy  `json:"replay_policy"`
	RateLimitBps       *uint64               `json:"rate_limit_bps"`
	RateLimit          uint64                `json:"rate_limit"`
	SampleFrequency    string                `json:"sample_freq"`
	MaxWaiting         int                   `json:"max_waiting"`
	MaxAckPending      int                   `json:"max_ack_pending"`
	MaxBatch           *int                  `json:"max_batch"`
	MaxBytes           *int                  `json:"max_bytes"`
	MaxExpires         string                `json:"max_expires"`
	MaxRequestBatch    int                   `json:"max_request_batch"`
	MaxRequestExpires  int64                 `json:"max_request_expires"`
	MaxRequestMaxBytes int                   `json:"max_request_max_bytes"`
	DeliverSubject     string                `json:"deliver_subject"`
	DeliverGroup       string                `json:"deliver_group"`
	FlowControl        bool                  `json:"flow_control"`
	IdleHeartbeat      string                `json:"idle_heartbeat"`
	HeadersOnly        bool                  `json:"headers_only"`
	InactiveThreshold  int64                 `json:"inactive_threshold"`
	Replicas           int                   `json:"replicas"`
	MemoryStorage      bool                  `json:"memory_storage"`
	Metadata           map[string]string     `json:"metadata"`
}

func (req *CreateConsumerRequest) Validate() error {
	if req.ConsumerName == "" {
		return NewValidationError("consumer_name", "consumer_name is required")
	}

	if req.StreamName == "" {
		return NewValidationError("stream_name", "stream_name is required")
	}

	if req.Mode == "" {
		return NewValidationError("mode", "mode is required")
	}

	if req.Mode != "push" && req.Mode != "pull" {
		return NewValidationError("mode", "mode must be 'push' or 'pull'")
	}

	if req.Mode == "push" {
		if req.DeliverSubject == "" {
			return NewValidationError("deliver_subject", "deliver_subject is required for push mode")
		}

		if req.MaxBatch != nil {
			return NewValidationError("max_batch", "max_batch is only applicable to pull mode")
		}
		if req.MaxBytes != nil {
			return NewValidationError("max_bytes", "max_bytes is only applicable to pull mode")
		}
		if req.MaxExpires != "" {
			return NewValidationError("max_expires", "max_expires is only applicable to pull mode")
		}
	}

	if req.Mode == "pull" {
		if req.DeliverSubject != "" {
			return NewValidationError("deliver_subject", "deliver_subject is only applicable to push mode")
		}
		if req.DeliverGroup != "" {
			return NewValidationError("deliver_group", "deliver_group is only applicable to push mode")
		}
		if req.FlowControl {
			return NewValidationError("flow_control", "flow_control is only applicable to push mode")
		}
		if req.IdleHeartbeat != "" {
			return NewValidationError("idle_heartbeat", "idle_heartbeat is only applicable to push mode")
		}
		if req.RateLimitBps != nil {
			return NewValidationError("rate_limit_bps", "rate_limit_bps is only applicable to push mode")
		}
	}

	if req.DeliverPolicy == DeliverByStartSeq && (req.StartSeq == nil || *req.StartSeq == 0) {
		return NewValidationError("start_seq", "start_seq is required when deliver_policy is 'by_start_sequence'")
	}

	if req.DeliverPolicy == DeliverByStartTime && req.StartTime == nil {
		return NewValidationError("start_time", "start_time is required when deliver_policy is 'by_start_time'")
	}

	if req.FilterSubject != "" && len(req.FilterSubjects) > 0 {
		return NewValidationError("filter_subject", "filter_subject and filter_subjects are mutually exclusive")
	}

	if req.DeliverPolicy == DeliverLastPerSubject {
		if req.FilterSubject == "" && len(req.FilterSubjects) == 0 {
			return NewValidationError("filter_subject", "filter_subject or filter_subjects is required when deliver_policy is 'last_per_subject'")
		}
	}

	if req.AckPolicy == AckNone {
		if req.MaxDeliver != nil && *req.MaxDeliver > 0 {
			return NewValidationError("max_deliver", "max_deliver cannot be set when ack_policy is 'AckNone'")
		}
		if len(req.BackOff) > 0 {
			return NewValidationError("backoff", "backoff cannot be set when ack_policy is 'AckNone'")
		}
	}

	if req.FlowControl && req.IdleHeartbeat == "" {
		return NewValidationError("idle_heartbeat", "idle_heartbeat is required when flow_control is enabled")
	}

	if req.Replicas < 0 {
		return NewValidationError("replicas", "replicas cannot be negative")
	}

	return nil
}

type UpdateConsumerRequest struct {
	StreamName        string                `json:"stream_name"`
	ConsumerName      string                `json:"consumer_name"`
	Mode              string                `json:"mode"`
	Name              string                `json:"name"`
	Durable           string                `json:"durable"`
	Description       string                `json:"description"`
	JetStreamID       string                `json:"jetstream_id"`
	ConsumerType      ConsumerType          `json:"consumer_type"`
	DeliverPolicy     ConsumerDeliverPolicy `json:"deliver_policy"`
	AckPolicy         ConsumerAckPolicy     `json:"ack_policy"`
	AckWait           *string               `json:"ack_wait"`
	MaxDeliver        *int                  `json:"max_deliver"`
	BackOff           []string              `json:"backoff"`
	RetryPolicy       ConsumerRetryPolicy   `json:"retry_policy"`
	ReplayPolicy      ConsumerReplayPolicy  `json:"replay_policy"`
	RateLimitBps      *uint64               `json:"rate_limit_bps"`
	RateLimit         *uint64               `json:"rate_limit"`
	FilterSubject     string                `json:"filter_subject"`
	FilterSubjects    []string              `json:"filter_subjects"`
	SampleFrequency   string                `json:"sample_freq"`
	MaxWaiting        *int                  `json:"max_waiting"`
	MaxAckPending     *int                  `json:"max_ack_pending"`
	MaxBatch          *int                  `json:"max_batch"`
	MaxBytes          *int                  `json:"max_bytes"`
	MaxExpires        *string               `json:"max_expires"`
	DeliverSubject    string                `json:"deliver_subject"`
	DeliverGroup      *string               `json:"deliver_group"`
	FlowControl       *bool                 `json:"flow_control"`
	IdleHeartbeat     *string               `json:"idle_heartbeat"`
	HeadersOnly       *bool                 `json:"headers_only"`
	InactiveThreshold *int64                `json:"inactive_threshold"`
	Replicas          *int                  `json:"replicas"`
	Metadata          map[string]string     `json:"metadata"`
}

func (req *UpdateConsumerRequest) Validate(mode string) error {
	if mode == "push" {
		if req.MaxWaiting != nil && *req.MaxWaiting > 0 {
			return NewValidationError("max_waiting", "max_waiting is only applicable to pull mode")
		}
		if req.MaxBatch != nil {
			return NewValidationError("max_batch", "max_batch is only applicable to pull mode")
		}
		if req.MaxBytes != nil {
			return NewValidationError("max_bytes", "max_bytes is only applicable to pull mode")
		}
		if req.MaxExpires != nil {
			return NewValidationError("max_expires", "max_expires is only applicable to pull mode")
		}
	}

	if mode == "pull" {
		if req.DeliverSubject != "" {
			return NewValidationError("deliver_subject", "deliver_subject is only applicable to push mode")
		}
		if req.DeliverGroup != nil && *req.DeliverGroup != "" {
			return NewValidationError("deliver_group", "deliver_group is only applicable to push mode")
		}
		if req.FlowControl != nil && *req.FlowControl {
			return NewValidationError("flow_control", "flow_control is only applicable to push mode")
		}
		if req.IdleHeartbeat != nil {
			return NewValidationError("idle_heartbeat", "idle_heartbeat is only applicable to push mode")
		}
		if req.RateLimitBps != nil {
			return NewValidationError("rate_limit_bps", "rate_limit_bps is only applicable to push mode")
		}
	}

	if req.FilterSubject != "" && len(req.FilterSubjects) > 0 {
		return NewValidationError("filter_subject", "filter_subject and filter_subjects are mutually exclusive")
	}

	return nil
}

type ConsumerListRequest struct {
	ClusterID         string       `form:"cluster_id"`
	JetStreamID       string       `form:"jetstream_id"`
	NatsOperateUserID string       `form:"nats_operate_user_id"`
	ConsumerType      ConsumerType `form:"consumer_type"`
	Status            string       `form:"status"`
	SyncStatus        string       `form:"sync_status"`
	Search            string       `form:"search"`
	IsDurable         *bool        `form:"is_durable"`
	Page              int          `form:"page"`
	PageSize          int          `form:"page_size"`
}

type ConsumerConfigDiff struct {
	HasDifference  bool                `json:"has_difference"`
	DatabaseConfig *ConsumerConfigView `json:"database_config"`
	ClusterConfig  *ConsumerConfigView `json:"cluster_config"`
	Differences    []ConfigDifference  `json:"differences"`
}

type ConsumerConfigView struct {
	Name              string                `json:"name"`
	Durable           string                `json:"durable,omitempty"`
	Description       string                `json:"description,omitempty"`
	DeliverPolicy     ConsumerDeliverPolicy `json:"deliver_policy"`
	OptStartSeq       uint64                `json:"opt_start_seq,omitempty"`
	OptStartTime      *time.Time            `json:"opt_start_time,omitempty"`
	AckPolicy         ConsumerAckPolicy     `json:"ack_policy"`
	AckWait           int64                 `json:"ack_wait"`
	MaxDeliver        int                   `json:"max_deliver"`
	FilterSubject     string                `json:"filter_subject,omitempty"`
	FilterSubjects    []string              `json:"filter_subjects,omitempty"`
	ReplayPolicy      ConsumerReplayPolicy  `json:"replay_policy"`
	RateLimit         uint64                `json:"rate_limit,omitempty"`
	SampleFrequency   string                `json:"sample_frequency,omitempty"`
	MaxWaiting        int                   `json:"max_waiting,omitempty"`
	MaxAckPending     int                   `json:"max_ack_pending"`
	DeliverSubject    string                `json:"deliver_subject,omitempty"`
	DeliverGroup      string                `json:"deliver_group,omitempty"`
	FlowControl       bool                  `json:"flow_control,omitempty"`
	IdleHeartbeat     int64                 `json:"idle_heartbeat,omitempty"`
	HeadersOnly       bool                  `json:"headers_only,omitempty"`
	InactiveThreshold int64                 `json:"inactive_threshold,omitempty"`
	Replicas          int                   `json:"replicas"`
	MemoryStorage     bool                  `json:"memory_storage,omitempty"`
}
