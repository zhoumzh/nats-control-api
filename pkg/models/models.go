package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// 时间格式常量 - Go使用参考时间2006-01-02 15:04:05来定义格式
const TimeFormat = "2006-01-02 15:04:05"

// CustomTime 自定义时间类型，用于统一JSON序列化格式
type CustomTime struct {
	time.Time
}

// MarshalJSON 自定义JSON序列化
func (ct CustomTime) MarshalJSON() ([]byte, error) {
	formatted := ct.Time.Format(TimeFormat)
	return json.Marshal(formatted)
}

// UnmarshalJSON 自定义JSON反序列化
func (ct *CustomTime) UnmarshalJSON(data []byte) error {
	var timeStr string
	if err := json.Unmarshal(data, &timeStr); err != nil {
		return err
	}

	// 尝试解析不同的时间格式
	formats := []string{
		TimeFormat,
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			ct.Time = t
			return nil
		}
	}

	return fmt.Errorf("无法解析时间格式: %s", timeStr)
}

// Scan implements sql.Scanner interface
func (ct *CustomTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		ct.Time = v
		return nil
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return err
		}
		ct.Time = t
		return nil
	case nil:
		ct.Time = time.Time{}
		return nil
	default:
		return fmt.Errorf("cannot scan %T into CustomTime", value)
	}
}

// Value implements driver.Valuer interface
func (ct CustomTime) Value() (driver.Value, error) {
	return ct.Time, nil
}

// String returns the formatted time string
func (ct CustomTime) String() string {
	return ct.Time.Format(TimeFormat)
}

// NewCustomTime creates a CustomTime from time.Time
func NewCustomTime(t time.Time) CustomTime {
	return CustomTime{Time: t}
}
