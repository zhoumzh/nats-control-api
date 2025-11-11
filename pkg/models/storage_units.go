package models

import (
	"fmt"
)

// StorageUnit 存储单位枚举
type StorageUnit string

const (
	StorageUnitBytes StorageUnit = "B"  // 字节
	StorageUnitKB    StorageUnit = "KB" // 千字节
	StorageUnitMB    StorageUnit = "MB" // 兆字节
	StorageUnitGB    StorageUnit = "GB" // 吉字节
	StorageUnitTB    StorageUnit = "TB" // 太字节
)

// StorageValue 带单位的存储值结构体
type StorageValue struct {
	Value float64     `json:"value" gorm:"type:decimal(15,3)" example:"1024"` // 数值
	Unit  StorageUnit `json:"unit" gorm:"type:varchar(10)" example:"MB"`      // 单位
}

// ConvertToBytes 从单位和数值转换为字节数
func ConvertToBytes(value float64, unit StorageUnit) int64 {
	switch unit {
	case StorageUnitBytes:
		return int64(value)
	case StorageUnitKB:
		return int64(value * 1024)
	case StorageUnitMB:
		return int64(value * 1024 * 1024)
	case StorageUnitGB:
		return int64(value * 1024 * 1024 * 1024)
	case StorageUnitTB:
		return int64(value * 1024 * 1024 * 1024 * 1024)
	default:
		return int64(value) // 默认按字节处理
	}
}

// ConvertFromBytes 从字节数转换为数值和单位，自动选择合适的单位
func ConvertFromBytes(bytes int64) (float64, StorageUnit) {
	if bytes < 0 {
		return float64(bytes), StorageUnitBytes
	}

	var value float64
	var unit StorageUnit

	// 选择合适的单位，保持可读性
	switch {
	case bytes >= 1024*1024*1024*1024: // TB
		value = float64(bytes) / (1024 * 1024 * 1024 * 1024)
		unit = StorageUnitTB
	case bytes >= 1024*1024*1024: // GB
		value = float64(bytes) / (1024 * 1024 * 1024)
		unit = StorageUnitGB
	case bytes >= 1024*1024: // MB
		value = float64(bytes) / (1024 * 1024)
		unit = StorageUnitMB
	case bytes >= 1024: // KB
		value = float64(bytes) / 1024
		unit = StorageUnitKB
	default: // B
		return float64(bytes), StorageUnitBytes
	}

	// 保留两位小数（四舍五入）
	value = float64(int64(value*100+0.5)) / 100
	return value, unit
}

// String 返回存储值的字符串表示
func (sv StorageValue) String() string {
	// 格式化数值，去除不必要的小数点
	if sv.Value == float64(int64(sv.Value)) {
		return fmt.Sprintf("%.0f%s", sv.Value, sv.Unit)
	}
	return fmt.Sprintf("%.2f%s", sv.Value, sv.Unit)
}

// IsValid 验证存储值是否有效
func (sv StorageValue) IsValid() bool {
	if sv.Value < 0 {
		return false
	}

	validUnits := []StorageUnit{
		StorageUnitBytes, StorageUnitKB, StorageUnitMB,
		StorageUnitGB, StorageUnitTB,
	}

	for _, unit := range validUnits {
		if sv.Unit == unit {
			return true
		}
	}
	return false
}
