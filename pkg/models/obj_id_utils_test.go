package models

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestGenerateUniqueID_Basic tests basic functionality of generateUniqueID
// 测试generateUniqueID的基本功能
func TestGenerateUniqueID_Basic(t *testing.T) {
	t.Run("should generate non-empty ID", func(t *testing.T) {
		id := generateUniqueID()

		// 验证返回的ID不为空
		if id == "" {
			t.Errorf("generateUniqueID() returned empty string")
		}

		// 验证返回的是字符串类型
		if len(id) == 0 {
			t.Errorf("generated ID should not be empty")
		}
	})
}

// TestGenerateUniqueID_Uniqueness tests uniqueness of generated IDs
// 测试生成ID的唯一性
func TestGenerateUniqueID_Uniqueness(t *testing.T) {
	t.Run("should generate unique IDs in sequence", func(t *testing.T) {
		ids := make(map[string]bool)
		const count = 1000

		// 生成多个ID并验证唯一性
		for i := 0; i < count; i++ {
			id := generateUniqueID()

			// 检查是否已存在相同的ID
			if ids[id] {
				t.Errorf("duplicate ID generated: %s", id)
			}

			ids[id] = true
		}

		// 验证生成了预期数量的唯一ID
		if len(ids) != count {
			t.Errorf("expected %d unique IDs, got %d", count, len(ids))
		}
	})
}

// TestGenerateUniqueID_Concurrent tests concurrent safety of generateUniqueID
// 测试generateUniqueID的并发安全性
func TestGenerateUniqueID_Concurrent(t *testing.T) {
	t.Run("should be thread-safe under concurrent access", func(t *testing.T) {
		const goroutines = 100
		const idsPerGoroutine = 100

		var wg sync.WaitGroup
		ids := make(chan string, goroutines*idsPerGoroutine)

		// 启动多个goroutine并发生成ID
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < idsPerGoroutine; j++ {
					ids <- generateUniqueID()
				}
			}()
		}

		// 等待所有goroutine完成
		go func() {
			wg.Wait()
			close(ids)
		}()

		// 收集所有生成的ID并验证唯一性
		idMap := make(map[string]bool)
		for id := range ids {
			if idMap[id] {
				t.Errorf("duplicate ID found in concurrent test: %s", id)
			}
			idMap[id] = true
		}

		expectedCount := goroutines * idsPerGoroutine
		if len(idMap) != expectedCount {
			t.Errorf("expected %d unique IDs, got %d", expectedCount, len(idMap))
		}
	})
}

// TestGenerateUniqueID_Format tests format of generated IDs
// 测试生成ID的格式
func TestGenerateUniqueID_Format(t *testing.T) {
	t.Run("should generate ID with valid format", func(t *testing.T) {
		id := generateUniqueID()

		// 验证ID长度合理（假设UUID-like格式）
		if len(id) < 10 {
			t.Errorf("generated ID seems too short: %s", id)
		}

		// 验证ID不包含非法字符（基础验证）
		if strings.ContainsAny(id, " \t\n\r") {
			t.Errorf("generated ID contains whitespace characters: %s", id)
		}

		// 验证ID只包含字母数字和常见分隔符
		for _, char := range id {
			if !((char >= 'a' && char <= 'z') ||
				(char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') ||
				char == '-' || char == '_') {
				t.Errorf("generated ID contains invalid character '%c': %s", char, id)
			}
		}
	})
}

// TestGenerateUniqueID_Performance tests performance of generateUniqueID
// 测试generateUniqueID的性能
func TestGenerateUniqueID_Performance(t *testing.T) {
	t.Run("should generate IDs efficiently", func(t *testing.T) {
		const count = 10000
		startTime := time.Now()

		// 批量生成ID测试性能
		for i := 0; i < count; i++ {
			_ = generateUniqueID()
		}

		duration := time.Since(startTime)
		avgTime := duration / time.Duration(count)

		// 验证平均生成时间在合理范围内（假设1毫秒内）
		if avgTime > time.Millisecond {
			t.Logf("Warning: average generation time is %v, which might be slow", avgTime)
		}

		t.Logf("Generated %d IDs in %v (average: %v per ID)", count, duration, avgTime)
	})
}

// BenchmarkGenerateUniqueID benchmarks the generateUniqueID function
// 基准测试generateUniqueID函数性能
func BenchmarkGenerateUniqueID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = generateUniqueID()
		//b.Logf("Generated ID: %s", id)
	}
}
