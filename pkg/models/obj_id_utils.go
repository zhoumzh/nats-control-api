package models

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
)

const (
	ClusterObjType   = "cl"
	AccountObjType   = "ac"
	UserObjType      = "us"
	JetStreamObjType = "js"
	ConsumerObjType  = "cm"
)

func GenID(objType string) string {
	if objType == "" {
		return generateUniqueID()
	}
	return objType + "_" + generateUniqueID()
}

func generateUniqueID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)[:15]
}

// ValidateObjectID 验证对象ID格式是否符合规范
// objType: 对象类型前缀，如 ClusterObjType, AccountObjType 等
// id: 待验证的ID字符串
// 返回: 是否有效
func ValidateObjectID(id string, objType string) bool {
	if id == "" {
		return false
	}

	// 验证特定对象类型的ID格式: objType_15位十六进制字符
	// 例如: cl_abc123def456789, ac_123456789abcdef
	pattern := fmt.Sprintf(`^%s_[0-9a-f]{15}$`, regexp.QuoteMeta(objType))
	matched, _ := regexp.MatchString(pattern, id)
	return matched
}
