package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// JSONMap represents a map that can be stored as JSON in database
type JSONMap map[string]interface{}

// Value implements driver.Valuer interface for database storage
func (j JSONMap) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// Scan implements sql.Scanner interface for database retrieval
func (j *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into JSONMap", value)
	}

	return json.Unmarshal(bytes, j)
}

// AccountLimits Value and Scan methods for database storage
func (a AccountLimits) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *AccountLimits) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into AccountLimits", value)
	}
	
	return json.Unmarshal(bytes, a)
}

// UserPermissions Value and Scan methods for database storage
func (u UserPermissions) Value() (driver.Value, error) {
	return json.Marshal(u)
}

func (u *UserPermissions) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into UserPermissions", value)
	}
	
	return json.Unmarshal(bytes, u)
}

// UserLimits Value and Scan methods for database storage
func (u UserLimits) Value() (driver.Value, error) {
	return json.Marshal(u)
}

func (u *UserLimits) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into UserLimits", value)
	}
	
	return json.Unmarshal(bytes, u)
}