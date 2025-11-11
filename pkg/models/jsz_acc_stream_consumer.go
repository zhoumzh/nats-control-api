package models

import (
	"encoding/json"
	"strings"
	"time"
)

type APIStats struct {
	Level  int `json:"level"`
	Total  int `json:"total"`
	Errors int `json:"errors"`
}

type Config struct {
	MaxMemory    int64  `json:"max_memory"`
	MaxStorage   int64  `json:"max_storage"`
	StoreDir     string `json:"store_dir"`
	SyncInterval int64  `json:"sync_interval"`
	Domain       string `json:"domain"`
	Strict       bool   `json:"strict"`
}

type Replica struct {
	Name    string `json:"name"`
	Current bool   `json:"current"`
	Active  int64  `json:"active"`
	Peer    string `json:"peer"`
}

type MetaCluster struct {
	Name        string    `json:"name"`
	Leader      string    `json:"leader"`
	Peer        string    `json:"peer"`
	Replicas    []Replica `json:"replicas"`
	ClusterSize int       `json:"cluster_size"`
	Pending     int       `json:"pending"`
}

type AccountDetail struct {
	Name                  string      `json:"name"`
	ID                    string      `json:"id"`
	MemoryValue           float64     `json:"memory_value"`
	MemoryUnit            StorageUnit `json:"memory_unit"`
	StorageValue          float64     `json:"storage_value"`
	StorageUnit           StorageUnit `json:"storage_unit"`
	ReservedMemoryValue   float64     `json:"reserved_memory_value"`
	ReservedMemoryUnit    StorageUnit `json:"reserved_memory_unit"`
	ReservedStorageValue  float64     `json:"reserved_storage_value"`
	ReservedStorageUnit   StorageUnit `json:"reserved_storage_unit"`
	Accounts              int         `json:"accounts"`
	HAAssets              int         `json:"ha_assets"`
	API                   APIStats    `json:"api"`
}

func (a *AccountDetail) UnmarshalJSON(data []byte) error {
	type Alias struct {
		Name            string          `json:"name"`
		ID              string          `json:"id"`
		Memory          int64           `json:"memory"`
		Storage         int64           `json:"storage"`
		ReservedMemory  json.Number     `json:"reserved_memory"`
		ReservedStorage json.Number     `json:"reserved_storage"`
		Accounts        int             `json:"accounts"`
		HAAssets        int             `json:"ha_assets"`
		API             APIStats        `json:"api"`
	}

	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var alias Alias
	if err := decoder.Decode(&alias); err != nil {
		return err
	}

	a.Name = alias.Name
	a.ID = alias.ID
	a.MemoryValue, a.MemoryUnit = ConvertFromBytes(alias.Memory)
	a.StorageValue, a.StorageUnit = ConvertFromBytes(alias.Storage)
	
	if alias.ReservedMemory != "" {
		if reservedMem, err := alias.ReservedMemory.Float64(); err == nil {
			a.ReservedMemoryValue, a.ReservedMemoryUnit = ConvertFromBytes(int64(reservedMem))
		}
	}
	if alias.ReservedStorage != "" {
		if reservedSto, err := alias.ReservedStorage.Float64(); err == nil {
			a.ReservedStorageValue, a.ReservedStorageUnit = ConvertFromBytes(int64(reservedSto))
		}
	}
	
	a.Accounts = alias.Accounts
	a.HAAssets = alias.HAAssets
	a.API = alias.API

	return nil
}

type JetStreamResponse struct {
	MemoryValue          float64     `json:"memory_value"`
	MemoryUnit           StorageUnit `json:"memory_unit"`
	StorageValue         float64     `json:"storage_value"`
	StorageUnit          StorageUnit `json:"storage_unit"`
	ReservedMemoryValue  float64     `json:"reserved_memory_value"`
	ReservedMemoryUnit   StorageUnit `json:"reserved_memory_unit"`
	ReservedStorageValue float64     `json:"reserved_storage_value"`
	ReservedStorageUnit  StorageUnit `json:"reserved_storage_unit"`

	Accounts int `json:"accounts"`
	HAAssets int `json:"ha_assets"`

	API APIStats `json:"api"`

	ServerID string    `json:"server_id"`
	Now      time.Time `json:"now"`

	Config Config                 `json:"config"`
	Limits map[string]interface{} `json:"limits"`

	Streams   int `json:"streams"`
	Consumers int `json:"consumers"`
	Messages  int `json:"messages"`
	Bytes     int `json:"bytes"`

	MetaCluster    MetaCluster     `json:"meta_cluster"`
	AccountDetails []AccountDetail `json:"account_details"`

	Total int `json:"total"`
}

func (j *JetStreamResponse) UnmarshalJSON(data []byte) error {
	type Alias struct {
		Memory          int64                  `json:"memory"`
		Storage         int64                  `json:"storage"`
		ReservedMemory  json.Number            `json:"reserved_memory"`
		ReservedStorage json.Number            `json:"reserved_storage"`
		Accounts        int                    `json:"accounts"`
		HAAssets        int                    `json:"ha_assets"`
		API             APIStats               `json:"api"`
		ServerID        string                 `json:"server_id"`
		Now             time.Time              `json:"now"`
		Config          Config                 `json:"config"`
		Limits          map[string]interface{} `json:"limits"`
		Streams         int                    `json:"streams"`
		Consumers       int                    `json:"consumers"`
		Messages        int                    `json:"messages"`
		Bytes           int                    `json:"bytes"`
		MetaCluster     MetaCluster            `json:"meta_cluster"`
		AccountDetails  []AccountDetail        `json:"account_details"`
		Total           int                    `json:"total"`
	}

	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var alias Alias
	if err := decoder.Decode(&alias); err != nil {
		return err
	}

	j.MemoryValue, j.MemoryUnit = ConvertFromBytes(alias.Memory)
	j.StorageValue, j.StorageUnit = ConvertFromBytes(alias.Storage)
	
	if alias.ReservedMemory != "" {
		if reservedMem, err := alias.ReservedMemory.Float64(); err == nil {
			j.ReservedMemoryValue, j.ReservedMemoryUnit = ConvertFromBytes(int64(reservedMem))
		}
	}
	if alias.ReservedStorage != "" {
		if reservedSto, err := alias.ReservedStorage.Float64(); err == nil {
			j.ReservedStorageValue, j.ReservedStorageUnit = ConvertFromBytes(int64(reservedSto))
		}
	}
	
	j.Accounts = alias.Accounts
	j.HAAssets = alias.HAAssets
	j.API = alias.API
	j.ServerID = alias.ServerID
	j.Now = alias.Now
	j.Config = alias.Config
	j.Limits = alias.Limits
	j.Streams = alias.Streams
	j.Consumers = alias.Consumers
	j.Messages = alias.Messages
	j.Bytes = alias.Bytes
	j.MetaCluster = alias.MetaCluster
	j.AccountDetails = alias.AccountDetails
	j.Total = alias.Total

	return nil
}
