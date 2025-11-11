package db

import (
	"context"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/pkg/models"

	"gorm.io/gorm"
)

type JetStreamRepository struct {
	db *gorm.DB
}

func NewJetStreamRepository(db *gorm.DB) *JetStreamRepository {
	return &JetStreamRepository{db: db}
}

// CreateJetStream creates a new JetStream in the database
func (r *JetStreamRepository) CreateJetStream(js *models.JetStream) error {
	log.WithContext(context.Background()).Infof("Creating JetStream: %+v", js)
	return r.db.Create(js).Error
}

// GetJetStream retrieves a JetStream by ID
func (r *JetStreamRepository) GetJetStream(id string) (*models.JetStream, error) {
	log.WithContext(context.Background()).Infof("Getting JetStream by ID: %s", id)
	var js models.JetStream
	err := r.db.First(&js, "id = ?", id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &js, nil
}

// GetJetStreamByNameAndUser retrieves a JetStream by name and nats operate user
func (r *JetStreamRepository) GetJetStreamByNameAndUser(name, natsOperateUserID string) (*models.JetStream, error) {
	log.WithContext(context.Background()).Infof("Getting JetStream by name: %s, nats operate user: %s", name, natsOperateUserID)
	var js models.JetStream
	err := r.db.Where("name = ? AND nats_operate_user_id = ?", name, natsOperateUserID).First(&js).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &js, nil
}

// GetJetStreamByNameAndCluster retrieves a JetStream by name and cluster
func (r *JetStreamRepository) GetJetStreamByNameAndCluster(name, clusterID string) (*models.JetStream, error) {
	log.WithContext(context.Background()).Infof("Getting JetStream by name: %s, cluster: %s", name, clusterID)
	var js models.JetStream
	err := r.db.Where("name = ? AND cluster_id = ?", name, clusterID).First(&js).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &js, nil
}

// ListJetStreams retrieves JetStreams with optional filters
func (r *JetStreamRepository) ListJetStreams(natsOperateUserID, status, search, clusterID, syncStatus string, limit, offset int) ([]*models.JetStream, int64, error) {
	log.WithContext(context.Background()).Infof("Listing JetStreams with filters - nats operate user: %s, status: %s, search: %s, cluster_id: %s, sync_status: %s, limit: %d, offset: %d", 
		natsOperateUserID, status, search, clusterID, syncStatus, limit, offset)
	
	var jetStreams []*models.JetStream
	var total int64
	
	query := r.db.Model(&models.JetStream{})
	
	// Apply filters
	if natsOperateUserID != "" {
		query = query.Where("nats_operate_user_id = ?", natsOperateUserID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}
	if search != "" {
		query = query.Where("name LIKE ? OR description LIKE ?", "%"+search+"%", "%"+search+"%")
	}
	if clusterID != "" {
		query = query.Where("cluster_id = ?", clusterID)
	}
	if syncStatus != "" {
		query = query.Where("sync_status = ?", syncStatus)
	}
	
	// Get total count
	err := query.Count(&total).Error
	if err != nil {
		return nil, 0, err
	}
	
	// Get paginated results
	err = query.Order("created_at DESC").Limit(limit).Offset(offset).Find(&jetStreams).Error
	if err != nil {
		return nil, 0, err
	}
	
	return jetStreams, total, nil
}

// UpdateJetStream updates a JetStream in the database
func (r *JetStreamRepository) UpdateJetStream(js *models.JetStream) error {
	log.WithContext(context.Background()).Infof("Updating JetStream: %+v", js)
	return r.db.Save(js).Error
}

// UpdateJetStreamFields updates specific fields of a JetStream
func (r *JetStreamRepository) UpdateJetStreamFields(id string, fields map[string]interface{}) error {
	log.WithContext(context.Background()).Infof("Updating JetStream %s with fields: %+v", id, fields)
	return r.db.Model(&models.JetStream{}).Where("id = ?", id).Updates(fields).Error
}

// DeleteJetStream deletes a JetStream from the database
func (r *JetStreamRepository) DeleteJetStream(id string) error {
	log.WithContext(context.Background()).Infof("Deleting JetStream: %s", id)
	return r.db.Delete(&models.JetStream{}, "id = ?", id).Error
}

// GetJetStreamStats retrieves statistics for JetStreams
func (r *JetStreamRepository) GetJetStreamStats() (map[string]interface{}, error) {
	log.WithContext(context.Background()).Infof("Getting JetStream statistics")
	
	var stats struct {
		Total       int64 `json:"total"`
		Active      int64 `json:"active"`
		Inactive    int64 `json:"inactive"`
		Error       int64 `json:"error"`
		SyncPending int64 `json:"sync_pending"`
		SyncSynced  int64 `json:"sync_synced"`
		SyncFailed  int64 `json:"sync_failed"`
	}
	
	// Total count
	r.db.Model(&models.JetStream{}).Count(&stats.Total)
	
	// Status counts
	r.db.Model(&models.JetStream{}).Where("status = ?", "active").Count(&stats.Active)
	r.db.Model(&models.JetStream{}).Where("status = ?", "inactive").Count(&stats.Inactive)
	r.db.Model(&models.JetStream{}).Where("status = ?", "error").Count(&stats.Error)
	
	// Sync status counts
	r.db.Model(&models.JetStream{}).Where("sync_status = ?", models.JetStreamSyncPending).Count(&stats.SyncPending)
	r.db.Model(&models.JetStream{}).Where("sync_status = ?", models.JetStreamSyncSynced).Count(&stats.SyncSynced)
	r.db.Model(&models.JetStream{}).Where("sync_status = ?", models.JetStreamSyncFailed).Count(&stats.SyncFailed)
	
	return map[string]interface{}{
		"total":        stats.Total,
		"active":       stats.Active,
		"inactive":     stats.Inactive,
		"error":        stats.Error,
		"sync_pending": stats.SyncPending,
		"sync_synced":  stats.SyncSynced,
		"sync_failed":  stats.SyncFailed,
	}, nil
}

// ListJetStreamsBySyncStatus retrieves JetStreams filtered by sync status
func (r *JetStreamRepository) ListJetStreamsBySyncStatus(syncStatus models.JetStreamSyncStatus, limit, offset int) ([]*models.JetStream, int64, error) {
	log.WithContext(context.Background()).Infof("Listing JetStreams by sync status: %s, limit: %d, offset: %d", syncStatus, limit, offset)
	
	var jetStreams []*models.JetStream
	var total int64
	
	query := r.db.Model(&models.JetStream{}).Where("sync_status = ?", syncStatus)
	
	// Get total count
	err := query.Count(&total).Error
	if err != nil {
		return nil, 0, err
	}
	
	// Get paginated results
	err = query.Order("created_at DESC").Limit(limit).Offset(offset).Find(&jetStreams).Error
	if err != nil {
		return nil, 0, err
	}
	
	return jetStreams, total, nil
}