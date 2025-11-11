package db

import (
	"context"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"

	"nats-control-api/pkg/models"

	"gorm.io/gorm"
)

type ConsumerRepository struct {
	db *gorm.DB
}

func NewConsumerRepository(db *gorm.DB) *ConsumerRepository {
	return &ConsumerRepository{db: db}
}

func (r *ConsumerRepository) CreateConsumer(consumer *models.Consumer) error {
	log.WithContext(context.Background()).Infof("Creating Consumer: %+v", consumer)
	return r.db.Create(consumer).Error
}

func (r *ConsumerRepository) GetConsumer(id string) (*models.Consumer, error) {
	log.WithContext(context.Background()).Infof("Getting Consumer by ID: %s", id)
	var consumer models.Consumer
	err := r.db.First(&consumer, "id = ?", id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &consumer, nil
}

func (r *ConsumerRepository) GetConsumerByNameAndJetStream(name, jetstreamID string) (*models.Consumer, error) {
	log.WithContext(context.Background()).Infof("Getting Consumer by name: %s, jetstream: %s", name, jetstreamID)
	var consumer models.Consumer
	err := r.db.Where("name = ? AND jetstream_id = ?", name, jetstreamID).First(&consumer).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &consumer, nil
}

func (r *ConsumerRepository) ListConsumers(clusterID, jetstreamID, natsOperateUserID, consumerType, status, syncStatus, search string, isDurable *bool, limit, offset int) ([]*models.Consumer, int64, error) {
	log.WithContext(context.Background()).Infof("Listing Consumers with filters - cluster: %s, jetstream: %s, nats_operate_user: %s, type: %s, status: %s, sync_status: %s, search: %s, is_durable: %v, limit: %d, offset: %d",
		clusterID, jetstreamID, natsOperateUserID, consumerType, status, syncStatus, search, isDurable, limit, offset)

	var consumers []*models.Consumer
	var total int64

	query := r.db.Model(&models.Consumer{})

	if clusterID != "" {
		if jetstreamID != "" {
			var js models.JetStream
			err := r.db.Select("id").Where("id = ? AND cluster_id = ?", jetstreamID, clusterID).First(&js).Error
			if err != nil {
				if err == gorm.ErrRecordNotFound {
					return []*models.Consumer{}, 0, nil
				}
				return nil, 0, err
			}
			query = query.Where("jetstream_id = ?", jetstreamID)
		} else {
			query = query.Joins("JOIN jetstreams ON consumers.jetstream_id = jetstreams.id").Where("jetstreams.cluster_id = ?", clusterID)
		}
	} else if jetstreamID != "" {
		query = query.Where("jetstream_id = ?", jetstreamID)
	}

	if natsOperateUserID != "" {
		query = query.Where("consumers.nats_operate_user_id = ?", natsOperateUserID)
	}
	if consumerType != "" {
		query = query.Where("consumers.consumer_type = ?", consumerType)
	}
	if status != "" {
		query = query.Where("consumers.status = ?", status)
	}
	if syncStatus != "" {
		query = query.Where("consumers.sync_status = ?", syncStatus)
	}
	if search != "" {
		query = query.Where("consumers.name LIKE ? OR consumers.description LIKE ?", "%"+search+"%", "%"+search+"%")
	}
	if isDurable != nil {
		if *isDurable {
			query = query.Where("consumers.durable != ''")
		} else {
			query = query.Where("consumers.durable = ''")
		}
	}

	err := query.Count(&total).Error
	if err != nil {
		return nil, 0, err
	}

	err = query.Order("consumers.created_at DESC").Limit(limit).Offset(offset).Find(&consumers).Error
	if err != nil {
		return nil, 0, err
	}

	return consumers, total, nil
}

func (r *ConsumerRepository) UpdateConsumer(consumer *models.Consumer) error {
	log.WithContext(context.Background()).Infof("Updating Consumer: %+v", consumer)
	return r.db.Save(consumer).Error
}

func (r *ConsumerRepository) UpdateConsumerFields(id string, fields map[string]interface{}) error {
	log.WithContext(context.Background()).Infof("Updating Consumer %s with fields: %+v", id, fields)
	return r.db.Model(&models.Consumer{}).Where("id = ?", id).Updates(fields).Error
}

func (r *ConsumerRepository) DeleteConsumer(id string) error {
	log.WithContext(context.Background()).Infof("Deleting Consumer: %s", id)
	return r.db.Delete(&models.Consumer{}, "id = ?", id).Error
}

func (r *ConsumerRepository) ListConsumersBySyncStatus(syncStatus models.ConsumerSyncStatus, limit, offset int) ([]*models.Consumer, int64, error) {
	log.WithContext(context.Background()).Infof("Listing Consumers by sync status: %s, limit: %d, offset: %d", syncStatus, limit, offset)

	var consumers []*models.Consumer
	var total int64

	query := r.db.Model(&models.Consumer{}).Where("sync_status = ?", syncStatus)

	err := query.Count(&total).Error
	if err != nil {
		return nil, 0, err
	}

	err = query.Order("created_at DESC").Limit(limit).Offset(offset).Find(&consumers).Error
	if err != nil {
		return nil, 0, err
	}

	return consumers, total, nil
}

func (r *ConsumerRepository) GetConsumerStats() (map[string]interface{}, error) {
	log.WithContext(context.Background()).Infof("Getting Consumer statistics")

	var stats struct {
		Total       int64 `json:"total"`
		Pull        int64 `json:"pull"`
		Push        int64 `json:"push"`
		Durable     int64 `json:"durable"`
		Ephemeral   int64 `json:"ephemeral"`
		Active      int64 `json:"active"`
		Paused      int64 `json:"paused"`
		SyncPending int64 `json:"sync_pending"`
		SyncSynced  int64 `json:"sync_synced"`
		SyncFailed  int64 `json:"sync_failed"`
	}

	r.db.Model(&models.Consumer{}).Count(&stats.Total)

	r.db.Model(&models.Consumer{}).Where("consumer_type = ?", models.ConsumerTypePull).Count(&stats.Pull)
	r.db.Model(&models.Consumer{}).Where("consumer_type = ?", models.ConsumerTypePush).Count(&stats.Push)

	r.db.Model(&models.Consumer{}).Where("durable != ''").Count(&stats.Durable)
	r.db.Model(&models.Consumer{}).Where("durable = ''").Count(&stats.Ephemeral)

	r.db.Model(&models.Consumer{}).Where("status = ?", "active").Count(&stats.Active)
	r.db.Model(&models.Consumer{}).Where("status = ?", "paused").Count(&stats.Paused)

	r.db.Model(&models.Consumer{}).Where("sync_status = ?", models.ConsumerSyncPending).Count(&stats.SyncPending)
	r.db.Model(&models.Consumer{}).Where("sync_status = ?", models.ConsumerSyncSynced).Count(&stats.SyncSynced)
	r.db.Model(&models.Consumer{}).Where("sync_status = ?", models.ConsumerSyncFailed).Count(&stats.SyncFailed)

	return map[string]interface{}{
		"total":        stats.Total,
		"pull":         stats.Pull,
		"push":         stats.Push,
		"durable":      stats.Durable,
		"ephemeral":    stats.Ephemeral,
		"active":       stats.Active,
		"paused":       stats.Paused,
		"sync_pending": stats.SyncPending,
		"sync_synced":  stats.SyncSynced,
		"sync_failed":  stats.SyncFailed,
	}, nil
}

func (r *ConsumerRepository) ListConsumersByJetStream(jetstreamID string) ([]*models.Consumer, error) {
	log.WithContext(context.Background()).Infof("Listing Consumers by JetStream: %s", jetstreamID)
	var consumers []*models.Consumer
	err := r.db.Where("jetstream_id = ?", jetstreamID).Order("created_at DESC").Find(&consumers).Error
	if err != nil {
		return nil, err
	}
	return consumers, nil
}