package db

import (
	"fmt"
	"time"

	"nats-control-api/pkg/models"

	"gorm.io/gorm"
)

// ClusterRepository handles cluster-related database operations
type ClusterRepository struct {
	db *gorm.DB
}

// NewClusterRepository creates a new cluster repository
func NewClusterRepository(db *gorm.DB) *ClusterRepository {
	return &ClusterRepository{db: db}
}

// Cluster basic operations

// CreateCluster creates a new cluster
func (r *ClusterRepository) CreateCluster(cluster *models.Cluster) error {
	return r.db.Create(cluster).Error
}

// GetClusterByID retrieves a cluster by ID
func (r *ClusterRepository) GetClusterByID(id string) (*models.Cluster, error) {
	var cluster models.Cluster
	err := r.db.Where("id = ?", id).First(&cluster).Error
	if err != nil {
		return nil, err
	}
	return &cluster, nil
}

// ListClusters retrieves all clusters with optional status filter
func (r *ClusterRepository) ListClusters(status string) ([]*models.Cluster, error) {
	var clusters []*models.Cluster
	query := r.db
	
	if status != "" {
		query = query.Where("status = ?", status)
	}
	
	err := query.Find(&clusters).Error
	return clusters, err
}

// UpdateCluster updates an existing cluster
func (r *ClusterRepository) UpdateCluster(cluster *models.Cluster) error {
	return r.db.Save(cluster).Error
}

// DeleteCluster deletes a cluster by ID
func (r *ClusterRepository) DeleteCluster(id string) error {
	return r.db.Where("id = ?", id).Delete(&models.Cluster{}).Error
}

// GetActiveClusters retrieves all active clusters
func (r *ClusterRepository) GetActiveClusters() ([]*models.Cluster, error) {
	return r.ListClusters(string(models.ClusterStatusActive))
}

// Cluster health monitoring operations

// CreateClusterHealth creates a new cluster health record
func (r *ClusterRepository) CreateClusterHealth(health *models.ClusterHealth) error {
	return r.db.Create(health).Error
}

// GetClusterHealth retrieves cluster health records with filters
func (r *ClusterRepository) GetClusterHealth(req *models.ClusterHealthListRequest) ([]*models.ClusterHealthResponse, int64, error) {
	var healths []*models.ClusterHealth
	var total int64
	
	query := r.db.Model(&models.ClusterHealth{})
	
	// Apply filters
	if req.ClusterID != "" {
		query = query.Where("cluster_id = ?", req.ClusterID)
	}
	if req.Status != "" {
		query = query.Where("status = ?", req.Status)
	}
	if req.ConnectionStatus != "" {
		query = query.Where("connection_status = ?", req.ConnectionStatus)
	}
	if req.StartDate != "" {
		startTime, err := time.Parse("2006-01-02", req.StartDate)
		if err == nil {
			query = query.Where("tested_at >= ?", startTime)
		}
	}
	if req.EndDate != "" {
		endTime, err := time.Parse("2006-01-02", req.EndDate)
		if err == nil {
			// Add 24 hours to include the entire end date
			endTime = endTime.Add(24 * time.Hour)
			query = query.Where("tested_at <= ?", endTime)
		}
	}
	
	// Count total records
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	
	// Apply pagination
	if req.Page > 0 && req.PageSize > 0 {
		offset := (req.Page - 1) * req.PageSize
		query = query.Offset(offset).Limit(req.PageSize)
	}
	
	// Get records with cluster information
	err := query.Preload("Cluster").Order("tested_at DESC").Find(&healths).Error
	if err != nil {
		return nil, 0, err
	}
	
	// Convert to response format
	var responses []*models.ClusterHealthResponse
	for _, health := range healths {
		response := &models.ClusterHealthResponse{
			ClusterHealth: health,
		}
		
		if health.Cluster != nil {
			response.ClusterName = health.Cluster.Name
			response.HostInfo = fmt.Sprintf("%s:%d", health.Cluster.Host, health.Cluster.NATSPort)
		}
		
		responses = append(responses, response)
	}
	
	return responses, total, nil
}

// GetLatestClusterHealth retrieves the latest health record for each cluster
func (r *ClusterRepository) GetLatestClusterHealth() ([]*models.ClusterHealthResponse, error) {
	var healths []*models.ClusterHealth
	
	// Get the latest health record for each cluster
	subQuery := r.db.Model(&models.ClusterHealth{}).
		Select("cluster_id, MAX(tested_at) as max_tested_at").
		Group("cluster_id")
	
	err := r.db.Model(&models.ClusterHealth{}).
		Joins("JOIN (?) as latest ON cluster_healths.cluster_id = latest.cluster_id AND cluster_healths.tested_at = latest.max_tested_at", subQuery).
		Preload("Cluster").
		Find(&healths).Error
		
	if err != nil {
		return nil, err
	}
	
	// Convert to response format
	var responses []*models.ClusterHealthResponse
	for _, health := range healths {
		response := &models.ClusterHealthResponse{
			ClusterHealth: health,
		}
		
		if health.Cluster != nil {
			response.ClusterName = health.Cluster.Name
			response.HostInfo = fmt.Sprintf("%s:%d", health.Cluster.Host, health.Cluster.NATSPort)
		}
		
		responses = append(responses, response)
	}
	
	return responses, nil
}

// GetClusterHealthStats calculates cluster health statistics
func (r *ClusterRepository) GetClusterHealthStats() (*models.ClusterStats, error) {
	var stats models.ClusterStats
	
	// Get total clusters count
	var totalCount int64
	if err := r.db.Model(&models.Cluster{}).Count(&totalCount).Error; err != nil {
		return nil, err
	}
	stats.TotalClusters = int(totalCount)
	
	// Get latest health records
	latestHealths, err := r.GetLatestClusterHealth()
	if err != nil {
		return nil, err
	}
	
	// Count by health status
	for _, health := range latestHealths {
		switch health.Status {
		case models.ClusterHealthStatusHealthy:
			stats.HealthyClusters++
		case models.ClusterHealthStatusUnhealthy:
			stats.UnhealthyClusters++
		default:
			stats.UnknownClusters++
		}
	}
	
	// Clusters without health records are considered unknown
	stats.UnknownClusters += stats.TotalClusters - len(latestHealths)
	
	return &stats, nil
}

// GetUnhealthyClusters retrieves clusters that are currently unhealthy
func (r *ClusterRepository) GetUnhealthyClusters() ([]*models.ClusterHealthResponse, error) {
	latestHealths, err := r.GetLatestClusterHealth()
	if err != nil {
		return nil, err
	}
	
	var unhealthy []*models.ClusterHealthResponse
	for _, health := range latestHealths {
		if health.Status == models.ClusterHealthStatusUnhealthy {
			unhealthy = append(unhealthy, health)
		}
	}
	
	return unhealthy, nil
}
