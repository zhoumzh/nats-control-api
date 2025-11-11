package integration

import (
	"path/filepath"
	"testing"
	"time"

	"nats-control-api/internal/config"
	"nats-control-api/internal/db"
	"nats-control-api/internal/service"
)

func TestGetJetStreamInfo(t *testing.T) {
	clusterID := "6179c422-583b-4f29-af1b-821f31308724"
	accountID := "replace-with-actual-account-id"

	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}

	cfg.Database.DSN = filepath.Join("../../", cfg.Database.DSN)
	t.Logf("Database DSN: %s", cfg.Database.DSN)

	repo, err := db.NewRepository(cfg.Database.Driver, cfg.Database.DSN)
	if err != nil {
		t.Fatalf("db.NewRepository() error = %v", err)
	}

	clusterService := service.NewClusterService(repo, cfg)
	monitorService := service.NewClusterMonitorService(repo, clusterService, cfg)

	start := time.Now()
	streamNames, err := monitorService.GetStreamNamesByAccount(clusterID, accountID)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("GetStreamNamesByAccount() error = %v", err)
	}

	t.Logf("Duration: %v", duration)
	t.Logf("Stream Names: %+v", streamNames)
}
