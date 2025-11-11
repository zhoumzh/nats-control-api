package integration

import (
	"nats-control-api/internal/config"
	"nats-control-api/internal/db"
	natssvc "nats-control-api/internal/nats"
	"nats-control-api/internal/service"
	"path/filepath"
	"testing"
	"time"
)

func TestListStreams(t *testing.T) {

	clusterID := "6179c422-583b-4f29-af1b-821f31308724"
	userID := "357cbade-d064-4b9e-8efb-a5283389172b"

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

	conn, js, err := clusterService.GetNatsJetStreamWithUser(clusterID, userID)
	if err != nil {
		t.Fatalf("GetNatsJetStreamWithUser() error = %v", err)
	}
	defer conn.Close()

	natsService := natssvc.NewService()

	start := time.Now()
	names, err := natsService.ListStreams(js)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("ListStreams() error = %v", err)
	}

	t.Logf("Duration: %v", duration)
	t.Logf("Found %d streams: %v", len(names), names)
}
