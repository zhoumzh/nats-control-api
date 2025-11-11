package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gitlabee.chehejia.com/gopkg/lsego/pkg/log"
	"github.com/nats-io/nats.go"
)

// GetStreamListWithJSAPI gets stream list using JS API subject
func (s *Service) GetStreamListWithJSAPI(nc *nats.Conn) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := nc.RequestWithContext(ctx, "$JS.API.STREAM.LIST", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to request stream list: %w", err)
	}

	var streamListResp map[string]interface{}
	if err := json.Unmarshal(resp.Data, &streamListResp); err != nil {
		log.WithContext(context.Background()).Errorf("Failed to parse stream list response: %v, Response data: %s", err, string(resp.Data))
		return nil, fmt.Errorf("failed to parse stream list response: %w", err)
	}

	if errorInfo, exists := streamListResp["error"]; exists {
		return nil, fmt.Errorf("JS API error: %v", errorInfo)
	}

	streams, _ := streamListResp["streams"].([]interface{})
	streamCount := 0
	if streams != nil {
		streamCount = len(streams)
	}

	log.WithContext(context.Background()).Infof("Successfully got stream list from JS API - Streams count: %d", streamCount)

	return streamListResp, nil
}

// GetStreamInfoWithJSAPI gets stream info using JS API subject
func (s *Service) GetStreamInfoWithJSAPI(nc *nats.Conn, streamName string) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subject := fmt.Sprintf("$JS.API.STREAM.INFO.%s", streamName)
	resp, err := nc.RequestWithContext(ctx, subject, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to request stream info for %s: %w", streamName, err)
	}

	var streamInfo map[string]interface{}
	if err := json.Unmarshal(resp.Data, &streamInfo); err != nil {
		return nil, fmt.Errorf("failed to parse stream info response: %w", err)
	}

	if errorInfo, exists := streamInfo["error"]; exists {
		return nil, fmt.Errorf("JS API error for stream %s: %v", streamName, errorInfo)
	}

	return streamInfo, nil
}