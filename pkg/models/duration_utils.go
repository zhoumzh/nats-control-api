package models

import (
	"fmt"
	"time"
)

func ParseDurationToSeconds(durationStr string) (int64, error) {
	if durationStr == "" {
		return 0, nil
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return 0, fmt.Errorf("invalid duration format: %w", err)
	}

	return int64(duration.Seconds()), nil
}

func ParseDurationSliceToSeconds(durationStrs []string) ([]int64, error) {
	if len(durationStrs) == 0 {
		return nil, nil
	}

	result := make([]int64, len(durationStrs))
	for i, str := range durationStrs {
		seconds, err := ParseDurationToSeconds(str)
		if err != nil {
			return nil, fmt.Errorf("invalid duration at index %d: %w", i, err)
		}
		result[i] = seconds
	}

	return result, nil
}