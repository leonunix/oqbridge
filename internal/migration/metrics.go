package migration

import (
	"context"
	"time"
)

// MigrationMetric records the outcome of a single index migration run.
type MigrationMetric struct {
	Timestamp         time.Time `json:"@timestamp"`
	Index             string    `json:"index"`
	StartedAt         time.Time `json:"started_at"`
	CompletedAt       time.Time `json:"completed_at"`
	DurationSec       float64   `json:"duration_sec"`
	DocumentsMigrated int64     `json:"documents_migrated"`
	DocsPerSec        float64   `json:"docs_per_sec"`
	Workers           int       `json:"workers"`
	BatchSize         int       `json:"batch_size"`
	Status            string    `json:"status"` // "success" or "failed"
	Error             string    `json:"error,omitempty"`
	CutoffTime        time.Time `json:"cutoff_time"`
}

// MetricsRecorder persists migration metrics for later analysis.
type MetricsRecorder interface {
	Record(ctx context.Context, metric *MigrationMetric) error
}

// NewSuccessMetric creates a metric for a successful migration run.
func NewSuccessMetric(index string, startTime time.Time, docsMigrated int64, cutoff time.Time, workers, batchSize int) *MigrationMetric {
	now := time.Now().UTC()
	elapsed := now.Sub(startTime)
	var rate float64
	if elapsed.Seconds() > 0 {
		rate = float64(docsMigrated) / elapsed.Seconds()
	}
	return &MigrationMetric{
		Timestamp:         now,
		Index:             index,
		StartedAt:         startTime.UTC(),
		CompletedAt:       now,
		DurationSec:       elapsed.Seconds(),
		DocumentsMigrated: docsMigrated,
		DocsPerSec:        rate,
		Workers:           workers,
		BatchSize:         batchSize,
		Status:            "success",
		CutoffTime:        cutoff.UTC(),
	}
}

// NewFailureMetric creates a metric for a failed migration run.
func NewFailureMetric(index string, startTime time.Time, docsMigrated int64, cutoff time.Time, workers, batchSize int, err error) *MigrationMetric {
	now := time.Now().UTC()
	elapsed := now.Sub(startTime)
	var rate float64
	if elapsed.Seconds() > 0 {
		rate = float64(docsMigrated) / elapsed.Seconds()
	}
	return &MigrationMetric{
		Timestamp:         now,
		Index:             index,
		StartedAt:         startTime.UTC(),
		CompletedAt:       now,
		DurationSec:       elapsed.Seconds(),
		DocumentsMigrated: docsMigrated,
		DocsPerSec:        rate,
		Workers:           workers,
		BatchSize:         batchSize,
		Status:            "failed",
		Error:             err.Error(),
		CutoffTime:        cutoff.UTC(),
	}
}
