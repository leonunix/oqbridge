package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
)

// Progress tracks real-time migration progress.
type Progress struct {
	Index      string
	TotalDocs  int64
	Migrated   atomic.Int64
	StartTime  time.Time
}

// Migrator handles parallel migration of data from OpenSearch to Quickwit.
type Migrator struct {
	cfg        *config.Config
	hot        *backend.OpenSearch
	cold       *backend.Quickwit
	checkpoint *CheckpointStore
}

// NewMigrator creates a new Migrator.
func NewMigrator(cfg *config.Config, hot *backend.OpenSearch, cold *backend.Quickwit) (*Migrator, error) {
	cpStore, err := NewCheckpointStore(cfg.Migration.CheckpointDir)
	if err != nil {
		return nil, fmt.Errorf("initializing checkpoint store: %w", err)
	}
	return &Migrator{
		cfg:        cfg,
		hot:        hot,
		cold:       cold,
		checkpoint: cpStore,
	}, nil
}

// MigrateAll migrates all configured indices.
func (m *Migrator) MigrateAll(ctx context.Context) error {
	indices := m.cfg.Migration.Indices
	if len(indices) == 0 {
		slog.Info("no indices configured for migration, skipping")
		return nil
	}

	for _, index := range indices {
		if err := m.MigrateIndex(ctx, index); err != nil {
			slog.Error("migration failed for index", "index", index, "error", err)
			continue
		}
	}
	return nil
}

// MigrateIndex migrates documents older than the retention threshold from
// OpenSearch to Quickwit using parallel sliced scroll workers.
func (m *Migrator) MigrateIndex(ctx context.Context, index string) error {
	tsField := m.cfg.TimestampFieldForIndex(index)
	workers := m.cfg.Migration.Workers
	batchSize := m.cfg.Migration.BatchSize

	// Load checkpoint for resume support.
	cp, err := m.checkpoint.Load(index)
	if err != nil {
		slog.Warn("failed to load checkpoint, starting fresh", "index", index, "error", err)
	}

	slog.Info("starting migration",
		"index", index,
		"timestamp_field", tsField,
		"retention_days", m.cfg.Retention.Days,
		"workers", workers,
		"batch_size", batchSize,
		"resuming", cp != nil,
	)

	progress := &Progress{
		Index:     index,
		StartTime: time.Now(),
	}

	// Initialize checkpoint if not resuming.
	if cp == nil {
		cp = &Checkpoint{
			Index:     index,
			StartedAt: time.Now().UTC(),
		}
	}

	// Build the base query for documents older than retention.
	query := buildOldDataQuery(tsField, m.cfg.Retention.Days, batchSize)
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("marshaling migration query: %w", err)
	}

	// Launch parallel sliced scroll workers.
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		// Skip slices already completed in a previous run.
		if cp.IsSliceDone(i) {
			slog.Info("skipping completed slice", "index", index, "slice", i)
			continue
		}

		wg.Add(1)
		go func(sliceID int) {
			defer wg.Done()
			if err := m.migrateSlice(ctx, index, queryBytes, sliceID, workers, progress, cp); err != nil {
				errCh <- fmt.Errorf("slice %d: %w", sliceID, err)
			}
		}(i)
	}

	// Start progress reporter.
	stopProgress := make(chan struct{})
	go m.reportProgress(progress, stopProgress)

	wg.Wait()
	close(stopProgress)
	close(errCh)

	// Collect errors.
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		// Save checkpoint for resume.
		m.checkpoint.Save(cp)
		return fmt.Errorf("migration had %d slice errors, first: %w", len(errs), errs[0])
	}

	totalMigrated := progress.Migrated.Load()

	// Delete migrated data from OpenSearch if configured.
	if m.cfg.Migration.DeleteAfterMigration && totalMigrated > 0 {
		slog.Info("deleting migrated documents from opensearch",
			"index", index,
			"count", totalMigrated,
		)
		deleteQuery := buildOldDataDeleteQuery(tsField, m.cfg.Retention.Days)
		deleteBytes, _ := json.Marshal(deleteQuery)
		if err := m.hot.DeleteByQuery(ctx, index, deleteBytes); err != nil {
			return fmt.Errorf("deleting migrated documents: %w", err)
		}
	}

	// Mark checkpoint as completed.
	m.checkpoint.MarkComplete(index)

	elapsed := time.Since(progress.StartTime)
	slog.Info("migration completed",
		"index", index,
		"total_migrated", totalMigrated,
		"elapsed", elapsed.Round(time.Second).String(),
		"docs_per_sec", float64(totalMigrated)/elapsed.Seconds(),
	)
	return nil
}

// migrateSlice processes a single sliced scroll partition.
func (m *Migrator) migrateSlice(ctx context.Context, index string, queryBytes []byte, sliceID, sliceMax int, progress *Progress, cp *Checkpoint) error {
	slice := &backend.SlicedScrollConfig{
		SliceID:    sliceID,
		SliceMax:   sliceMax,
		ScrollKeep: "10m",
	}

	slog.Info("slice worker starting", "index", index, "slice", sliceID, "max", sliceMax)

	// Initial scroll.
	result, err := m.hot.SlicedScroll(ctx, index, queryBytes, "", slice)
	if err != nil {
		return fmt.Errorf("initiating scroll: %w", err)
	}

	defer func() {
		if result != nil && result.ScrollID != "" {
			if err := m.hot.ClearScroll(ctx, result.ScrollID); err != nil {
				slog.Warn("failed to clear scroll", "slice", sliceID, "error", err)
			}
		}
	}()

	sliceMigrated := 0

	for len(result.Hits) > 0 {
		// Transform documents.
		docs, err := TransformBatch(result.Hits)
		if err != nil {
			return fmt.Errorf("transforming batch: %w", err)
		}

		// Ingest into Quickwit.
		if err := m.cold.BulkIngest(ctx, index, docs); err != nil {
			return fmt.Errorf("ingesting batch: %w", err)
		}

		batchLen := len(docs)
		sliceMigrated += batchLen
		progress.Migrated.Add(int64(batchLen))

		// Continue scroll.
		result, err = m.hot.SlicedScroll(ctx, index, nil, result.ScrollID, slice)
		if err != nil {
			return fmt.Errorf("continuing scroll: %w", err)
		}
	}

	// Mark this slice as done in checkpoint.
	cp.SlicesDone = append(cp.SlicesDone, sliceID)
	cp.Migrated += int64(sliceMigrated)
	m.checkpoint.Save(cp)

	slog.Info("slice worker completed", "index", index, "slice", sliceID, "migrated", sliceMigrated)
	return nil
}

func (m *Migrator) reportProgress(progress *Progress, stop chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			migrated := progress.Migrated.Load()
			elapsed := time.Since(progress.StartTime)
			rate := float64(migrated) / elapsed.Seconds()
			slog.Info("migration progress",
				"index", progress.Index,
				"migrated", migrated,
				"elapsed", elapsed.Round(time.Second).String(),
				"docs_per_sec", int(rate),
			)
		}
	}
}

func buildOldDataQuery(tsField string, retentionDays, size int) map[string]interface{} {
	return map[string]interface{}{
		"size": size,
		"sort": []map[string]string{
			{tsField: "asc"},
		},
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				tsField: map[string]interface{}{
					"lt": fmt.Sprintf("now-%dd", retentionDays),
				},
			},
		},
	}
}

func buildOldDataDeleteQuery(tsField string, retentionDays int) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				tsField: map[string]interface{}{
					"lt": fmt.Sprintf("now-%dd", retentionDays),
				},
			},
		},
	}
}
