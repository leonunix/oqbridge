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
	Index     string
	TotalDocs int64
	Migrated  atomic.Int64
	StartTime time.Time
}

// Migrator handles parallel migration of data from OpenSearch to Quickwit.
type Migrator struct {
	cfg              *config.Config
	hot              HotClient
	cold             ColdClient
	checkpoint       *CheckpointStore
	progressInterval time.Duration
}

// NewMigrator creates a new Migrator.
func NewMigrator(cfg *config.Config, hot HotClient, cold ColdClient) (*Migrator, error) {
	cpStore, err := NewCheckpointStore(cfg.Migration.CheckpointDir)
	if err != nil {
		return nil, fmt.Errorf("initializing checkpoint store: %w", err)
	}
	return &Migrator{
		cfg:              cfg,
		hot:              hot,
		cold:             cold,
		checkpoint:       cpStore,
		progressInterval: 10 * time.Second,
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

	// Ensure Quickwit index exists before migration.
	if err := m.ensureQuickwitIndex(ctx, index, tsField); err != nil {
		return fmt.Errorf("ensuring quickwit index: %w", err)
	}

	workers := m.cfg.Migration.Workers
	batchSize := m.cfg.Migration.BatchSize

	// Load checkpoint for resume support.
	cp, err := m.checkpoint.Load(index)
	if err != nil {
		slog.Warn("failed to load checkpoint, starting fresh", "index", index, "error", err)
	}

	migrateDays := m.cfg.Migration.MigrateAfterDays
	cutoffTime := time.Now().UTC().AddDate(0, 0, -migrateDays)

	// Load watermark from last successful run for incremental migration.
	wm, wmErr := m.checkpoint.LoadWatermark(index)
	if wmErr != nil {
		slog.Warn("failed to load watermark, will migrate all old data", "index", index, "error", wmErr)
	}

	slog.Info("starting migration",
		"index", index,
		"timestamp_field", tsField,
		"migrate_after_days", migrateDays,
		"cutoff", cutoffTime.Format(time.RFC3339),
		"watermark", watermarkStr(wm),
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

	// Snapshot completed slices at start to avoid data races while workers run.
	doneSlices := make(map[int]struct{}, len(cp.SlicesDone))
	for _, id := range cp.SlicesDone {
		doneSlices[id] = struct{}{}
	}
	var cpMu sync.Mutex

	// Build the query for the incremental time window.
	var fromTime *time.Time
	if wm != nil && !wm.MigratedBefore.IsZero() {
		fromTime = &wm.MigratedBefore
	}
	query := buildMigrationQuery(tsField, fromTime, cutoffTime, batchSize)
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("marshaling migration query: %w", err)
	}

	// Launch parallel sliced scroll workers.
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		// Skip slices already completed in a previous run.
		if _, ok := doneSlices[i]; ok {
			slog.Info("skipping completed slice", "index", index, "slice", i)
			continue
		}

		wg.Add(1)
		go func(sliceID int) {
			defer wg.Done()
			if err := m.migrateSlice(ctx, index, queryBytes, sliceID, workers, progress, cp, &cpMu); err != nil {
				errCh <- fmt.Errorf("slice %d: %w", sliceID, err)
			}
		}(i)
	}

	// Start progress reporter.
	stopProgress := make(chan struct{})
	ticker := time.NewTicker(m.progressInterval)
	go m.reportProgress(progress, stopProgress, ticker.C)

	wg.Wait()
	close(stopProgress)
	ticker.Stop()
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
		deleteQuery := buildMigrationDeleteQuery(tsField, fromTime, cutoffTime)
		deleteBytes, _ := json.Marshal(deleteQuery)
		if err := m.hot.DeleteByQuery(ctx, index, deleteBytes); err != nil {
			return fmt.Errorf("deleting migrated documents: %w", err)
		}
	}

	// Mark checkpoint as completed and save watermark for next incremental run.
	m.checkpoint.MarkComplete(index)
	if err := m.checkpoint.SaveWatermark(&Watermark{
		Index:          index,
		MigratedBefore: cutoffTime,
	}); err != nil {
		slog.Warn("failed to save watermark", "index", index, "error", err)
	}

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
func (m *Migrator) migrateSlice(ctx context.Context, index string, queryBytes []byte, sliceID, sliceMax int, progress *Progress, cp *Checkpoint, cpMu *sync.Mutex) error {
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
	cpMu.Lock()
	if !cp.IsSliceDone(sliceID) {
		cp.SlicesDone = append(cp.SlicesDone, sliceID)
	}
	cp.Migrated += int64(sliceMigrated)
	saveErr := m.checkpoint.Save(cp)
	cpMu.Unlock()
	if saveErr != nil {
		return fmt.Errorf("saving checkpoint: %w", saveErr)
	}

	slog.Info("slice worker completed", "index", index, "slice", sliceID, "migrated", sliceMigrated)
	return nil
}

func (m *Migrator) reportProgress(progress *Progress, stop <-chan struct{}, tick <-chan time.Time) {
	for {
		select {
		case <-stop:
			return
		case <-tick:
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

// buildMigrationQuery builds a scroll query for the incremental time window.
// If fromTime is nil, it migrates all data older than cutoff (first run).
// Otherwise it migrates data in [fromTime, cutoff).
func buildMigrationQuery(tsField string, fromTime *time.Time, cutoff time.Time, size int) map[string]interface{} {
	rangeClause := map[string]interface{}{
		"lt": cutoff.Format(time.RFC3339),
	}
	if fromTime != nil {
		rangeClause["gte"] = fromTime.Format(time.RFC3339)
	}
	return map[string]interface{}{
		"size": size,
		"sort": []map[string]string{
			{tsField: "asc"},
		},
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				tsField: rangeClause,
			},
		},
	}
}

// buildMigrationDeleteQuery builds a delete-by-query for the same time window.
func buildMigrationDeleteQuery(tsField string, fromTime *time.Time, cutoff time.Time) map[string]interface{} {
	rangeClause := map[string]interface{}{
		"lt": cutoff.Format(time.RFC3339),
	}
	if fromTime != nil {
		rangeClause["gte"] = fromTime.Format(time.RFC3339)
	}
	return map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				tsField: rangeClause,
			},
		},
	}
}

func watermarkStr(wm *Watermark) string {
	if wm == nil || wm.MigratedBefore.IsZero() {
		return "none (first run)"
	}
	return wm.MigratedBefore.Format(time.RFC3339)
}

// ensureQuickwitIndex checks if the index exists in Quickwit and creates it if not.
func (m *Migrator) ensureQuickwitIndex(ctx context.Context, index, tsField string) error {
	exists, err := m.cold.IndexExists(ctx, index)
	if err != nil {
		return fmt.Errorf("checking index existence: %w", err)
	}
	if exists {
		slog.Info("quickwit index already exists", "index", index)
		return nil
	}
	slog.Info("creating quickwit index", "index", index, "timestamp_field", tsField)
	if err := m.cold.CreateIndex(ctx, index, tsField); err != nil {
		return fmt.Errorf("creating index: %w", err)
	}
	return nil
}
