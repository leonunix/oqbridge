package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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
	checkpoint       CheckpointStore
	lock             DistLock // optional distributed lock to prevent multi-instance duplication
	lockTTL          time.Duration
	progressInterval time.Duration
}

// MigratorOption configures optional Migrator behavior.
type MigratorOption func(*Migrator)

// WithDistLock enables distributed locking to prevent multiple instances
// from migrating the same index concurrently.
func WithDistLock(lock DistLock) MigratorOption {
	return func(m *Migrator) {
		m.lock = lock
	}
}

// WithLockTTL sets the TTL for distributed locks. Defaults to 2 hours.
func WithLockTTL(ttl time.Duration) MigratorOption {
	return func(m *Migrator) {
		m.lockTTL = ttl
	}
}

// WithCheckpointStore overrides the default local filesystem checkpoint store.
// Use this with OpenSearchCheckpointStore for multi-instance deployments where
// checkpoint and watermark state must be shared across instances.
func WithCheckpointStore(store CheckpointStore) MigratorOption {
	return func(m *Migrator) {
		m.checkpoint = store
	}
}

// NewMigrator creates a new Migrator.
func NewMigrator(cfg *config.Config, hot HotClient, cold ColdClient, opts ...MigratorOption) (*Migrator, error) {
	m := &Migrator{
		cfg:              cfg,
		hot:              hot,
		cold:             cold,
		lockTTL:          2 * time.Hour,
		progressInterval: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(m)
	}
	// Fall back to local filesystem checkpoint store if no override was provided.
	if m.checkpoint == nil {
		cpStore, err := NewLocalCheckpointStore(cfg.Migration.CheckpointDir)
		if err != nil {
			return nil, fmt.Errorf("initializing checkpoint store: %w", err)
		}
		m.checkpoint = cpStore
	}
	return m, nil
}

// MigrateAll migrates all configured indices.
// Wildcard patterns (e.g., "logs-*", "*") are resolved to concrete index
// names via the OpenSearch _cat/indices API before migration.
func (m *Migrator) MigrateAll(ctx context.Context) error {
	patterns := m.cfg.Migration.Indices
	if len(patterns) == 0 {
		slog.Info("no indices configured for migration, skipping")
		return nil
	}

	for _, pattern := range patterns {
		concrete, err := m.resolvePattern(ctx, pattern)
		if err != nil {
			slog.Error("failed to resolve index pattern", "pattern", pattern, "error", err)
			continue
		}
		for _, index := range concrete {
			if err := m.MigrateIndex(ctx, index); err != nil {
				slog.Error("migration failed for index", "index", index, "error", err)
				continue
			}
		}
	}
	return nil
}

// resolvePattern expands a wildcard pattern to concrete index names.
// If the pattern contains no wildcards, it is returned as-is.
func (m *Migrator) resolvePattern(ctx context.Context, pattern string) ([]string, error) {
	if !containsWildcard(pattern) {
		return []string{pattern}, nil
	}
	resolved, err := m.hot.ResolveIndices(ctx, pattern)
	if err != nil {
		return nil, fmt.Errorf("resolving pattern %q: %w", pattern, err)
	}
	slog.Info("resolved index pattern", "pattern", pattern, "count", len(resolved))
	return resolved, nil
}

func containsWildcard(s string) bool {
	return strings.ContainsAny(s, "*?[]")
}

// MigrateIndex migrates documents older than the retention threshold from
// OpenSearch to Quickwit using parallel sliced scroll workers.
func (m *Migrator) MigrateIndex(ctx context.Context, index string) error {
	// Acquire distributed lock if configured, preventing multiple instances
	// from migrating the same index concurrently.
	if m.lock != nil {
		acquired, err := m.lock.Acquire(ctx, index, m.lockTTL)
		if err != nil {
			return fmt.Errorf("acquiring migration lock for %s: %w", index, err)
		}
		if !acquired {
			slog.Info("skipping index, migration lock held by another instance", "index", index)
			return nil
		}
		defer func() {
			if err := m.lock.Release(ctx, index); err != nil {
				slog.Warn("failed to release migration lock", "index", index, "error", err)
			}
		}()
	}

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
	coldDays := m.cfg.Retention.ColdDays
	slog.Info("creating quickwit index", "index", index, "timestamp_field", tsField, "retention_days", coldDays)
	if err := m.cold.CreateIndex(ctx, index, tsField, coldDays); err != nil {
		return fmt.Errorf("creating index: %w", err)
	}
	return nil
}
