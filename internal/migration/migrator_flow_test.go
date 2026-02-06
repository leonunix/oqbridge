package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
)

type fakeHot struct {
	mu sync.Mutex

	// pages[sliceID] is a list of pages; each page is a list of hits.
	pages map[int][][]json.RawMessage

	// pos[sliceID] is the next page to return.
	pos map[int]int

	// allowStart, if present, blocks the initial scroll for a slice until closed.
	allowStart map[int]<-chan struct{}

	// record whether a slice was requested.
	requested map[int]bool

	// resolvedIndices maps pattern → concrete index names for ResolveIndices.
	resolvedIndices map[string][]string
}

func newFakeHot(pages map[int][][]json.RawMessage) *fakeHot {
	return &fakeHot{
		pages:      pages,
		pos:        make(map[int]int),
		allowStart: make(map[int]<-chan struct{}),
		requested:  make(map[int]bool),
	}
}

func (f *fakeHot) SlicedScroll(_ context.Context, _ string, body []byte, scrollID string, slice *backend.SlicedScrollConfig) (*backend.ScrollResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if scrollID == "" {
		if slice == nil {
			return nil, fmt.Errorf("missing slice config")
		}
		f.requested[slice.SliceID] = true
		if ch := f.allowStart[slice.SliceID]; ch != nil {
			f.mu.Unlock()
			<-ch
			f.mu.Lock()
		}
	}

	var sliceID int
	if scrollID == "" {
		sliceID = slice.SliceID
	} else {
		_, err := fmt.Sscanf(scrollID, "sid-%d", &sliceID)
		if err != nil {
			return nil, fmt.Errorf("bad scrollID: %q", scrollID)
		}
	}

	pages := f.pages[sliceID]
	i := f.pos[sliceID]
	if i >= len(pages) {
		return &backend.ScrollResult{ScrollID: fmt.Sprintf("sid-%d", sliceID), Hits: nil, Total: 0}, nil
	}
	f.pos[sliceID] = i + 1
	return &backend.ScrollResult{
		ScrollID: fmt.Sprintf("sid-%d", sliceID),
		Hits:     pages[i],
		Total:    len(pages[i]),
	}, nil
}

func (f *fakeHot) ClearScroll(_ context.Context, _ string) error             { return nil }
func (f *fakeHot) DeleteByQuery(_ context.Context, _ string, _ []byte) error { return nil }

// resolvedIndices maps pattern → concrete index names for ResolveIndices.
// If nil, defaults to returning []string{"logs"}.
func (f *fakeHot) ResolveIndices(_ context.Context, pattern string) ([]string, error) {
	if f.resolvedIndices != nil {
		if v, ok := f.resolvedIndices[pattern]; ok {
			return v, nil
		}
		return nil, nil
	}
	return []string{"logs"}, nil
}

type fakeCold struct {
	mu sync.Mutex

	docsByIndex map[string][]json.RawMessage
	failOnSlice *int
	onIngest    func(index string, docs []json.RawMessage)
}

func newFakeCold() *fakeCold {
	return &fakeCold{docsByIndex: make(map[string][]json.RawMessage)}
}

func (f *fakeCold) BulkIngest(_ context.Context, index string, docs []json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.onIngest != nil {
		f.onIngest(index, docs)
	}

	if f.failOnSlice != nil {
		for _, d := range docs {
			var m map[string]any
			if json.Unmarshal(d, &m) == nil {
				if v, ok := m["slice"].(float64); ok && int(v) == *f.failOnSlice {
					return errors.New("ingest failed")
				}
			}
		}
	}

	f.docsByIndex[index] = append(f.docsByIndex[index], docs...)
	return nil
}

func (f *fakeCold) IndexExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (f *fakeCold) CreateIndex(_ context.Context, _ string, _ string, _ int) error {
	return nil
}

func makeHits(sliceID int, n int) []json.RawMessage {
	hits := make([]json.RawMessage, 0, n)
	for i := 0; i < n; i++ {
		hits = append(hits, json.RawMessage(fmt.Sprintf(`{"_source":{"slice":%d,"n":%d}}`, sliceID, i)))
	}
	return hits
}

func newTestMigrator(t *testing.T, hot HotClient, cold ColdClient, checkpointDir string) *Migrator {
	t.Helper()
	cfg := &config.Config{
		Retention: config.RetentionConfig{Days: 30, TimestampField: "@timestamp"},
		Migration: config.MigrationConfig{
			Schedule:      "* * * * *",
			BatchSize:     2,
			Workers:       2,
			CheckpointDir: checkpointDir,
			Indices:       []string{"logs"},
		},
	}
	m, err := NewMigrator(cfg, hot, cold)
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	m.progressInterval = time.Millisecond
	return m
}

func readCheckpoint(t *testing.T, dir, index string) *Checkpoint {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, filepath.Base(index)+".checkpoint.json"))
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	return &cp
}

func TestMigrator_MigrateIndex_Success_MultipleSlices(t *testing.T) {
	dir := t.TempDir()
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 2), nil},
		1: {makeHits(1, 1), makeHits(1, 1), nil},
	})
	cold := newFakeCold()

	m := newTestMigrator(t, hot, cold, dir)

	if err := m.MigrateIndex(context.Background(), "logs"); err != nil {
		t.Fatalf("MigrateIndex: %v", err)
	}

	cold.mu.Lock()
	gotDocs := len(cold.docsByIndex["logs"])
	cold.mu.Unlock()
	if gotDocs != 4 {
		t.Fatalf("ingested docs=%d, want %d", gotDocs, 4)
	}

	cp := readCheckpoint(t, dir, "logs")
	if !cp.Completed {
		t.Fatalf("Completed=false, want true")
	}
	if cp.Migrated != 4 {
		t.Fatalf("Migrated=%d, want %d", cp.Migrated, 4)
	}
	sort.Ints(cp.SlicesDone)
	if len(cp.SlicesDone) != 2 || cp.SlicesDone[0] != 0 || cp.SlicesDone[1] != 1 {
		t.Fatalf("SlicesDone=%v, want [0 1]", cp.SlicesDone)
	}
}

func TestMigrator_MigrateIndex_Resume_SkipsCompletedSlice(t *testing.T) {
	dir := t.TempDir()

	// Pre-create a checkpoint where slice 0 is done.
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	if err := store.Save(&Checkpoint{Index: "logs", SlicesDone: []int{0}}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	hot := newFakeHot(map[int][][]json.RawMessage{
		1: {makeHits(1, 1), nil},
	})
	cold := newFakeCold()

	m := newTestMigrator(t, hot, cold, dir)

	if err := m.MigrateIndex(context.Background(), "logs"); err != nil {
		t.Fatalf("MigrateIndex: %v", err)
	}

	hot.mu.Lock()
	if hot.requested[0] {
		t.Fatalf("slice 0 was requested but should have been skipped")
	}
	if !hot.requested[1] {
		t.Fatalf("slice 1 was not requested")
	}
	hot.mu.Unlock()
}

func TestMigrator_MigrateIndex_PartialFailure_SavesCheckpoint(t *testing.T) {
	dir := t.TempDir()

	slice0CanStart := make(chan struct{})
	close(slice0CanStart)
	slice1CanStart := make(chan struct{})

	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	hot.allowStart[0] = slice0CanStart
	hot.allowStart[1] = slice1CanStart

	cold := newFakeCold()
	failSlice := 1
	cold.failOnSlice = &failSlice
	slice0Ingested := make(chan struct{})
	cold.onIngest = func(_ string, docs []json.RawMessage) {
		for _, d := range docs {
			var m map[string]any
			if json.Unmarshal(d, &m) == nil {
				if v, ok := m["slice"].(float64); ok && int(v) == 0 {
					select {
					case <-slice0Ingested:
					default:
						close(slice0Ingested)
					}
					return
				}
			}
		}
	}

	m := newTestMigrator(t, hot, cold, dir)

	// Allow slice 0 to finish first, then start slice 1 which will fail ingest.
	go func() {
		<-slice0Ingested
		close(slice1CanStart)
	}()

	if err := m.MigrateIndex(context.Background(), "logs"); err == nil {
		t.Fatalf("expected error")
	}

	cp := readCheckpoint(t, dir, "logs")
	if cp.Completed {
		t.Fatalf("Completed=true, want false")
	}
	// At least slice 0 should be persisted as done.
	if !cp.IsSliceDone(0) {
		t.Fatalf("expected slice 0 done in checkpoint, got %v", cp.SlicesDone)
	}
}

func TestMigrator_reportProgress_TickAndStop(t *testing.T) {
	m := &Migrator{}
	progress := &Progress{Index: "logs", StartTime: time.Now()}
	stop := make(chan struct{})
	tick := make(chan time.Time, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		m.reportProgress(progress, stop, tick)
	}()

	tick <- time.Now()
	close(stop)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("reportProgress did not stop")
	}
}

func TestMigrator_MigrateAll_WildcardResolution(t *testing.T) {
	dir := t.TempDir()

	// One concrete index resolved from "logs-*".
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	hot.resolvedIndices = map[string][]string{
		"logs-*": {"logs-2025-01-01"},
	}
	cold := newFakeCold()

	cfg := &config.Config{
		Retention: config.RetentionConfig{Days: 30, TimestampField: "@timestamp"},
		Migration: config.MigrationConfig{
			Schedule:      "* * * * *",
			BatchSize:     2,
			Workers:       2,
			CheckpointDir: dir,
			Indices:       []string{"logs-*"},
		},
	}
	m, err := NewMigrator(cfg, hot, cold)
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	m.progressInterval = time.Millisecond

	if err := m.MigrateAll(context.Background()); err != nil {
		t.Fatalf("MigrateAll: %v", err)
	}

	cold.mu.Lock()
	// Docs should be ingested under the concrete index name, not the wildcard.
	if _, ok := cold.docsByIndex["logs-*"]; ok {
		t.Fatalf("docs should NOT be under wildcard pattern 'logs-*'")
	}
	if docs, ok := cold.docsByIndex["logs-2025-01-01"]; !ok || len(docs) == 0 {
		t.Fatalf("expected docs under 'logs-2025-01-01', got %v", cold.docsByIndex)
	}
	cold.mu.Unlock()
}
