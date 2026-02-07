package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint tracks migration progress for an index, enabling resume after failure.
type Checkpoint struct {
	Index      string    `json:"index"`
	StartedAt  time.Time `json:"started_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	TotalDocs  int64     `json:"total_docs"`
	Migrated   int64     `json:"migrated"`
	SlicesDone []int     `json:"slices_done"`
	Completed  bool      `json:"completed"`
	// CutoffTime is the upper bound of the time range for this migration run
	// (i.e., now - migrate_after_days at the time the run started).
	CutoffTime time.Time `json:"cutoff_time,omitempty"`
}

// Watermark records the high-water mark for incremental migration.
// After a successful run, we persist the cutoff time so the next run
// only processes the delta (new data that crossed the threshold).
type Watermark struct {
	Index          string    `json:"index"`
	MigratedBefore time.Time `json:"migrated_before"` // Upper bound of last successful migration.
	UpdatedAt      time.Time `json:"updated_at"`
}

// CheckpointStore is the interface for checkpoint and watermark persistence.
// Implementations include LocalCheckpointStore (local filesystem) and
// OpenSearchCheckpointStore (shared via OpenSearch for multi-instance deployments).
type CheckpointStore interface {
	Load(index string) (*Checkpoint, error)
	Save(cp *Checkpoint) error
	MarkComplete(index string) error
	LoadWatermark(index string) (*Watermark, error)
	SaveWatermark(wm *Watermark) error
}

// LocalCheckpointStore manages checkpoint persistence on the local filesystem.
type LocalCheckpointStore struct {
	dir string
}

// NewLocalCheckpointStore creates a new local checkpoint store in the given directory.
func NewLocalCheckpointStore(dir string) (*LocalCheckpointStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating checkpoint dir %s: %w", dir, err)
	}
	return &LocalCheckpointStore{dir: dir}, nil
}

func (s *LocalCheckpointStore) path(index string) string {
	// Replace slashes and wildcards for safe filenames.
	safe := filepath.Base(index)
	return filepath.Join(s.dir, safe+".checkpoint.json")
}

// Load reads a checkpoint for the given index. Returns nil if no checkpoint exists.
func (s *LocalCheckpointStore) Load(index string) (*Checkpoint, error) {
	data, err := os.ReadFile(s.path(index))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parsing checkpoint: %w", err)
	}

	// If the previous run completed, return nil so we start fresh.
	if cp.Completed {
		return nil, nil
	}
	return &cp, nil
}

// Save persists the checkpoint to disk.
func (s *LocalCheckpointStore) Save(cp *Checkpoint) error {
	cp.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling checkpoint: %w", err)
	}
	if err := os.WriteFile(s.path(cp.Index), data, 0644); err != nil {
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	return nil
}

// MarkComplete marks the checkpoint as completed.
func (s *LocalCheckpointStore) MarkComplete(index string) error {
	cp, err := s.Load(index)
	if err != nil {
		return err
	}
	if cp == nil {
		cp = &Checkpoint{Index: index}
	}
	cp.Completed = true
	return s.Save(cp)
}

// IsSliceDone checks if a given slice has already been completed.
func (cp *Checkpoint) IsSliceDone(sliceID int) bool {
	if cp == nil {
		return false
	}
	for _, id := range cp.SlicesDone {
		if id == sliceID {
			return true
		}
	}
	return false
}

func (s *LocalCheckpointStore) watermarkPath(index string) string {
	safe := filepath.Base(index)
	return filepath.Join(s.dir, safe+".watermark.json")
}

// LoadWatermark reads the watermark for the given index.
// Returns nil if no watermark exists (first run).
func (s *LocalCheckpointStore) LoadWatermark(index string) (*Watermark, error) {
	data, err := os.ReadFile(s.watermarkPath(index))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading watermark: %w", err)
	}
	var wm Watermark
	if err := json.Unmarshal(data, &wm); err != nil {
		return nil, fmt.Errorf("parsing watermark: %w", err)
	}
	return &wm, nil
}

// SaveWatermark persists the watermark to disk after a successful migration.
func (s *LocalCheckpointStore) SaveWatermark(wm *Watermark) error {
	wm.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(wm, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling watermark: %w", err)
	}
	if err := os.WriteFile(s.watermarkPath(wm.Index), data, 0644); err != nil {
		return fmt.Errorf("writing watermark: %w", err)
	}
	return nil
}
