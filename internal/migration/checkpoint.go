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
}

// CheckpointStore manages checkpoint persistence.
type CheckpointStore struct {
	dir string
}

// NewCheckpointStore creates a new checkpoint store in the given directory.
func NewCheckpointStore(dir string) (*CheckpointStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating checkpoint dir %s: %w", dir, err)
	}
	return &CheckpointStore{dir: dir}, nil
}

func (s *CheckpointStore) path(index string) string {
	// Replace slashes and wildcards for safe filenames.
	safe := filepath.Base(index)
	return filepath.Join(s.dir, safe+".checkpoint.json")
}

// Load reads a checkpoint for the given index. Returns nil if no checkpoint exists.
func (s *CheckpointStore) Load(index string) (*Checkpoint, error) {
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
func (s *CheckpointStore) Save(cp *Checkpoint) error {
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
func (s *CheckpointStore) MarkComplete(index string) error {
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
