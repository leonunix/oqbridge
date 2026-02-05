package migration

import "testing"

func TestCheckpointStore_SaveLoadAndComplete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}

	cp := &Checkpoint{
		Index:      "logs-2025.01.01",
		TotalDocs:  123,
		Migrated:   45,
		SlicesDone: []int{0, 2},
	}
	if err := store.Save(cp); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := store.Load(cp.Index)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected checkpoint, got nil")
	}
	if loaded.Index != cp.Index || loaded.TotalDocs != 123 || loaded.Migrated != 45 {
		t.Fatalf("loaded checkpoint mismatch: %+v", loaded)
	}
	if !loaded.IsSliceDone(0) || loaded.IsSliceDone(1) || !loaded.IsSliceDone(2) {
		t.Fatalf("IsSliceDone unexpected: %+v", loaded.SlicesDone)
	}

	if err := store.MarkComplete(cp.Index); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	loaded2, err := store.Load(cp.Index)
	if err != nil {
		t.Fatalf("Load after complete: %v", err)
	}
	if loaded2 != nil {
		t.Fatalf("expected nil after completion, got %+v", loaded2)
	}
}

