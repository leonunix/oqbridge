package migration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestOpenSearchCheckpointStore_SaveAndLoad(t *testing.T) {
	var mu sync.Mutex
	docs := make(map[string][]byte)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		// Extract doc ID from URL path: /.oqbridge-state/_doc/{id}
		// The path looks like "/.oqbridge-state/_doc/checkpoint-logs"
		id := r.URL.Path[len("/.oqbridge-state/_doc/"):]

		switch r.Method {
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			docs[id] = body
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
		case http.MethodGet:
			if data, ok := docs[id]; ok {
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"found":   true,
					"_source": json.RawMessage(data),
				}
				json.NewEncoder(w).Encode(resp)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	store := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())

	// Save a checkpoint.
	cp := &Checkpoint{
		Index:      "logs-2025-01-01",
		TotalDocs:  100,
		Migrated:   50,
		SlicesDone: []int{0, 1},
	}
	if err := store.Save(cp); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Load the checkpoint.
	loaded, err := store.Load("logs-2025-01-01")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint, got nil")
	}
	if loaded.Index != "logs-2025-01-01" {
		t.Fatalf("Index=%s, want logs-2025-01-01", loaded.Index)
	}
	if loaded.TotalDocs != 100 || loaded.Migrated != 50 {
		t.Fatalf("TotalDocs=%d Migrated=%d, want 100/50", loaded.TotalDocs, loaded.Migrated)
	}
	if !loaded.IsSliceDone(0) || !loaded.IsSliceDone(1) || loaded.IsSliceDone(2) {
		t.Fatalf("SlicesDone=%v, unexpected", loaded.SlicesDone)
	}
}

func TestOpenSearchCheckpointStore_MarkComplete(t *testing.T) {
	var mu sync.Mutex
	docs := make(map[string][]byte)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		id := r.URL.Path[len("/.oqbridge-state/_doc/"):]

		switch r.Method {
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			docs[id] = body
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
		case http.MethodGet:
			if data, ok := docs[id]; ok {
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"found":   true,
					"_source": json.RawMessage(data),
				}
				json.NewEncoder(w).Encode(resp)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	store := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())

	// Save a checkpoint first.
	cp := &Checkpoint{Index: "logs", Migrated: 10, SlicesDone: []int{0}}
	if err := store.Save(cp); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Mark it complete.
	if err := store.MarkComplete("logs"); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}

	// Load should return nil (completed runs start fresh).
	loaded, err := store.Load("logs")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded != nil {
		t.Fatalf("expected nil after MarkComplete, got %+v", loaded)
	}
}

func TestOpenSearchCheckpointStore_LoadNonExistent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	store := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())
	cp, err := store.Load("nonexistent")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp != nil {
		t.Fatalf("expected nil, got %+v", cp)
	}
}

func TestOpenSearchCheckpointStore_SaveAndLoadWatermark(t *testing.T) {
	var mu sync.Mutex
	docs := make(map[string][]byte)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		id := r.URL.Path[len("/.oqbridge-state/_doc/"):]

		switch r.Method {
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			docs[id] = body
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			if data, ok := docs[id]; ok {
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"found":   true,
					"_source": json.RawMessage(data),
				}
				json.NewEncoder(w).Encode(resp)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	store := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())

	cutoff := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	wm := &Watermark{
		Index:          "logs",
		MigratedBefore: cutoff,
	}
	if err := store.SaveWatermark(wm); err != nil {
		t.Fatalf("SaveWatermark: %v", err)
	}

	loaded, err := store.LoadWatermark("logs")
	if err != nil {
		t.Fatalf("LoadWatermark: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected watermark, got nil")
	}
	if !loaded.MigratedBefore.Equal(cutoff) {
		t.Fatalf("MigratedBefore=%v, want %v", loaded.MigratedBefore, cutoff)
	}
}

func TestOpenSearchCheckpointStore_LoadWatermarkNonExistent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	store := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())
	wm, err := store.LoadWatermark("nonexistent")
	if err != nil {
		t.Fatalf("LoadWatermark: %v", err)
	}
	if wm != nil {
		t.Fatalf("expected nil, got %+v", wm)
	}
}

func TestOpenSearchCheckpointStore_PutDoc_IndexAutoCreate(t *testing.T) {
	callCount := 0
	indexCreated := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/.oqbridge-state":
			// Create index.
			indexCreated = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			callCount++
			if callCount == 1 && !indexCreated {
				// First put: index doesn't exist.
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	store := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())
	wm := &Watermark{Index: "logs", MigratedBefore: time.Now()}
	if err := store.SaveWatermark(wm); err != nil {
		t.Fatalf("SaveWatermark: %v", err)
	}
	if !indexCreated {
		t.Fatal("expected index to be created")
	}
	if callCount != 2 {
		t.Fatalf("expected 2 put attempts, got %d", callCount)
	}
}

func TestOpenSearchCheckpointStore_SharedBetweenInstances(t *testing.T) {
	// Simulates two instances sharing the same checkpoint store.
	var mu sync.Mutex
	docs := make(map[string][]byte)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		id := r.URL.Path[len("/.oqbridge-state/_doc/"):]

		switch r.Method {
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			docs[id] = body
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			if data, ok := docs[id]; ok {
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"found":   true,
					"_source": json.RawMessage(data),
				}
				json.NewEncoder(w).Encode(resp)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	// Instance 1 saves a watermark.
	store1 := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())
	cutoff := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	if err := store1.SaveWatermark(&Watermark{Index: "logs", MigratedBefore: cutoff}); err != nil {
		t.Fatalf("SaveWatermark: %v", err)
	}

	// Instance 2 can read the same watermark.
	store2 := NewOpenSearchCheckpointStore(srv.URL, "", "", srv.Client())
	wm, err := store2.LoadWatermark("logs")
	if err != nil {
		t.Fatalf("LoadWatermark: %v", err)
	}
	if wm == nil {
		t.Fatal("instance 2 should see the watermark saved by instance 1")
	}
	if !wm.MigratedBefore.Equal(cutoff) {
		t.Fatalf("MigratedBefore=%v, want %v", wm.MigratedBefore, cutoff)
	}
}

// Verify compile-time interface compliance.
var _ CheckpointStore = (*OpenSearchCheckpointStore)(nil)
var _ CheckpointStore = (*LocalCheckpointStore)(nil)
