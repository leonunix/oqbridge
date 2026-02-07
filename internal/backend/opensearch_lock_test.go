package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestOpenSearchLock_Acquire_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			// cleanupExpired: no lock exists.
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut && r.URL.Query().Get("op_type") == "create":
			// Lock creation succeeds.
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "", "", srv.Client())
	ok, err := lock.Acquire(context.Background(), "logs-2025-01-01", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected lock to be acquired")
	}
}

func TestOpenSearchLock_Acquire_AlreadyHeld(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			// cleanupExpired: lock exists but not expired.
			w.Header().Set("Content-Type", "application/json")
			doc := map[string]interface{}{
				"found":         true,
				"_seq_no":       1,
				"_primary_term": 1,
				"_source": map[string]interface{}{
					"owner":       "other-host-999",
					"acquired_at": time.Now().UTC().Format(time.RFC3339),
					"expires_at":  time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
				},
			}
			json.NewEncoder(w).Encode(doc)
		case r.Method == http.MethodPut:
			// Lock creation fails: conflict.
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error":"version_conflict"}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "", "", srv.Client())
	ok, err := lock.Acquire(context.Background(), "logs-2025-01-01", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if ok {
		t.Fatal("expected lock NOT to be acquired (already held)")
	}
}

func TestOpenSearchLock_Acquire_ExpiredLockCleanup(t *testing.T) {
	var mu sync.Mutex
	deleteCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case r.Method == http.MethodGet:
			// Lock exists but expired.
			w.Header().Set("Content-Type", "application/json")
			doc := map[string]interface{}{
				"found":         true,
				"_seq_no":       5,
				"_primary_term": 1,
				"_source": map[string]interface{}{
					"owner":       "crashed-host-123",
					"acquired_at": time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339),
					"expires_at":  time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339),
				},
			}
			json.NewEncoder(w).Encode(doc)
		case r.Method == http.MethodDelete:
			// Expired lock cleanup.
			deleteCalled = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			// Lock creation succeeds after cleanup.
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "", "", srv.Client())
	ok, err := lock.Acquire(context.Background(), "logs-2025-01-01", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected lock to be acquired after cleanup")
	}

	mu.Lock()
	if !deleteCalled {
		t.Fatal("expected expired lock to be deleted")
	}
	mu.Unlock()
}

func TestOpenSearchLock_Release(t *testing.T) {
	deleteCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteCalled = true
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "", "", srv.Client())
	if err := lock.Release(context.Background(), "logs-2025-01-01"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !deleteCalled {
		t.Fatal("expected DELETE to be called")
	}
}

func TestOpenSearchLock_Release_NotFound_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "", "", srv.Client())
	if err := lock.Release(context.Background(), "logs-2025-01-01"); err != nil {
		t.Fatalf("Release should not error on 404: %v", err)
	}
}

func TestOpenSearchLock_Acquire_IndexMissing_AutoCreates(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			// cleanupExpired: index doesn't exist.
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut && r.URL.Query().Get("op_type") == "create":
			callCount++
			if callCount == 1 {
				// First attempt: index doesn't exist.
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// Second attempt: after index creation, succeeds.
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
		case r.Method == http.MethodPut && r.URL.Query().Get("op_type") == "":
			// ensureIndex: create index.
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s %s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "", "", srv.Client())
	ok, err := lock.Acquire(context.Background(), "logs", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected lock to be acquired after index auto-creation")
	}
	if callCount != 2 {
		t.Fatalf("expected 2 create attempts, got %d", callCount)
	}
}

func TestOpenSearchLock_Auth(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		gotUser = u
		gotPass = p
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	lock := NewOpenSearchLock(srv.URL, "admin", "secret", srv.Client())
	lock.Acquire(context.Background(), "test-index", time.Hour)

	if gotUser != "admin" || gotPass != "secret" {
		t.Fatalf("auth: got %s/%s, want admin/secret", gotUser, gotPass)
	}
}
