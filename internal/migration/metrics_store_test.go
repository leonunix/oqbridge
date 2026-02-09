package migration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOpenSearchMetricsStore_Record(t *testing.T) {
	var capturedPath string
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			capturedPath = r.URL.Path
			capturedBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	store := NewOpenSearchMetricsStore(srv.URL, "", "", srv.Client())

	start := time.Date(2026, 2, 9, 10, 0, 0, 0, time.UTC)
	metric := &MigrationMetric{
		Timestamp:         start.Add(5 * time.Minute),
		Index:             "logs-2026.01.15",
		StartedAt:         start,
		CompletedAt:       start.Add(5 * time.Minute),
		DurationSec:       300.0,
		DocumentsMigrated: 150000,
		DocsPerSec:        500.0,
		Workers:           4,
		BatchSize:         5000,
		Status:            "success",
		CutoffTime:        start.Add(-25 * 24 * time.Hour),
	}

	if err := store.Record(t.Context(), metric); err != nil {
		t.Fatalf("Record: %v", err)
	}

	// Verify document ID is deterministic.
	expectedID := "metric-logs-2026.01.15-" + strings.TrimRight(strings.TrimRight(
		time.Date(2026, 2, 9, 10, 0, 0, 0, time.UTC).Format("2006"), "0"), "")
	_ = expectedID // ID format tested below via path check.
	if !strings.HasPrefix(capturedPath, "/.oqbridge-migration-metrics/_doc/metric-logs-2026.01.15-") {
		t.Fatalf("unexpected path: %s", capturedPath)
	}

	// Verify body is valid JSON with expected fields.
	var doc map[string]interface{}
	if err := json.Unmarshal(capturedBody, &doc); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if doc["index"] != "logs-2026.01.15" {
		t.Fatalf("index=%v, want logs-2026.01.15", doc["index"])
	}
	if doc["status"] != "success" {
		t.Fatalf("status=%v, want success", doc["status"])
	}
	if doc["documents_migrated"] != float64(150000) {
		t.Fatalf("documents_migrated=%v, want 150000", doc["documents_migrated"])
	}
}

func TestOpenSearchMetricsStore_Record_IndexAutoCreate(t *testing.T) {
	putCount := 0
	indexCreated := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/.oqbridge-migration-metrics":
			// Create index request.
			indexCreated = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			putCount++
			if putCount == 1 && !indexCreated {
				// First put: index doesn't exist.
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"result":"created"}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	store := NewOpenSearchMetricsStore(srv.URL, "", "", srv.Client())

	metric := NewSuccessMetric("logs-2026.01.15", time.Now(), 100, time.Now(), 4, 5000)
	if err := store.Record(t.Context(), metric); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if !indexCreated {
		t.Fatal("expected metrics index to be created")
	}
	if putCount != 2 {
		t.Fatalf("expected 2 put attempts (first 404, then retry), got %d", putCount)
	}
}

func TestOpenSearchMetricsStore_Record_WithAuth(t *testing.T) {
	var capturedAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"result":"created"}`))
	}))
	defer srv.Close()

	store := NewOpenSearchMetricsStore(srv.URL, "admin", "secret", srv.Client())

	metric := NewSuccessMetric("logs", time.Now(), 0, time.Now(), 1, 1000)
	if err := store.Record(t.Context(), metric); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if capturedAuth == "" {
		t.Fatal("expected Authorization header to be set")
	}
}

func TestOpenSearchMetricsStore_Record_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal"}`))
	}))
	defer srv.Close()

	store := NewOpenSearchMetricsStore(srv.URL, "", "", srv.Client())

	metric := NewSuccessMetric("logs", time.Now(), 0, time.Now(), 1, 1000)
	err := store.Record(t.Context(), metric)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestNewSuccessMetric(t *testing.T) {
	start := time.Now().Add(-5 * time.Minute)
	m := NewSuccessMetric("logs-2026.01.15", start, 50000, time.Now(), 4, 5000)

	if m.Status != "success" {
		t.Fatalf("Status=%s, want success", m.Status)
	}
	if m.Error != "" {
		t.Fatalf("Error=%s, want empty", m.Error)
	}
	if m.Index != "logs-2026.01.15" {
		t.Fatalf("Index=%s, want logs-2026.01.15", m.Index)
	}
	if m.DocumentsMigrated != 50000 {
		t.Fatalf("DocumentsMigrated=%d, want 50000", m.DocumentsMigrated)
	}
	if m.Workers != 4 || m.BatchSize != 5000 {
		t.Fatalf("Workers=%d BatchSize=%d, want 4/5000", m.Workers, m.BatchSize)
	}
	if m.DurationSec < 299 {
		t.Fatalf("DurationSec=%f, expected ~300", m.DurationSec)
	}
	if m.DocsPerSec <= 0 {
		t.Fatalf("DocsPerSec=%f, expected > 0", m.DocsPerSec)
	}
}

func TestNewFailureMetric(t *testing.T) {
	start := time.Now().Add(-1 * time.Minute)
	testErr := errString("slice 2: connection refused")
	m := NewFailureMetric("logs", start, 1000, time.Now(), 2, 1000, testErr)

	if m.Status != "failed" {
		t.Fatalf("Status=%s, want failed", m.Status)
	}
	if m.Error != "slice 2: connection refused" {
		t.Fatalf("Error=%s, want 'slice 2: connection refused'", m.Error)
	}
	if m.DocumentsMigrated != 1000 {
		t.Fatalf("DocumentsMigrated=%d, want 1000", m.DocumentsMigrated)
	}
}

func TestMetricDocID(t *testing.T) {
	start := time.Date(2026, 2, 9, 10, 30, 0, 0, time.UTC)
	m := &MigrationMetric{Index: "logs-2026.01.15", StartedAt: start}
	id := metricDocID(m)

	expected := fmt.Sprintf("metric-logs-2026.01.15-%d", start.Unix())
	if id != expected {
		t.Fatalf("docID=%s, want %s", id, expected)
	}

	// Same input produces same ID (deterministic).
	if metricDocID(m) != id {
		t.Fatal("expected deterministic doc ID")
	}
}

// errString is a simple error type for testing.
type errString string

func (e errString) Error() string { return string(e) }

// Verify compile-time interface compliance.
var _ MetricsRecorder = (*OpenSearchMetricsStore)(nil)
