package migration

import (
	"testing"
	"time"
)

func TestBuildMigrationQuery_FirstRun(t *testing.T) {
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := buildMigrationQuery("@timestamp", nil, cutoff, 5000)

	if q["size"] != 5000 {
		t.Fatalf("size=%v, want %v", q["size"], 5000)
	}

	sortAny, ok := q["sort"].([]map[string]string)
	if !ok || len(sortAny) != 1 || sortAny[0]["@timestamp"] != "asc" {
		t.Fatalf("sort=%v, want [{@timestamp:asc}]", q["sort"])
	}

	fieldAny := extractRangeField(t, q, "@timestamp")
	if fieldAny["lt"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("lt=%v, want 2026-01-01T00:00:00Z", fieldAny["lt"])
	}
	if _, exists := fieldAny["gte"]; exists {
		t.Fatalf("gte should not be set on first run, got %v", fieldAny["gte"])
	}
}

func TestBuildMigrationQuery_IncrementalRun(t *testing.T) {
	from := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := buildMigrationQuery("@timestamp", &from, cutoff, 5000)

	fieldAny := extractRangeField(t, q, "@timestamp")
	if fieldAny["lt"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("lt=%v, want 2026-01-01T00:00:00Z", fieldAny["lt"])
	}
	if fieldAny["gte"] != "2025-12-31T00:00:00Z" {
		t.Fatalf("gte=%v, want 2025-12-31T00:00:00Z", fieldAny["gte"])
	}
}

func TestBuildMigrationDeleteQuery_FirstRun(t *testing.T) {
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := buildMigrationDeleteQuery("ts", nil, cutoff)

	fieldAny := extractRangeField(t, q, "ts")
	if fieldAny["lt"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("lt=%v, want 2026-01-01T00:00:00Z", fieldAny["lt"])
	}
	if _, exists := fieldAny["gte"]; exists {
		t.Fatalf("gte should not be set on first run")
	}
}

func TestBuildMigrationDeleteQuery_IncrementalRun(t *testing.T) {
	from := time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := buildMigrationDeleteQuery("ts", &from, cutoff)

	fieldAny := extractRangeField(t, q, "ts")
	if fieldAny["gte"] != "2025-12-25T00:00:00Z" {
		t.Fatalf("gte=%v, want 2025-12-25T00:00:00Z", fieldAny["gte"])
	}
}

func extractRangeField(t *testing.T, q map[string]interface{}, field string) map[string]interface{} {
	t.Helper()
	queryAny, ok := q["query"].(map[string]interface{})
	if !ok {
		t.Fatalf("query type=%T, want map", q["query"])
	}
	rangeAny, ok := queryAny["range"].(map[string]interface{})
	if !ok {
		t.Fatalf("range type=%T, want map", queryAny["range"])
	}
	fieldAny, ok := rangeAny[field].(map[string]interface{})
	if !ok {
		t.Fatalf("field type=%T, want map", rangeAny[field])
	}
	return fieldAny
}
