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

func TestParseIndexDate(t *testing.T) {
	tests := []struct {
		index string
		want  string
		ok    bool
	}{
		{"dev-c3j1-syslog-2026.02.08", "2026-02-08", true},
		{"logs-2025-01-01", "2025-01-01", true},
		{"logs-2025.01.01", "2025-01-01", true},
		{"myindex-2024.12.31", "2024-12-31", true},
		{"logs", "", false},
		{"logs-v2", "", false},
		{"", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.index, func(t *testing.T) {
			got, ok := parseIndexDate(tc.index)
			if ok != tc.ok {
				t.Fatalf("parseIndexDate(%q) ok=%v, want %v", tc.index, ok, tc.ok)
			}
			if ok && got.Format("2006-01-02") != tc.want {
				t.Fatalf("parseIndexDate(%q) = %s, want %s", tc.index, got.Format("2006-01-02"), tc.want)
			}
		})
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
