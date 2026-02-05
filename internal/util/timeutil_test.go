package util

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExtractTimeRange_RangeInBoolFilter(t *testing.T) {
	body := `{
		"query": {
			"bool": {
				"filter": [
					{
						"range": {
							"@timestamp": {
								"gte": "2025-01-01T00:00:00Z",
								"lte": "2025-01-31T23:59:59Z"
							}
						}
					}
				]
			}
		}
	}`

	tr := ExtractTimeRange([]byte(body), "@timestamp")
	if tr == nil {
		t.Fatal("expected time range, got nil")
	}

	expectedFrom := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	expectedTo := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)

	if !tr.From.Equal(expectedFrom) {
		t.Errorf("From = %v, want %v", tr.From, expectedFrom)
	}
	if !tr.To.Equal(expectedTo) {
		t.Errorf("To = %v, want %v", tr.To, expectedTo)
	}
}

func TestExtractTimeRange_RangeInBoolMust(t *testing.T) {
	body := `{
		"query": {
			"bool": {
				"must": [
					{
						"range": {
							"created_at": {
								"gte": "2025-06-01",
								"lt": "2025-07-01"
							}
						}
					}
				]
			}
		}
	}`

	tr := ExtractTimeRange([]byte(body), "created_at")
	if tr == nil {
		t.Fatal("expected time range, got nil")
	}

	if tr.From == nil {
		t.Fatal("expected From, got nil")
	}
	if tr.From.Year() != 2025 || tr.From.Month() != 6 || tr.From.Day() != 1 {
		t.Errorf("From = %v, want 2025-06-01", tr.From)
	}
}

func TestExtractTimeRange_TopLevelRange(t *testing.T) {
	body := `{
		"query": {
			"range": {
				"@timestamp": {
					"gte": "2025-03-01T00:00:00Z"
				}
			}
		}
	}`

	tr := ExtractTimeRange([]byte(body), "@timestamp")
	if tr == nil {
		t.Fatal("expected time range, got nil")
	}
	if tr.From == nil {
		t.Fatal("expected From, got nil")
	}
	if tr.To != nil {
		t.Errorf("expected To to be nil, got %v", tr.To)
	}
}

func TestExtractTimeRange_EpochMillis(t *testing.T) {
	// 2025-01-15T00:00:00Z in epoch millis
	epochMs := float64(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC).UnixMilli())

	bodyMap := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"@timestamp": map[string]interface{}{
					"gte": epochMs,
				},
			},
		},
	}
	body, _ := json.Marshal(bodyMap)

	tr := ExtractTimeRange(body, "@timestamp")
	if tr == nil {
		t.Fatal("expected time range, got nil")
	}
	if tr.From == nil {
		t.Fatal("expected From, got nil")
	}

	expected := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	if !tr.From.Equal(expected) {
		t.Errorf("From = %v, want %v", tr.From, expected)
	}
}

func TestExtractTimeRange_NoQuery(t *testing.T) {
	body := `{"size": 10}`
	tr := ExtractTimeRange([]byte(body), "@timestamp")
	if tr != nil {
		t.Errorf("expected nil, got %v", tr)
	}
}

func TestExtractTimeRange_WrongField(t *testing.T) {
	body := `{
		"query": {
			"range": {
				"other_field": {
					"gte": "2025-01-01"
				}
			}
		}
	}`
	tr := ExtractTimeRange([]byte(body), "@timestamp")
	if tr != nil {
		t.Errorf("expected nil for wrong field, got %v", tr)
	}
}

func TestExtractTimeRange_InvalidJSON(t *testing.T) {
	tr := ExtractTimeRange([]byte("not json"), "@timestamp")
	if tr != nil {
		t.Errorf("expected nil for invalid JSON, got %v", tr)
	}
}

func TestExtractTimeRange_NowDateMath(t *testing.T) {
	start := time.Now().UTC()
	body := `{
		"query": {
			"range": {
				"@timestamp": {
					"gte": "now-7d",
					"lte": "now"
				}
			}
		}
	}`

	tr := ExtractTimeRange([]byte(body), "@timestamp")
	end := time.Now().UTC()

	if tr == nil || tr.From == nil || tr.To == nil {
		t.Fatalf("expected From/To, got %+v", tr)
	}

	// "now" should be within the test execution window.
	if tr.To.Before(start) || tr.To.After(end) {
		t.Fatalf("To=%v not within [%v, %v]", tr.To, start, end)
	}

	// "now-7d" should be within the shifted execution window.
	startFrom := start.AddDate(0, 0, -7)
	endFrom := end.AddDate(0, 0, -7)
	if tr.From.Before(startFrom) || tr.From.After(endFrom) {
		t.Fatalf("From=%v not within [%v, %v]", tr.From, startFrom, endFrom)
	}
}

func TestExtractTimeRange_NowDateMath_WithRoundingSuffix(t *testing.T) {
	start := time.Now().UTC()
	body := `{
		"query": {
			"range": {
				"@timestamp": {
					"gte": "now-1d||/d"
				}
			}
		}
	}`

	tr := ExtractTimeRange([]byte(body), "@timestamp")
	end := time.Now().UTC()

	if tr == nil || tr.From == nil {
		t.Fatalf("expected From, got %+v", tr)
	}

	startFrom := start.AddDate(0, 0, -1)
	endFrom := end.AddDate(0, 0, -1)
	if tr.From.Before(startFrom) || tr.From.After(endFrom) {
		t.Fatalf("From=%v not within [%v, %v]", tr.From, startFrom, endFrom)
	}
}
