package proxy

import (
	"fmt"
	"testing"
	"time"
)

func TestRouter_Route(t *testing.T) {
	router := NewRouter(30)

	now := time.Now().UTC()
	recentTime := now.Add(-24 * time.Hour).Format(time.RFC3339)        // 1 day ago
	oldTime := now.Add(-60 * 24 * time.Hour).Format(time.RFC3339)      // 60 days ago
	veryOldTime := now.Add(-90 * 24 * time.Hour).Format(time.RFC3339)  // 90 days ago
	boundaryTime := now.Add(-15 * 24 * time.Hour).Format(time.RFC3339) // 15 days ago

	tests := []struct {
		name     string
		body     string
		expected RouteTarget
	}{
		{
			name: "recent data only - hot",
			body: fmt.Sprintf(`{
				"query": {"range": {"@timestamp": {"gte": "%s", "lte": "%s"}}}
			}`, recentTime, now.Format(time.RFC3339)),
			expected: RouteHotOnly,
		},
		{
			name: "old data only - cold",
			body: fmt.Sprintf(`{
				"query": {"range": {"@timestamp": {"gte": "%s", "lte": "%s"}}}
			}`, veryOldTime, oldTime),
			expected: RouteColdOnly,
		},
		{
			name: "spanning boundary - both",
			body: fmt.Sprintf(`{
				"query": {"range": {"@timestamp": {"gte": "%s", "lte": "%s"}}}
			}`, oldTime, boundaryTime),
			expected: RouteBoth,
		},
		{
			name:     "no time range - both (safe default)",
			body:     `{"query": {"match_all": {}}}`,
			expected: RouteBoth,
		},
		{
			name:     "empty body - both",
			body:     `{}`,
			expected: RouteBoth,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := router.Route([]byte(tt.body), "@timestamp")
			if result != tt.expected {
				t.Errorf("Route() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRouteTarget_String(t *testing.T) {
	tests := []struct {
		target   RouteTarget
		expected string
	}{
		{RouteHotOnly, "hot_only"},
		{RouteColdOnly, "cold_only"},
		{RouteBoth, "both"},
	}

	for _, tt := range tests {
		if got := tt.target.String(); got != tt.expected {
			t.Errorf("%d.String() = %q, want %q", tt.target, got, tt.expected)
		}
	}
}
