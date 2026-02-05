package proxy

import (
	"encoding/json"
	"sort"

	"github.com/leonunix/oqbridge/internal/backend"
)

// MergeSearchResponses combines search results from two backends.
// It merges hits, sums totals, and picks the larger took value.
func MergeSearchResponses(hot, cold *backend.SearchResponse) *backend.SearchResponse {
	if hot == nil {
		return cold
	}
	if cold == nil {
		return hot
	}

	merged := &backend.SearchResponse{
		Took:     max(hot.Took, cold.Took),
		TimedOut: hot.TimedOut || cold.TimedOut,
		Shards:   hot.Shards,
		Hits: backend.HitsResult{
			Total: backend.HitsTotal{
				Value:    hot.Hits.Total.Value + cold.Hits.Total.Value,
				Relation: mergeRelation(hot.Hits.Total.Relation, cold.Hits.Total.Relation),
			},
			MaxScore: mergeMaxScore(hot.Hits.MaxScore, cold.Hits.MaxScore),
			Hits:     append(hot.Hits.Hits, cold.Hits.Hits...),
		},
		Aggregations: mergeAggregations(hot.Aggregations, cold.Aggregations),
	}

	// Re-sort by _score descending (default OpenSearch sort).
	sortHitsByScore(merged.Hits.Hits)

	return merged
}

func mergeRelation(a, b string) string {
	if a == "gte" || b == "gte" {
		return "gte"
	}
	return "eq"
}

func mergeMaxScore(a, b *float64) *float64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	v := *a
	if *b > v {
		v = *b
	}
	return &v
}

func sortHitsByScore(hits []json.RawMessage) {
	sort.SliceStable(hits, func(i, j int) bool {
		si := extractScore(hits[i])
		sj := extractScore(hits[j])
		return si > sj
	})
}

func extractScore(hit json.RawMessage) float64 {
	var h struct {
		Score *float64 `json:"_score"`
	}
	if err := json.Unmarshal(hit, &h); err != nil || h.Score == nil {
		return 0
	}
	return *h.Score
}

// mergeAggregations performs a shallow merge of aggregation results.
// For conflicting keys, the hot backend's values take precedence.
func mergeAggregations(hot, cold json.RawMessage) json.RawMessage {
	if len(hot) == 0 {
		return cold
	}
	if len(cold) == 0 {
		return hot
	}

	var hotMap, coldMap map[string]json.RawMessage
	if json.Unmarshal(hot, &hotMap) != nil || json.Unmarshal(cold, &coldMap) != nil {
		return hot
	}

	// Merge cold into hot (hot wins on conflict).
	for k, v := range coldMap {
		if _, exists := hotMap[k]; !exists {
			hotMap[k] = v
		}
	}

	merged, err := json.Marshal(hotMap)
	if err != nil {
		return hot
	}
	return merged
}
