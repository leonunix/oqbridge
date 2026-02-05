package proxy

import (
	"encoding/json"
	"testing"

	"github.com/leonunix/oqbridge/internal/backend"
)

func float64Ptr(f float64) *float64 { return &f }

func TestMergeSearchResponses_BothNonNil(t *testing.T) {
	hot := &backend.SearchResponse{
		Took: 10,
		Hits: backend.HitsResult{
			Total:    backend.HitsTotal{Value: 2, Relation: "eq"},
			MaxScore: float64Ptr(1.5),
			Hits: []json.RawMessage{
				json.RawMessage(`{"_score": 1.5, "_source": {"msg": "hot1"}}`),
				json.RawMessage(`{"_score": 1.0, "_source": {"msg": "hot2"}}`),
			},
		},
	}

	cold := &backend.SearchResponse{
		Took: 20,
		Hits: backend.HitsResult{
			Total:    backend.HitsTotal{Value: 1, Relation: "eq"},
			MaxScore: float64Ptr(1.2),
			Hits: []json.RawMessage{
				json.RawMessage(`{"_score": 1.2, "_source": {"msg": "cold1"}}`),
			},
		},
	}

	merged := MergeSearchResponses(hot, cold)

	if merged.Took != 20 {
		t.Errorf("Took = %d, want 20", merged.Took)
	}
	if merged.Hits.Total.Value != 3 {
		t.Errorf("Total.Value = %d, want 3", merged.Hits.Total.Value)
	}
	if len(merged.Hits.Hits) != 3 {
		t.Errorf("len(Hits) = %d, want 3", len(merged.Hits.Hits))
	}
	if *merged.Hits.MaxScore != 1.5 {
		t.Errorf("MaxScore = %f, want 1.5", *merged.Hits.MaxScore)
	}

	// Check sort order: 1.5, 1.2, 1.0
	scores := make([]float64, len(merged.Hits.Hits))
	for i, h := range merged.Hits.Hits {
		scores[i] = extractScore(h)
	}
	if scores[0] != 1.5 || scores[1] != 1.2 || scores[2] != 1.0 {
		t.Errorf("sort order = %v, want [1.5, 1.2, 1.0]", scores)
	}
}

func TestMergeSearchResponses_OneNil(t *testing.T) {
	resp := &backend.SearchResponse{
		Took: 5,
		Hits: backend.HitsResult{
			Total: backend.HitsTotal{Value: 1, Relation: "eq"},
		},
	}

	if result := MergeSearchResponses(resp, nil); result != resp {
		t.Error("expected hot response when cold is nil")
	}
	if result := MergeSearchResponses(nil, resp); result != resp {
		t.Error("expected cold response when hot is nil")
	}
}

func TestMergeSearchResponses_GteRelation(t *testing.T) {
	hot := &backend.SearchResponse{
		Hits: backend.HitsResult{
			Total: backend.HitsTotal{Value: 10, Relation: "gte"},
		},
	}
	cold := &backend.SearchResponse{
		Hits: backend.HitsResult{
			Total: backend.HitsTotal{Value: 5, Relation: "eq"},
		},
	}

	merged := MergeSearchResponses(hot, cold)
	if merged.Hits.Total.Relation != "gte" {
		t.Errorf("Relation = %q, want \"gte\"", merged.Hits.Total.Relation)
	}
}
