package proxy

import (
	"encoding/json"
	"fmt"
)

type fanoutPlan struct {
	Body  []byte
	Merge MergeOptions
}

// planFanout prepares a query body for fan-out merging.
// It supports only score-based ordering (default or explicit _score sort).
// For from/size pagination, it rewrites backend requests to fetch enough hits
// (size = from+size, from = 0) so that the merged page is correct.
func planFanout(body []byte) (fanoutPlan, error) {
	plan := fanoutPlan{
		Body: body,
		Merge: MergeOptions{
			From:     0,
			Size:     10, // OpenSearch default
			Paginate: false,
		},
	}

	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		// If the body isn't JSON, fall back to best-effort merge without pagination.
		return plan, nil
	}

	from := getInt(m, "from", 0)
	size := getInt(m, "size", 10)
	if size < 0 {
		size = 0
	}
	if from < 0 {
		from = 0
	}

	scoreAsc, ok := parseScoreSort(m["sort"])
	if !ok {
		return plan, fmt.Errorf("explicit sort is not supported (only _score)")
	}

	if _, exists := m["search_after"]; exists {
		return plan, fmt.Errorf("search_after is not supported for cross-tier merge")
	}
	if _, exists := m["pit"]; exists {
		return plan, fmt.Errorf("pit is not supported for cross-tier merge")
	}

	need := size
	if size > 0 && from > 0 {
		need = from + size
	}
	if size == 0 {
		need = 0
	}

	m["from"] = 0
	m["size"] = need

	rebuilt, err := json.Marshal(m)
	if err != nil {
		return plan, nil
	}

	plan.Body = rebuilt
	plan.Merge = MergeOptions{
		From:     from,
		Size:     size,
		ScoreAsc: scoreAsc,
		Paginate: true,
	}
	return plan, nil
}

func getInt(m map[string]any, key string, def int) int {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	default:
		return def
	}
}

func parseScoreSort(sortVal any) (scoreAsc bool, ok bool) {
	if sortVal == nil {
		return false, true
	}

	switch s := sortVal.(type) {
	case string:
		return false, s == "_score"
	case []any:
		if len(s) == 0 {
			return false, true
		}
		if len(s) != 1 {
			return false, false
		}
		return parseScoreSort(s[0])
	case map[string]any:
		if len(s) != 1 {
			return false, false
		}
		v, ok := s["_score"]
		if !ok {
			return false, false
		}
		switch vv := v.(type) {
		case string:
			switch vv {
			case "asc":
				return true, true
			case "desc":
				return false, true
			default:
				return false, true
			}
		case map[string]any:
			if o, ok := vv["order"].(string); ok {
				if o == "asc" {
					return true, true
				}
				return false, true
			}
			return false, true
		default:
			return false, true
		}
	default:
		return false, false
	}
}
