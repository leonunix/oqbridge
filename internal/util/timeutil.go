package util

import (
	"encoding/json"
	"time"
)

// TimeRange represents an extracted time range from a query.
type TimeRange struct {
	From *time.Time
	To   *time.Time
}

// ExtractTimeRange parses an OpenSearch query DSL body and extracts the time range
// on the given timestamp field. It looks for "range" clauses in "query.bool.filter",
// "query.bool.must", and top-level "query.range".
func ExtractTimeRange(body []byte, timestampField string) *TimeRange {
	var query map[string]json.RawMessage
	if err := json.Unmarshal(body, &query); err != nil {
		return nil
	}

	qRaw, ok := query["query"]
	if !ok {
		return nil
	}

	var q map[string]json.RawMessage
	if err := json.Unmarshal(qRaw, &q); err != nil {
		return nil
	}

	// Try top-level "range" directly under "query".
	if rangeRaw, ok := q["range"]; ok {
		if tr := parseRangeClause(rangeRaw, timestampField); tr != nil {
			return tr
		}
	}

	// Try "bool" query.
	boolRaw, ok := q["bool"]
	if !ok {
		return nil
	}

	var boolQ map[string]json.RawMessage
	if err := json.Unmarshal(boolRaw, &boolQ); err != nil {
		return nil
	}

	// Search in "filter" and "must" arrays.
	for _, key := range []string{"filter", "must"} {
		clauseRaw, ok := boolQ[key]
		if !ok {
			continue
		}

		// Clauses can be a single object or an array.
		var clauses []json.RawMessage
		if err := json.Unmarshal(clauseRaw, &clauses); err != nil {
			// Try as single object.
			clauses = []json.RawMessage{clauseRaw}
		}

		for _, clause := range clauses {
			var c map[string]json.RawMessage
			if err := json.Unmarshal(clause, &c); err != nil {
				continue
			}
			if rangeRaw, ok := c["range"]; ok {
				if tr := parseRangeClause(rangeRaw, timestampField); tr != nil {
					return tr
				}
			}
		}
	}

	return nil
}

func parseRangeClause(rangeRaw json.RawMessage, timestampField string) *TimeRange {
	var rangeMap map[string]json.RawMessage
	if err := json.Unmarshal(rangeRaw, &rangeMap); err != nil {
		return nil
	}

	fieldRaw, ok := rangeMap[timestampField]
	if !ok {
		return nil
	}

	var bounds map[string]interface{}
	if err := json.Unmarshal(fieldRaw, &bounds); err != nil {
		return nil
	}

	tr := &TimeRange{}
	for _, key := range []string{"gte", "gt", "from"} {
		if v, ok := bounds[key]; ok {
			if t := parseTimeValue(v); t != nil {
				tr.From = t
				break
			}
		}
	}
	for _, key := range []string{"lte", "lt", "to"} {
		if v, ok := bounds[key]; ok {
			if t := parseTimeValue(v); t != nil {
				tr.To = t
				break
			}
		}
	}

	if tr.From == nil && tr.To == nil {
		return nil
	}
	return tr
}

func parseTimeValue(v interface{}) *time.Time {
	switch val := v.(type) {
	case string:
		// Try common time formats.
		for _, layout := range []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, val); err == nil {
				return &t
			}
		}
		// Handle "now" expressions (simplified: treat as current time).
		if len(val) >= 3 && val[:3] == "now" {
			t := time.Now().UTC()
			return &t
		}
	case float64:
		// Epoch milliseconds.
		t := time.UnixMilli(int64(val)).UTC()
		return &t
	}
	return nil
}
