package util

import (
	"encoding/json"
	"regexp"
	"strconv"
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
		if t := parseNowDateMath(val); t != nil {
			return t
		}
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
	case float64:
		// Epoch milliseconds.
		t := time.UnixMilli(int64(val)).UTC()
		return &t
	}
	return nil
}

var nowDateMathRe = regexp.MustCompile(`^now(?:(?P<op>[+-])(?P<num>\d+)(?P<unit>[smhdwMy]))?(?:\|\|.*)?$`)

// parseNowDateMath parses a small, safe subset of OpenSearch date math:
// - now
// - now-<N>[s|m|h|d|w]
// - now+<N>[s|m|h|d|w]
// - now-<N>[M|y] (months/years via AddDate)
// Any unsupported expression returns nil so callers can fall back to "unknown".
func parseNowDateMath(s string) *time.Time {
	m := nowDateMathRe.FindStringSubmatch(s)
	if m == nil {
		return nil
	}

	now := time.Now().UTC()
	op := m[nowDateMathRe.SubexpIndex("op")]
	if op == "" {
		return &now
	}
	numStr := m[nowDateMathRe.SubexpIndex("num")]
	unit := m[nowDateMathRe.SubexpIndex("unit")]

	n, err := strconv.Atoi(numStr)
	if err != nil || n < 0 {
		return nil
	}
	if op == "-" {
		n = -n
	}

	switch unit {
	case "s":
		t := now.Add(time.Duration(n) * time.Second)
		return &t
	case "m":
		t := now.Add(time.Duration(n) * time.Minute)
		return &t
	case "h":
		t := now.Add(time.Duration(n) * time.Hour)
		return &t
	case "d":
		t := now.AddDate(0, 0, n)
		return &t
	case "w":
		t := now.AddDate(0, 0, 7*n)
		return &t
	case "M":
		t := now.AddDate(0, n, 0)
		return &t
	case "y":
		t := now.AddDate(n, 0, 0)
		return &t
	default:
		return nil
	}
}
