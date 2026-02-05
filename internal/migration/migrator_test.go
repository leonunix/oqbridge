package migration

import "testing"

func TestBuildOldDataQuery(t *testing.T) {
	q := buildOldDataQuery("@timestamp", 30, 5000)

	if q["size"] != 5000 {
		t.Fatalf("size=%v, want %v", q["size"], 5000)
	}

	sortAny, ok := q["sort"].([]map[string]string)
	if !ok || len(sortAny) != 1 || sortAny[0]["@timestamp"] != "asc" {
		t.Fatalf("sort=%v, want [{@timestamp:asc}]", q["sort"])
	}

	queryAny, ok := q["query"].(map[string]interface{})
	if !ok {
		t.Fatalf("query type=%T, want map", q["query"])
	}
	rangeAny, ok := queryAny["range"].(map[string]interface{})
	if !ok {
		t.Fatalf("range type=%T, want map", queryAny["range"])
	}
	fieldAny, ok := rangeAny["@timestamp"].(map[string]interface{})
	if !ok {
		t.Fatalf("field type=%T, want map", rangeAny["@timestamp"])
	}
	if fieldAny["lt"] != "now-30d" {
		t.Fatalf("lt=%v, want %q", fieldAny["lt"], "now-30d")
	}
}

func TestBuildOldDataDeleteQuery(t *testing.T) {
	q := buildOldDataDeleteQuery("ts", 7)

	queryAny, ok := q["query"].(map[string]interface{})
	if !ok {
		t.Fatalf("query type=%T, want map", q["query"])
	}
	rangeAny, ok := queryAny["range"].(map[string]interface{})
	if !ok {
		t.Fatalf("range type=%T, want map", queryAny["range"])
	}
	fieldAny, ok := rangeAny["ts"].(map[string]interface{})
	if !ok {
		t.Fatalf("field type=%T, want map", rangeAny["ts"])
	}
	if fieldAny["lt"] != "now-7d" {
		t.Fatalf("lt=%v, want %q", fieldAny["lt"], "now-7d")
	}
}
