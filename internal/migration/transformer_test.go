package migration

import (
	"encoding/json"
	"testing"
)

func TestTransformDocument(t *testing.T) {
	hit := json.RawMessage(`{
		"_index": "logs",
		"_id": "abc123",
		"_score": 1.0,
		"_source": {"message": "hello", "@timestamp": "2025-01-01T00:00:00Z"}
	}`)

	doc, err := TransformDocument(hit)
	if err != nil {
		t.Fatalf("TransformDocument() error = %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(doc, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if result["message"] != "hello" {
		t.Errorf("message = %v, want hello", result["message"])
	}
	// Should not contain _index, _id, _score metadata.
	if _, ok := result["_index"]; ok {
		t.Error("result should not contain _index")
	}
}

func TestTransformDocument_MissingSource(t *testing.T) {
	hit := json.RawMessage(`{"_index": "logs", "_id": "abc123"}`)

	_, err := TransformDocument(hit)
	if err == nil {
		t.Fatal("expected error for missing _source")
	}
}

func TestTransformBatch(t *testing.T) {
	hits := []json.RawMessage{
		json.RawMessage(`{"_source": {"msg": "a"}}`),
		json.RawMessage(`{"_source": {"msg": "b"}}`),
	}

	docs, err := TransformBatch(hits)
	if err != nil {
		t.Fatalf("TransformBatch() error = %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("len(docs) = %d, want 2", len(docs))
	}
}
