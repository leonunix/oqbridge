package migration

import (
	"encoding/json"
	"fmt"
)

// TransformDocument extracts the _source field from an OpenSearch scroll hit
// and returns a clean document suitable for Quickwit ingest.
func TransformDocument(hit json.RawMessage) (json.RawMessage, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(hit, &doc); err != nil {
		return nil, fmt.Errorf("parsing hit: %w", err)
	}

	source, ok := doc["_source"]
	if !ok {
		return nil, fmt.Errorf("hit missing _source field")
	}

	return source, nil
}

// TransformBatch transforms a batch of OpenSearch scroll hits into clean
// documents for Quickwit ingest.
func TransformBatch(hits []json.RawMessage) ([]json.RawMessage, error) {
	docs := make([]json.RawMessage, 0, len(hits))
	for i, hit := range hits {
		doc, err := TransformDocument(hit)
		if err != nil {
			return nil, fmt.Errorf("transforming hit %d: %w", i, err)
		}
		docs = append(docs, doc)
	}
	return docs, nil
}
