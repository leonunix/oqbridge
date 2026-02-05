package backend

import (
	"context"
	"encoding/json"
)

// SearchResponse represents a unified search response from either backend.
type SearchResponse struct {
	Took     int             `json:"took"`
	TimedOut bool            `json:"timed_out"`
	Shards   json.RawMessage `json:"_shards,omitempty"`
	Hits     HitsResult      `json:"hits"`
	// Aggregations are kept as raw JSON for pass-through merging.
	Aggregations json.RawMessage `json:"aggregations,omitempty"`
}

// HitsResult contains the search hits.
type HitsResult struct {
	Total    HitsTotal         `json:"total"`
	MaxScore *float64          `json:"max_score"`
	Hits     []json.RawMessage `json:"hits"`
}

// HitsTotal represents the total hit count.
type HitsTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

// ScrollResult contains a batch of documents from a scroll operation.
type ScrollResult struct {
	ScrollID string
	Hits     []json.RawMessage
	Total    int
}

// Backend defines the interface for search backends (OpenSearch, Quickwit).
type Backend interface {
	// Search executes a search query against the given index.
	Search(ctx context.Context, index string, body []byte) (*SearchResponse, error)

	// Scroll initiates or continues a scroll query for bulk reading.
	Scroll(ctx context.Context, index string, body []byte, scrollID string) (*ScrollResult, error)

	// ClearScroll releases server-side scroll resources.
	ClearScroll(ctx context.Context, scrollID string) error

	// BulkIngest sends a batch of documents to the backend.
	BulkIngest(ctx context.Context, index string, docs []json.RawMessage) error

	// Name returns the backend name for logging purposes.
	Name() string
}
