package migration

import (
	"context"
	"encoding/json"

	"github.com/leonunix/oqbridge/internal/backend"
)

// HotClient is the subset of OpenSearch operations needed by Migrator.
type HotClient interface {
	SlicedScroll(ctx context.Context, index string, body []byte, scrollID string, slice *backend.SlicedScrollConfig) (*backend.ScrollResult, error)
	ClearScroll(ctx context.Context, scrollID string) error
	DeleteByQuery(ctx context.Context, index string, body []byte) error
}

// ColdClient is the subset of Quickwit operations needed by Migrator.
type ColdClient interface {
	BulkIngest(ctx context.Context, index string, docs []json.RawMessage) error
	IndexExists(ctx context.Context, index string) (bool, error)
	CreateIndex(ctx context.Context, index string, timestampField string) error
}
