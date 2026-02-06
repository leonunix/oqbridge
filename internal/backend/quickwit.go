package backend

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// Quickwit implements the Backend interface for Quickwit.
// Quickwit provides an Elasticsearch-compatible search API at /{index}/_search.
type Quickwit struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	compress bool // Enable gzip compression for ingest requests.
}

// NewQuickwit creates a new Quickwit backend client.
// If httpClient is nil, a default client is used.
func NewQuickwit(baseURL, username, password string, compress bool, httpClient *http.Client) *Quickwit {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Quickwit{
		baseURL:  baseURL,
		username: username,
		password: password,
		client:   httpClient,
		compress: compress,
	}
}

func (q *Quickwit) Name() string { return "quickwit" }

func (q *Quickwit) Search(ctx context.Context, index string, body []byte) (*SearchResponse, error) {
	url := fmt.Sprintf("%s/api/v1/%s/search", q.baseURL, index)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	q.setAuth(req)

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing search request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading search response: %w", err)
	}

	if resp.StatusCode >= 400 {
		slog.Error("quickwit search error", "status", resp.StatusCode, "body", string(respBody))
		return nil, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}

	var result SearchResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decoding search response: %w", err)
	}
	return &result, nil
}

func (q *Quickwit) Scroll(_ context.Context, _ string, _ []byte, _ string) (*ScrollResult, error) {
	return nil, fmt.Errorf("quickwit does not support scroll API")
}

func (q *Quickwit) ClearScroll(_ context.Context, _ string) error {
	return nil
}

func (q *Quickwit) BulkIngest(ctx context.Context, index string, docs []json.RawMessage) error {
	// Build NDJSON body.
	var raw bytes.Buffer
	for _, doc := range docs {
		var docMap map[string]json.RawMessage
		if err := json.Unmarshal(doc, &docMap); err == nil {
			if src, ok := docMap["_source"]; ok {
				raw.Write(src)
				raw.WriteByte('\n')
				continue
			}
		}
		raw.Write(doc)
		raw.WriteByte('\n')
	}

	var body io.Reader = &raw
	contentEncoding := ""

	// Gzip compress if enabled (significant savings for 200GB+ daily transfers).
	if q.compress {
		var compressed bytes.Buffer
		gz, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
		if err != nil {
			return fmt.Errorf("gzip init: %w", err)
		}
		if _, err := gz.Write(raw.Bytes()); err != nil {
			return fmt.Errorf("gzip compression: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
		body = &compressed
		contentEncoding = "gzip"
	}

	url := fmt.Sprintf("%s/api/v1/%s/ingest", q.baseURL, index)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("creating ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	q.setAuth(req)

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing ingest request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return &HTTPStatusError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}
	return nil
}

func (q *Quickwit) setAuth(req *http.Request) {
	if q.username != "" {
		req.SetBasicAuth(q.username, q.password)
	}
}

// IndexExists checks if an index exists in Quickwit.
func (q *Quickwit) IndexExists(ctx context.Context, index string) (bool, error) {
	url := fmt.Sprintf("%s/api/v1/indexes/%s", q.baseURL, index)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("creating index exists request: %w", err)
	}
	q.setAuth(req)

	resp, err := q.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("executing index exists request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return false, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}
	return true, nil
}

// CreateIndex creates a new index in Quickwit with dynamic schema mode.
func (q *Quickwit) CreateIndex(ctx context.Context, index string, timestampField string) error {
	indexConfig := map[string]interface{}{
		"version":  "0.8",
		"index_id": index,
		"doc_mapping": map[string]interface{}{
			"mode":            "dynamic",
			"timestamp_field": timestampField,
		},
		"indexing_settings": map[string]interface{}{
			"commit_timeout_secs": 60,
		},
	}

	body, err := json.Marshal(indexConfig)
	if err != nil {
		return fmt.Errorf("marshaling index config: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/indexes", q.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating create index request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	q.setAuth(req)

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing create index request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return &HTTPStatusError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}
	return nil
}
