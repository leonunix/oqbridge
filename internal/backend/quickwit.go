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
	"os"
	"path/filepath"
)

// Quickwit implements the Backend interface for Quickwit.
// Quickwit provides an Elasticsearch-compatible search API at /{index}/_search.
type Quickwit struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	compress bool   // Enable gzip compression for ingest requests.
	tempDir  string // When non-empty, stage ingest payloads on disk instead of in memory.
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

// SetTempDir configures a directory for staging ingest payloads on disk.
// When set, BulkIngest writes NDJSON (and optional gzip) to temporary files
// instead of in-memory buffers, reducing memory usage for large batches.
func (q *Quickwit) SetTempDir(dir string) {
	q.tempDir = dir
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
	if q.tempDir != "" {
		return q.bulkIngestViaDisk(ctx, index, docs)
	}
	return q.bulkIngestInMemory(ctx, index, docs)
}

// bulkIngestInMemory stages the NDJSON payload entirely in memory.
func (q *Quickwit) bulkIngestInMemory(ctx context.Context, index string, docs []json.RawMessage) error {
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

	return q.sendIngest(ctx, index, body, contentEncoding)
}

// bulkIngestViaDisk stages the NDJSON payload to a temporary file on disk,
// reducing memory usage for large batches.
func (q *Quickwit) bulkIngestViaDisk(ctx context.Context, index string, docs []json.RawMessage) error {
	// Write NDJSON to a temp file.
	ndjsonFile, err := os.CreateTemp(q.tempDir, "oqbridge-ingest-*.ndjson")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := ndjsonFile.Name()
	defer os.Remove(tmpPath)

	for _, doc := range docs {
		var docMap map[string]json.RawMessage
		if err := json.Unmarshal(doc, &docMap); err == nil {
			if src, ok := docMap["_source"]; ok {
				if _, err := ndjsonFile.Write(src); err != nil {
					ndjsonFile.Close()
					return fmt.Errorf("writing to temp file: %w", err)
				}
				if _, err := ndjsonFile.Write([]byte{'\n'}); err != nil {
					ndjsonFile.Close()
					return fmt.Errorf("writing newline to temp file: %w", err)
				}
				continue
			}
		}
		if _, err := ndjsonFile.Write(doc); err != nil {
			ndjsonFile.Close()
			return fmt.Errorf("writing to temp file: %w", err)
		}
		if _, err := ndjsonFile.Write([]byte{'\n'}); err != nil {
			ndjsonFile.Close()
			return fmt.Errorf("writing newline to temp file: %w", err)
		}
	}
	if err := ndjsonFile.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	contentEncoding := ""
	uploadPath := tmpPath

	// Gzip compress the temp file if enabled.
	if q.compress {
		gzPath := tmpPath + ".gz"
		defer os.Remove(gzPath)

		if err := gzipFile(tmpPath, gzPath); err != nil {
			return fmt.Errorf("gzip compression: %w", err)
		}
		uploadPath = gzPath
		contentEncoding = "gzip"
	}

	f, err := os.Open(uploadPath)
	if err != nil {
		return fmt.Errorf("opening staged file: %w", err)
	}
	defer f.Close()

	return q.sendIngest(ctx, index, f, contentEncoding)
}

// sendIngest sends an ingest request to Quickwit.
func (q *Quickwit) sendIngest(ctx context.Context, index string, body io.Reader, contentEncoding string) error {
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

// gzipFile compresses src to dst using gzip.
func gzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(filepath.Clean(dst))
	if err != nil {
		return err
	}
	defer out.Close()

	gz, err := gzip.NewWriterLevel(out, gzip.BestSpeed)
	if err != nil {
		return err
	}

	if _, err := io.Copy(gz, in); err != nil {
		gz.Close()
		return err
	}
	return gz.Close()
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

// ListIndices returns all index IDs from Quickwit.
func (q *Quickwit) ListIndices(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/indexes", q.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating list indices request: %w", err)
	}
	q.setAuth(req)

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing list indices request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading list indices response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			URL:        url,
			Body:       string(respBody),
		}
	}

	var entries []struct {
		IndexConfig struct {
			IndexID string `json:"index_id"`
		} `json:"index_config"`
	}
	if err := json.Unmarshal(respBody, &entries); err != nil {
		return nil, fmt.Errorf("decoding list indices response: %w", err)
	}

	indices := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IndexConfig.IndexID != "" {
			indices = append(indices, e.IndexConfig.IndexID)
		}
	}
	return indices, nil
}

// CreateIndex creates a new index in Quickwit with dynamic schema mode.
// If retentionDays > 0, a retention policy is set so Quickwit automatically
// deletes data older than the specified number of days.
func (q *Quickwit) CreateIndex(ctx context.Context, index string, timestampField string, retentionDays int) error {
	indexConfig := map[string]interface{}{
		"version":  "0.8",
		"index_id": index,
		"doc_mapping": map[string]interface{}{
			"mode":            "dynamic",
			"timestamp_field": timestampField,
			"field_mappings": []map[string]interface{}{
				{
					"name":          timestampField,
					"type":          "datetime",
					"input_formats": []string{"rfc3339", "unix_timestamp"},
					"output_format": "rfc3339",
					"fast":          true,
				},
			},
		},
		"indexing_settings": map[string]interface{}{
			"commit_timeout_secs": 60,
		},
	}

	if retentionDays > 0 {
		indexConfig["retention"] = map[string]interface{}{
			"period":   fmt.Sprintf("%d days", retentionDays),
			"schedule": "daily",
		}
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
