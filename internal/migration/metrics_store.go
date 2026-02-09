package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const metricsIndex = ".oqbridge-migration-metrics"

// OpenSearchMetricsStore records migration metrics into an OpenSearch index.
type OpenSearchMetricsStore struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

// NewOpenSearchMetricsStore creates a metrics store backed by OpenSearch.
func NewOpenSearchMetricsStore(baseURL, username, password string, httpClient *http.Client) *OpenSearchMetricsStore {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &OpenSearchMetricsStore{
		baseURL:  baseURL,
		username: username,
		password: password,
		client:   httpClient,
	}
}

// Record persists a migration metric document to OpenSearch.
func (s *OpenSearchMetricsStore) Record(ctx context.Context, metric *MigrationMetric) error {
	return s.putDoc(ctx, metricDocID(metric), metric)
}

// metricDocID returns a deterministic document ID for the metric,
// allowing safe retries without creating duplicates.
func metricDocID(m *MigrationMetric) string {
	return fmt.Sprintf("metric-%s-%d", m.Index, m.StartedAt.Unix())
}

// putDoc writes a document by ID to the metrics index.
// If the index doesn't exist, it is created automatically.
func (s *OpenSearchMetricsStore) putDoc(ctx context.Context, id string, doc interface{}) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshaling metric: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, metricsIndex, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating put request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	s.setAuth(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing put request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		if createErr := s.ensureIndex(ctx); createErr != nil {
			return fmt.Errorf("creating metrics index: %w", createErr)
		}
		return s.putDocDirect(ctx, id, body)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("put metric %s failed: status=%d body=%s", id, resp.StatusCode, string(respBody))
	}
	return nil
}

// putDocDirect writes pre-marshaled body directly (used for retry after index creation).
func (s *OpenSearchMetricsStore) putDocDirect(ctx context.Context, id string, body []byte) error {
	url := fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, metricsIndex, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating put request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	s.setAuth(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing put request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("put metric %s failed: status=%d body=%s", id, resp.StatusCode, string(respBody))
	}
	return nil
}

// ensureIndex creates the metrics index with appropriate mappings.
func (s *OpenSearchMetricsStore) ensureIndex(ctx context.Context) error {
	url := fmt.Sprintf("%s/%s", s.baseURL, metricsIndex)
	mapping := `{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  },
  "mappings": {
    "properties": {
      "@timestamp":          { "type": "date" },
      "index":               { "type": "keyword" },
      "started_at":          { "type": "date" },
      "completed_at":        { "type": "date" },
      "duration_sec":        { "type": "float" },
      "documents_migrated":  { "type": "long" },
      "docs_per_sec":        { "type": "float" },
      "workers":             { "type": "integer" },
      "batch_size":          { "type": "integer" },
      "status":              { "type": "keyword" },
      "error":               { "type": "text" },
      "cutoff_time":         { "type": "date" }
    }
  }
}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader([]byte(mapping)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	s.setAuth(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == 400 && bytes.Contains(respBody, []byte("resource_already_exists_exception")) {
		return nil
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("creating metrics index: status=%d body=%s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (s *OpenSearchMetricsStore) setAuth(req *http.Request) {
	if s.username != "" {
		req.SetBasicAuth(s.username, s.password)
	}
}

// Verify compile-time interface compliance.
var _ MetricsRecorder = (*OpenSearchMetricsStore)(nil)
