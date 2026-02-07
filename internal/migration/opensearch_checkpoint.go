package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const stateIndex = ".oqbridge-state"

// OpenSearchCheckpointStore stores migration checkpoints and watermarks in
// OpenSearch, making them visible to all oqbridge-migrate instances.
// This is essential for multi-instance deployments where instances need to
// share migration progress and know what time range was already migrated.
type OpenSearchCheckpointStore struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

// NewOpenSearchCheckpointStore creates a checkpoint store backed by OpenSearch.
func NewOpenSearchCheckpointStore(baseURL, username, password string, httpClient *http.Client) *OpenSearchCheckpointStore {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &OpenSearchCheckpointStore{
		baseURL:  baseURL,
		username: username,
		password: password,
		client:   httpClient,
	}
}

// Load reads a checkpoint for the given index from OpenSearch.
// Returns nil if no checkpoint exists or if the previous run completed.
func (s *OpenSearchCheckpointStore) Load(index string) (*Checkpoint, error) {
	doc, err := s.getDoc(context.Background(), "checkpoint-"+index)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}

	var cp Checkpoint
	if err := json.Unmarshal(doc, &cp); err != nil {
		return nil, fmt.Errorf("parsing checkpoint: %w", err)
	}

	if cp.Completed {
		return nil, nil
	}
	return &cp, nil
}

// Save persists the checkpoint to OpenSearch.
func (s *OpenSearchCheckpointStore) Save(cp *Checkpoint) error {
	cp.UpdatedAt = time.Now().UTC()
	return s.putDoc(context.Background(), "checkpoint-"+cp.Index, cp)
}

// MarkComplete marks the checkpoint as completed.
func (s *OpenSearchCheckpointStore) MarkComplete(index string) error {
	cp, err := s.Load(index)
	if err != nil {
		return err
	}
	if cp == nil {
		cp = &Checkpoint{Index: index}
	}
	cp.Completed = true
	return s.Save(cp)
}

// LoadWatermark reads the watermark for the given index from OpenSearch.
// Returns nil if no watermark exists (first run).
func (s *OpenSearchCheckpointStore) LoadWatermark(index string) (*Watermark, error) {
	doc, err := s.getDoc(context.Background(), "watermark-"+index)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}

	var wm Watermark
	if err := json.Unmarshal(doc, &wm); err != nil {
		return nil, fmt.Errorf("parsing watermark: %w", err)
	}
	return &wm, nil
}

// SaveWatermark persists the watermark to OpenSearch after a successful migration.
func (s *OpenSearchCheckpointStore) SaveWatermark(wm *Watermark) error {
	wm.UpdatedAt = time.Now().UTC()
	return s.putDoc(context.Background(), "watermark-"+wm.Index, wm)
}

// getDoc retrieves a document by ID from the state index.
// Returns nil if the document does not exist.
func (s *OpenSearchCheckpointStore) getDoc(ctx context.Context, id string) (json.RawMessage, error) {
	url := fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, stateIndex, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating get request: %w", err)
	}
	s.setAuth(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing get request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("get doc %s failed: status=%d", id, resp.StatusCode)
	}

	var result struct {
		Found  bool            `json:"found"`
		Source json.RawMessage `json:"_source"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parsing get response: %w", err)
	}
	if !result.Found {
		return nil, nil
	}
	return result.Source, nil
}

// putDoc writes a document by ID to the state index.
// If the index doesn't exist, it is created automatically.
func (s *OpenSearchCheckpointStore) putDoc(ctx context.Context, id string, doc interface{}) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshaling doc: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_doc/%s?refresh=true", s.baseURL, stateIndex, id)
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
		// Index doesn't exist, create it and retry once.
		if createErr := s.ensureIndex(ctx); createErr != nil {
			return fmt.Errorf("creating state index: %w", createErr)
		}
		return s.putDocDirect(ctx, id, body)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("put doc %s failed: status=%d body=%s", id, resp.StatusCode, string(respBody))
	}
	return nil
}

// putDocDirect writes pre-marshaled body directly (used for retry after index creation).
func (s *OpenSearchCheckpointStore) putDocDirect(ctx context.Context, id string, body []byte) error {
	url := fmt.Sprintf("%s/%s/_doc/%s?refresh=true", s.baseURL, stateIndex, id)
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
		return fmt.Errorf("put doc %s failed: status=%d body=%s", id, resp.StatusCode, string(respBody))
	}
	return nil
}

// ensureIndex creates the state index if it doesn't exist.
func (s *OpenSearchCheckpointStore) ensureIndex(ctx context.Context) error {
	url := fmt.Sprintf("%s/%s", s.baseURL, stateIndex)
	body := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":1}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
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

	// Another instance may have created it concurrently.
	if resp.StatusCode == 400 && bytes.Contains(respBody, []byte("resource_already_exists_exception")) {
		return nil
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("creating state index: status=%d body=%s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (s *OpenSearchCheckpointStore) setAuth(req *http.Request) {
	if s.username != "" {
		req.SetBasicAuth(s.username, s.password)
	}
}
