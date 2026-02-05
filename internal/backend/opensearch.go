package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// OpenSearch implements the Backend interface for OpenSearch.
type OpenSearch struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

// NewOpenSearch creates a new OpenSearch backend client.
func NewOpenSearch(baseURL, username, password string) *OpenSearch {
	return &OpenSearch{
		baseURL:  baseURL,
		username: username,
		password: password,
		client:   &http.Client{},
	}
}

func (o *OpenSearch) Name() string { return "opensearch" }

// Authenticate validates the given credentials against OpenSearch's _security/authinfo
// or by performing a lightweight request. Returns nil if auth succeeds, error otherwise.
func (o *OpenSearch) Authenticate(ctx context.Context, authHeader string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.baseURL+"/_plugins/_security/authinfo", nil)
	if err != nil {
		return fmt.Errorf("creating auth request: %w", err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("auth request failed: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("authentication failed: %d", resp.StatusCode)
	}
	return nil
}

func (o *OpenSearch) Search(ctx context.Context, index string, body []byte) (*SearchResponse, error) {
	return o.SearchAs(ctx, index, body, "")
}

// SearchAs executes a search using the given auth header instead of the
// configured service account. If authHeader is empty, falls back to service account.
func (o *OpenSearch) SearchAs(ctx context.Context, index string, body []byte, authHeader string) (*SearchResponse, error) {
	url := fmt.Sprintf("%s/%s/_search", o.baseURL, index)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	} else {
		o.setAuth(req)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing search request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading search response: %w", err)
	}

	if resp.StatusCode >= 400 {
		slog.Error("opensearch search error", "status", resp.StatusCode, "body", string(respBody))
		return nil, fmt.Errorf("search returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result SearchResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decoding search response: %w", err)
	}
	return &result, nil
}

// SlicedScrollConfig configures a sliced scroll request for parallel reads.
type SlicedScrollConfig struct {
	SliceID    int // This slice's ID (0-based).
	SliceMax   int // Total number of slices.
	ScrollKeep string // Scroll context TTL (e.g., "5m").
}

func (o *OpenSearch) Scroll(ctx context.Context, index string, body []byte, scrollID string) (*ScrollResult, error) {
	return o.SlicedScroll(ctx, index, body, scrollID, nil)
}

// SlicedScroll performs a scroll request, optionally using sliced scroll for parallelism.
// On initial request (scrollID == ""), slice config is injected into the query body.
// On continuation (scrollID != ""), slice config is ignored.
func (o *OpenSearch) SlicedScroll(ctx context.Context, index string, body []byte, scrollID string, slice *SlicedScrollConfig) (*ScrollResult, error) {
	var reqURL string
	var reqBody []byte

	scrollKeep := "5m"
	if slice != nil && slice.ScrollKeep != "" {
		scrollKeep = slice.ScrollKeep
	}

	if scrollID == "" {
		// Initial scroll request.
		reqURL = fmt.Sprintf("%s/%s/_search?scroll=%s", o.baseURL, index, scrollKeep)

		// Inject slice config into the query body if provided.
		if slice != nil && slice.SliceMax > 1 {
			var queryMap map[string]interface{}
			if err := json.Unmarshal(body, &queryMap); err != nil {
				return nil, fmt.Errorf("parsing query for slice injection: %w", err)
			}
			queryMap["slice"] = map[string]int{
				"id":  slice.SliceID,
				"max": slice.SliceMax,
			}
			var err error
			reqBody, err = json.Marshal(queryMap)
			if err != nil {
				return nil, fmt.Errorf("marshaling sliced query: %w", err)
			}
		} else {
			reqBody = body
		}
	} else {
		// Continue scroll.
		reqURL = fmt.Sprintf("%s/_search/scroll", o.baseURL)
		scrollReq := map[string]string{
			"scroll":    scrollKeep,
			"scroll_id": scrollID,
		}
		var err error
		reqBody, err = json.Marshal(scrollReq)
		if err != nil {
			return nil, fmt.Errorf("marshaling scroll request: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating scroll request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	o.setAuth(req)

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing scroll request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading scroll response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("scroll returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var raw struct {
		ScrollID string     `json:"_scroll_id"`
		Hits     HitsResult `json:"hits"`
	}
	if err := json.Unmarshal(respBody, &raw); err != nil {
		return nil, fmt.Errorf("decoding scroll response: %w", err)
	}

	return &ScrollResult{
		ScrollID: raw.ScrollID,
		Hits:     raw.Hits.Hits,
		Total:    raw.Hits.Total.Value,
	}, nil
}

func (o *OpenSearch) ClearScroll(ctx context.Context, scrollID string) error {
	body, _ := json.Marshal(map[string]string{"scroll_id": scrollID})
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, o.baseURL+"/_search/scroll", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating clear scroll request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	o.setAuth(req)

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing clear scroll: %w", err)
	}
	resp.Body.Close()
	return nil
}

func (o *OpenSearch) BulkIngest(ctx context.Context, index string, docs []json.RawMessage) error {
	var buf bytes.Buffer
	for _, doc := range docs {
		// Extract _id if present in the document source.
		var docMap map[string]json.RawMessage
		if err := json.Unmarshal(doc, &docMap); err != nil {
			return fmt.Errorf("parsing document: %w", err)
		}

		// Build the action line.
		action := map[string]interface{}{
			"index": map[string]string{"_index": index},
		}
		actionBytes, _ := json.Marshal(action)
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Write the source document.
		source := doc
		if src, ok := docMap["_source"]; ok {
			source = src
		}
		buf.Write(source)
		buf.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/_bulk", o.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return fmt.Errorf("creating bulk request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	o.setAuth(req)

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing bulk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bulk ingest returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// DeleteByQuery deletes documents matching the given query from the index.
func (o *OpenSearch) DeleteByQuery(ctx context.Context, index string, body []byte) error {
	url := fmt.Sprintf("%s/%s/_delete_by_query", o.baseURL, index)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating delete_by_query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	o.setAuth(req)

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing delete_by_query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete_by_query returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (o *OpenSearch) setAuth(req *http.Request) {
	if o.username != "" {
		req.SetBasicAuth(o.username, o.password)
	}
}
