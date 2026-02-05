package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
)

const validToken = "Basic dXNlcjpwYXNz" // user:pass

// newMockOpenSearch creates a mock OpenSearch server that validates auth
// and responds to _search and _plugins/_security/authinfo requests.
func newMockOpenSearch(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Auth check endpoint.
		if r.URL.Path == "/_plugins/_security/authinfo" {
			auth := r.Header.Get("Authorization")
			if auth != validToken {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"user":"user","roles":["admin"]}`))
			return
		}

		// Search endpoint — also validates auth.
		if strings.HasSuffix(r.URL.Path, "/_search") {
			auth := r.Header.Get("Authorization")
			if auth != validToken {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			resp := backend.SearchResponse{
				Took: 5,
				Hits: backend.HitsResult{
					Total: backend.HitsTotal{Value: 1, Relation: "eq"},
					Hits:  []json.RawMessage{json.RawMessage(`{"_score":1.0,"_source":{"msg":"hot"}}`)},
				},
			}
			json.NewEncoder(w).Encode(resp)
			return
		}

		// Default: passthrough.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
}

// newMockQuickwit creates a mock Quickwit server that always responds.
func newMockQuickwit(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := backend.SearchResponse{
			Took: 10,
			Hits: backend.HitsResult{
				Total: backend.HitsTotal{Value: 1, Relation: "eq"},
				Hits:  []json.RawMessage{json.RawMessage(`{"_score":0.8,"_source":{"msg":"cold"}}`)},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
}

func newTestProxy(t *testing.T, osURL, qwURL string) *Proxy {
	t.Helper()
	cfg := &config.Config{
		Server:     config.ServerConfig{Listen: ":0"},
		OpenSearch: config.OpenSearchConfig{URL: osURL},
		Quickwit:   config.QuickwitConfig{URL: qwURL},
		Retention: config.RetentionConfig{
			Days:           30,
			TimestampField: "@timestamp",
		},
	}
	hot := backend.NewOpenSearch(osURL, "", "")
	cold := backend.NewQuickwit(qwURL, "", "", false)
	p, err := New(cfg, hot, cold)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return p
}

// buildColdOnlyQuery returns a query body that targets data older than 30 days,
// forcing a RouteColdOnly decision.
func buildColdOnlyQuery() string {
	old := time.Now().UTC().AddDate(0, 0, -90).Format(time.RFC3339)
	veryOld := time.Now().UTC().AddDate(0, 0, -60).Format(time.RFC3339)
	return fmt.Sprintf(`{"query":{"range":{"@timestamp":{"gte":"%s","lte":"%s"}}}}`, old, veryOld)
}

// buildHotOnlyQuery returns a query body targeting recent data (hot only).
func buildHotOnlyQuery() string {
	recent := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)
	return fmt.Sprintf(`{"query":{"range":{"@timestamp":{"gte":"%s","lte":"%s"}}}}`, recent, now)
}

// buildBothQuery returns a query body that spans hot and cold data.
func buildBothQuery() string {
	old := time.Now().UTC().AddDate(0, 0, -60).Format(time.RFC3339)
	recent := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	return fmt.Sprintf(`{"query":{"range":{"@timestamp":{"gte":"%s","lte":"%s"}}}}`, old, recent)
}

func TestProxy_ColdOnly_ValidAuth(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildColdOnlyQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp backend.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Hits.Total.Value != 1 {
		t.Errorf("expected 1 hit, got %d", resp.Hits.Total.Value)
	}
}

func TestProxy_ColdOnly_InvalidAuth(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildColdOnlyQuery()))
	req.Header.Set("Authorization", "Basic aW52YWxpZDppbnZhbGlk") // invalid:invalid
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_ColdOnly_NoAuth(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildColdOnlyQuery()))
	// No Authorization header.
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_HotOnly_PassthroughAuth(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	// Valid auth — should passthrough to OpenSearch and succeed.
	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildHotOnlyQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_HotOnly_InvalidAuth_PassthroughToOS(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	// Invalid auth — reverse proxy forwards to OpenSearch, which returns 401.
	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildHotOnlyQuery()))
	req.Header.Set("Authorization", "Basic bad")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	// OpenSearch returns 401, reverse proxy forwards it.
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 from OpenSearch passthrough, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_Both_ValidAuth_MergedResults(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildBothQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp backend.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	// Should have merged results from both backends.
	if resp.Hits.Total.Value != 2 {
		t.Errorf("expected 2 merged hits, got %d", resp.Hits.Total.Value)
	}
}

func TestProxy_Both_ExplicitSort_Unsupported(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(fmt.Sprintf(`%s`, buildBothQuery())))
	// Inject an explicit sort that isn't _score.
	req.Body = io.NopCloser(strings.NewReader(fmt.Sprintf(`{"sort":[{"@timestamp":"desc"}],%s`, strings.TrimPrefix(buildBothQuery(), "{"))))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_MultiIndex_Both_MergedResults(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/a,b/_search", strings.NewReader(buildBothQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp backend.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	// Hot leg returns 1 hit; cold leg queries Quickwit per index (2 hits).
	if resp.Hits.Total.Value != 3 {
		t.Fatalf("expected 3 merged hits, got %d", resp.Hits.Total.Value)
	}
}

func TestProxy_MultiIndex_ColdOnly_MergedResults(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/a,b/_search", strings.NewReader(buildColdOnlyQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp backend.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Hits.Total.Value != 2 {
		t.Fatalf("expected 2 cold merged hits, got %d", resp.Hits.Total.Value)
	}
}

func TestProxy_MultiIndex_ColdOnly_ExplicitSort_Unsupported(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	cold := buildColdOnlyQuery()
	body := fmt.Sprintf(`{"sort":[{"@timestamp":"desc"}],%s`, strings.TrimPrefix(cold, "{"))
	req := httptest.NewRequest(http.MethodPost, "/a,b/_search", strings.NewReader(body))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_RootSearch_Passthrough(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/_search", strings.NewReader(buildHotOnlyQuery()))
	req.Header.Set("Authorization", "Basic bad")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_MSearch_MixedRouting(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	body := strings.Join([]string{
		`{}`,
		buildHotOnlyQuery(),
		`{}`,
		buildColdOnlyQuery(),
		"",
	}, "\n")

	req := httptest.NewRequest(http.MethodPost, "/logs/_msearch", strings.NewReader(body))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/x-ndjson")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var out struct {
		Responses []json.RawMessage `json:"responses"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("parse msearch response: %v", err)
	}
	if len(out.Responses) != 2 {
		t.Fatalf("responses=%d, want 2", len(out.Responses))
	}
}

func TestProxy_MSearch_NoAuth_WhenColdPresent(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	body := strings.Join([]string{
		`{}`,
		buildColdOnlyQuery(),
		"",
	}, "\n")

	req := httptest.NewRequest(http.MethodPost, "/logs/_msearch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_Both_FromSize_PaginatesAndRewritesBackendRequests(t *testing.T) {
	const wantFrom = 1
	const wantSize = 1

	os := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_plugins/_security/authinfo" {
			if r.Header.Get("Authorization") != validToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/_search") {
			b, _ := io.ReadAll(r.Body)
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			if int(m["from"].(float64)) != 0 || int(m["size"].(float64)) != wantFrom+wantSize {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"error":"body not rewritten"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			resp := backend.SearchResponse{
				Took: 1,
				Hits: backend.HitsResult{
					Total: backend.HitsTotal{Value: 2, Relation: "eq"},
					Hits: []json.RawMessage{
						json.RawMessage(`{"_score":2.0}`),
						json.RawMessage(`{"_score":1.0}`),
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer os.Close()

	qw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/search") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if int(m["from"].(float64)) != 0 || int(m["size"].(float64)) != wantFrom+wantSize {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"body not rewritten"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := backend.SearchResponse{
			Took: 1,
			Hits: backend.HitsResult{
				Total: backend.HitsTotal{Value: 2, Relation: "eq"},
				Hits: []json.RawMessage{
					json.RawMessage(`{"_score":3.0}`),
					json.RawMessage(`{"_score":0.0}`),
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	both := buildBothQuery()
	body := fmt.Sprintf(`{"from":%d,"size":%d,%s`, wantFrom, wantSize, strings.TrimPrefix(both, "{"))
	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(body))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp backend.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if len(resp.Hits.Hits) != 1 {
		t.Fatalf("hits=%d, want 1", len(resp.Hits.Hits))
	}
	if s := extractScore(resp.Hits.Hits[0]); s != 2.0 {
		t.Fatalf("score=%v, want 2.0", s)
	}
}

func TestProxy_Both_InvalidAuth_DoesNotLeakCold(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildBothQuery()))
	req.Header.Set("Authorization", "Basic aW52YWxpZDppbnZhbGlk") // invalid:invalid
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_Both_NoAuth_DoesNotLeakCold(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildBothQuery()))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_Both_OpenSearch500_ValidAuth_ReturnsCold(t *testing.T) {
	t.Helper()

	// OpenSearch authinfo succeeds, but _search returns 500.
	os := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_plugins/_security/authinfo" {
			if r.Header.Get("Authorization") != validToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"user":"user"}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/_search") {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"boom"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer os.Close()

	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildBothQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp backend.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Hits.Total.Value != 1 {
		t.Errorf("expected cold-only result (1 hit), got %d", resp.Hits.Total.Value)
	}
}

func TestProxy_ColdOnly_OpenSearchAuthError_ReturnsBadGateway(t *testing.T) {
	// OpenSearch authinfo returns 500; proxy should not treat it as successful auth.
	os := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_plugins/_security/authinfo" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"oops"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer os.Close()

	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodPost, "/logs/_search", strings.NewReader(buildColdOnlyQuery()))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d: %s", w.Code, w.Body.String())
	}
}

func TestProxy_HealthEndpoint_NoAuth(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	// No auth header — health should not require auth.
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	body, _ := io.ReadAll(w.Body)
	if !strings.Contains(string(body), "oqbridge") {
		t.Errorf("health response should contain 'oqbridge', got: %s", body)
	}
}

func TestProxy_InternalIndex_SkipsInterception(t *testing.T) {
	os := newMockOpenSearch(t)
	defer os.Close()
	qw := newMockQuickwit(t)
	defer qw.Close()

	p := newTestProxy(t, os.URL, qw.URL)

	// Internal index (starts with .) should be passed through to OpenSearch.
	req := httptest.NewRequest(http.MethodPost, "/.kibana/_search", strings.NewReader(`{}`))
	req.Header.Set("Authorization", validToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}
