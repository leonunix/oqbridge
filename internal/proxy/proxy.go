package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
	"github.com/leonunix/oqbridge/internal/util"
)

type endpointKind int

const (
	endpointNone endpointKind = iota
	endpointSearch
	endpointMSearch
)

// Proxy is the core HTTP handler that routes requests between OpenSearch and Quickwit.
type Proxy struct {
	cfg          *config.Config
	router       *Router
	hotBackend   *backend.OpenSearch
	coldBackend  *backend.Quickwit
	reverseProxy *httputil.ReverseProxy
}

// New creates a new Proxy instance.
// If transport is non-nil it is used by the reverse proxy (e.g. for custom TLS).
func New(cfg *config.Config, hot *backend.OpenSearch, cold *backend.Quickwit, transport http.RoundTripper) (*Proxy, error) {
	osURL, err := url.Parse(cfg.OpenSearch.URL)
	if err != nil {
		return nil, err
	}

	rp := httputil.NewSingleHostReverseProxy(osURL)
	if transport != nil {
		rp.Transport = transport
	}
	originalDirector := rp.Director
	rp.Director = func(req *http.Request) {
		originalDirector(req)
		// Do NOT override the client's auth header — let OpenSearch validate
		// the original user credentials. The config's username/password is only
		// used by the backend clients for internal operations.
	}

	return &Proxy{
		cfg:          cfg,
		router:       NewRouter(cfg.Retention.Days),
		hotBackend:   hot,
		coldBackend:  cold,
		reverseProxy: rp,
	}, nil
}

// ServeHTTP handles incoming HTTP requests.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Health check endpoint.
	if r.URL.Path == "/health" || r.URL.Path == "/_health" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"oqbridge"}`))
		return
	}

	kind, indices := parseEndpoint(r.URL.Path)
	switch kind {
	case endpointSearch:
		// Root /_search: passthrough (no reliable index list for Quickwit fan-out).
		if len(indices) == 0 {
			p.reverseProxy.ServeHTTP(w, r)
			return
		}
		// Skip internal indices (start with ".") — these are OpenSearch system indices.
		if hasInternal(indices) {
			p.reverseProxy.ServeHTTP(w, r)
			return
		}
		p.handleSearch(w, r, indices)
		return
	case endpointMSearch:
		if hasInternal(indices) {
			p.reverseProxy.ServeHTTP(w, r)
			return
		}
		p.handleMSearch(w, r, indices)
		return
	}

	// All other requests: passthrough to OpenSearch (OpenSearch validates auth).
	p.reverseProxy.ServeHTTP(w, r)
}

func (p *Proxy) handleSearch(w http.ResponseWriter, r *http.Request, indices []string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// Restore body for potential passthrough.
	r.Body = io.NopCloser(bytes.NewReader(body))

	target := p.routeForIndices(body, indices)

	slog.Debug("search routing decision",
		"indices", strings.Join(indices, ","),
		"target", target.String(),
	)

	switch target {
	case RouteHotOnly:
		// Passthrough to OpenSearch via reverse proxy (OpenSearch validates auth).
		p.reverseProxy.ServeHTTP(w, r)
		return

	case RouteColdOnly:
		// Single non-wildcard index: passthrough to Quickwit (no merge needed).
		if len(indices) == 1 && !hasWildcard(indices) {
			authHeader := r.Header.Get("Authorization")
			if err := p.authenticateViaOpenSearch(r.Context(), authHeader); err != nil {
				status := http.StatusBadGateway
				if authHeader == "" || isAuthError(err) {
					status = statusFromAuthError(err)
				}
				slog.Warn("auth failed for cold-only query", "indices", strings.Join(indices, ","), "status", status, "error", err)
				http.Error(w, `{"error":"authentication failed"}`, status)
				return
			}

			resp, err := p.coldBackend.Search(r.Context(), indices[0], body)
			if err != nil {
				slog.Error("quickwit search failed", "error", err)
				r.Body = io.NopCloser(bytes.NewReader(body))
				p.reverseProxy.ServeHTTP(w, r)
				return
			}
			writeJSON(w, resp)
			return
		}

		fanout, fanoutErr := planFanout(body)
		if fanoutErr != nil {
			http.Error(w, fmt.Sprintf(`{"error":"unsupported query for multi-index merge","detail":%q}`, fanoutErr.Error()), http.StatusBadRequest)
			return
		}

		// Must validate user auth against OpenSearch first, because Quickwit
		// has no knowledge of OpenSearch users.
		authHeader := r.Header.Get("Authorization")
		if err := p.authenticateViaOpenSearch(r.Context(), authHeader); err != nil {
			status := http.StatusBadGateway
			if authHeader == "" || isAuthError(err) {
				status = statusFromAuthError(err)
			}
			slog.Warn("auth failed for cold-only query", "indices", strings.Join(indices, ","), "status", status, "error", err)
			http.Error(w, `{"error":"authentication failed"}`, status)
			return
		}

		resp, err := p.searchColdIndices(r.Context(), indices, fanout.Body)
		if err != nil {
			slog.Error("quickwit search failed", "error", err)
			r.Body = io.NopCloser(bytes.NewReader(body))
			p.reverseProxy.ServeHTTP(w, r)
			return
		}
		writeJSON(w, MergeSearchResponsesWithOptions(nil, resp, fanout.Merge))
		return

	case RouteBoth:
		fanout, fanoutErr := planFanout(body)
		if fanoutErr != nil {
			http.Error(w, fmt.Sprintf(`{"error":"unsupported query for cross-tier merge","detail":%q}`, fanoutErr.Error()), http.StatusBadRequest)
			return
		}

		// Fan-out: We query both backends in parallel and merge results.
		// Security: If OpenSearch indicates auth failure (401/403) or we cannot
		// validate auth due to backend errors, we must not return cold data.
		authHeader := r.Header.Get("Authorization")
		p.handleFanoutSearch(w, r.Context(), strings.Join(indices, ","), r.URL.Path, r.URL.RawQuery, fanout.Body, fanout.Merge, authHeader)
		return
	}
}

// authenticateViaOpenSearch validates the client's credentials by making a
// lightweight call to OpenSearch's security plugin.
func (p *Proxy) authenticateViaOpenSearch(ctx context.Context, authHeader string) error {
	if authHeader == "" {
		return fmt.Errorf("no authorization header")
	}
	return p.hotBackend.Authenticate(ctx, authHeader)
}

func (p *Proxy) handleFanoutSearch(w http.ResponseWriter, ctx context.Context, index string, path string, rawQuery string, body []byte, merge MergeOptions, authHeader string) {
	if authHeader == "" {
		http.Error(w, `{"error":"authentication required"}`, http.StatusUnauthorized)
		return
	}

	var (
		hotResp  *backend.SearchResponse
		coldResp *backend.SearchResponse
		hotErr   error
		coldErr  error
		wg       sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		hotResp, hotErr = p.hotBackend.SearchRaw(ctx, path, rawQuery, body, authHeader)
	}()
	go func() {
		defer wg.Done()
		coldResp, coldErr = p.searchColdIndices(ctx, strings.Split(index, ","), body)
	}()
	wg.Wait()

	if hotErr != nil {
		slog.Error("opensearch search failed during fan-out", "error", hotErr)
	}
	if coldErr != nil {
		slog.Error("quickwit search failed during fan-out", "error", coldErr)
	}

	// If OpenSearch reports auth failure, do NOT return cold data.
	if isAuthError(hotErr) {
		http.Error(w, `{"error":"authentication failed"}`, statusFromAuthError(hotErr))
		return
	}

	// If OpenSearch failed for non-auth reasons, validate auth explicitly before
	// returning any cold data.
	if hotErr != nil {
		if err := p.authenticateViaOpenSearch(ctx, authHeader); err != nil {
			status := http.StatusBadGateway
			if isAuthError(err) {
				status = statusFromAuthError(err)
			}
			http.Error(w, `{"error":"authentication failed"}`, status)
			return
		}
	}

	// If both fail, return error.
	if hotErr != nil && coldErr != nil {
		http.Error(w, `{"error":"both backends failed"}`, http.StatusBadGateway)
		return
	}

	merged := MergeSearchResponsesWithOptions(hotResp, coldResp, merge)
	writeJSON(w, merged)
}

func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	var httpErr *backend.HTTPStatusError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden
	}
	return false
}

func statusFromAuthError(err error) int {
	var httpErr *backend.HTTPStatusError
	if errors.As(err, &httpErr) {
		if httpErr.StatusCode == http.StatusForbidden {
			return http.StatusForbidden
		}
	}
	return http.StatusUnauthorized
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to write response", "error", err)
	}
}

func parseEndpoint(path string) (endpointKind, []string) {
	p := strings.TrimSuffix(path, "/")
	if p == "" {
		return endpointNone, nil
	}
	if p == "/_search" {
		return endpointSearch, nil
	}
	if p == "/_msearch" {
		return endpointMSearch, nil
	}
	if !strings.HasPrefix(p, "/") {
		return endpointNone, nil
	}

	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	if len(parts) != 2 {
		return endpointNone, nil
	}
	indices := splitIndices(parts[0])
	switch parts[1] {
	case "_search":
		return endpointSearch, indices
	case "_msearch":
		return endpointMSearch, indices
	default:
		return endpointNone, nil
	}
}

func splitIndices(seg string) []string {
	if seg == "" {
		return nil
	}
	parts := strings.Split(seg, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func hasInternal(indices []string) bool {
	for _, idx := range indices {
		if strings.HasPrefix(idx, ".") {
			return true
		}
	}
	return false
}

func hasWildcard(indices []string) bool {
	for _, idx := range indices {
		if strings.ContainsAny(idx, "*?[]") {
			return true
		}
	}
	return false
}

func (p *Proxy) routeForIndices(body []byte, indices []string) RouteTarget {
	if len(indices) == 0 {
		return RouteBoth
	}
	target := RouteHotOnly
	first := true
	for _, index := range indices {
		tsField := p.cfg.TimestampFieldForIndex(index)
		t := p.router.Route(body, tsField)
		if first {
			target = t
			first = false
			continue
		}
		if target != t {
			return RouteBoth
		}
	}
	return target
}

// resolveColdIndices expands wildcard patterns in the index list to concrete
// Quickwit index names. Non-wildcard indices are returned as-is.
func (p *Proxy) resolveColdIndices(ctx context.Context, indices []string) ([]string, error) {
	if !hasWildcard(indices) {
		return indices, nil
	}
	all, err := p.coldBackend.ListIndices(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing quickwit indices: %w", err)
	}
	seen := make(map[string]struct{})
	var resolved []string
	for _, idx := range indices {
		if !strings.ContainsAny(idx, "*?[]") {
			if _, ok := seen[idx]; !ok {
				seen[idx] = struct{}{}
				resolved = append(resolved, idx)
			}
			continue
		}
		for _, name := range all {
			if _, ok := seen[name]; ok {
				continue
			}
			if util.MatchWildcard(idx, name) {
				seen[name] = struct{}{}
				resolved = append(resolved, name)
			}
		}
	}
	return resolved, nil
}

func (p *Proxy) searchColdIndices(ctx context.Context, indices []string, body []byte) (*backend.SearchResponse, error) {
	// Resolve wildcard patterns to concrete Quickwit index names.
	resolved, err := p.resolveColdIndices(ctx, indices)
	if err != nil {
		return nil, err
	}
	indices = resolved

	if len(indices) == 0 {
		return nil, fmt.Errorf("no indices for cold search")
	}
	if len(indices) == 1 {
		return p.coldBackend.Search(ctx, indices[0], body)
	}

	type res struct {
		resp *backend.SearchResponse
		err  error
	}
	ch := make(chan res, len(indices))
	for _, idx := range indices {
		idx := idx
		go func() {
			r, err := p.coldBackend.Search(ctx, idx, body)
			ch <- res{resp: r, err: err}
		}()
	}

	var merged *backend.SearchResponse
	for i := 0; i < len(indices); i++ {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		merged = MergeSearchResponses(merged, r.resp)
	}
	return merged, nil
}

func (p *Proxy) handleMSearch(w http.ResponseWriter, r *http.Request, defaultIndices []string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// Restore body for potential passthrough.
	r.Body = io.NopCloser(bytes.NewReader(body))

	entries, parseErr := parseMSearchNDJSON(body, defaultIndices)
	if parseErr != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid msearch body","detail":%q}`, parseErr.Error()), http.StatusBadRequest)
		return
	}
	if len(entries) == 0 {
		http.Error(w, `{"error":"empty msearch"}`, http.StatusBadRequest)
		return
	}

	// If any entry has internal indices, passthrough for compatibility.
	for _, e := range entries {
		if hasInternal(e.Indices) || len(e.Indices) == 0 {
			p.reverseProxy.ServeHTTP(w, r)
			return
		}
	}

	authHeader := r.Header.Get("Authorization")
	needsCold := false
	for _, e := range entries {
		if p.routeForIndices(e.Body, e.Indices) != RouteHotOnly {
			needsCold = true
			break
		}
	}
	if needsCold {
		if authHeader == "" {
			http.Error(w, `{"error":"authentication required"}`, http.StatusUnauthorized)
			return
		}
		if err := p.authenticateViaOpenSearch(r.Context(), authHeader); err != nil {
			status := http.StatusBadGateway
			if isAuthError(err) {
				status = statusFromAuthError(err)
			}
			http.Error(w, `{"error":"authentication failed"}`, status)
			return
		}
	}

	out := make([]json.RawMessage, 0, len(entries))

	for _, e := range entries {
		target := p.routeForIndices(e.Body, e.Indices)
		needsMerge := target == RouteBoth || (target == RouteColdOnly && len(e.Indices) > 1)
		fanout := fanoutPlan{Body: e.Body, Merge: MergeOptions{}}
		var fanoutErr error
		if needsMerge {
			fanout, fanoutErr = planFanout(e.Body)
			if fanoutErr != nil {
				out = append(out, json.RawMessage(fmt.Sprintf(`{"error":{"reason":%q},"status":400}`, fanoutErr.Error())))
				continue
			}
		}

		switch target {
		case RouteHotOnly:
			resp, err := p.hotBackend.SearchAs(r.Context(), strings.Join(e.Indices, ","), e.Body, authHeader)
			if err != nil {
				status := 502
				if isAuthError(err) {
					status = statusFromAuthError(err)
				}
				out = append(out, json.RawMessage(fmt.Sprintf(`{"error":{"reason":%q},"status":%d}`, err.Error(), status)))
				continue
			}
			b, _ := json.Marshal(resp)
			out = append(out, b)
		case RouteColdOnly:
			queryBody := e.Body
			if needsMerge {
				queryBody = fanout.Body
			}
			resp, err := p.searchColdIndices(r.Context(), e.Indices, queryBody)
			if err != nil {
				out = append(out, json.RawMessage(fmt.Sprintf(`{"error":{"reason":%q},"status":502}`, err.Error())))
				continue
			}
			if needsMerge {
				resp = MergeSearchResponsesWithOptions(nil, resp, fanout.Merge)
			}
			b, _ := json.Marshal(resp)
			out = append(out, b)
		case RouteBoth:
			hotResp, hotErr := p.hotBackend.SearchAs(r.Context(), strings.Join(e.Indices, ","), fanout.Body, authHeader)
			coldResp, coldErr := p.searchColdIndices(r.Context(), e.Indices, fanout.Body)
			if hotErr != nil && coldErr != nil {
				out = append(out, json.RawMessage(fmt.Sprintf(`{"error":{"reason":%q},"status":502}`, "both backends failed")))
				continue
			}
			if isAuthError(hotErr) {
				out = append(out, json.RawMessage(fmt.Sprintf(`{"error":{"reason":"authentication failed"},"status":%d}`, statusFromAuthError(hotErr))))
				continue
			}
			merged := MergeSearchResponsesWithOptions(hotResp, coldResp, fanout.Merge)
			b, _ := json.Marshal(merged)
			out = append(out, b)
		}
	}

	writeJSON(w, map[string]any{"responses": out})
}

type msearchEntry struct {
	Indices []string
	Body    []byte
}

func parseMSearchNDJSON(body []byte, defaultIndices []string) ([]msearchEntry, error) {
	sc := bufio.NewScanner(bytes.NewReader(body))
	// Allow larger msearch bodies than the default 64K token limit.
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	var lines [][]byte
	for sc.Scan() {
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		lines = append(lines, cp)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(lines)%2 != 0 {
		return nil, fmt.Errorf("msearch expects even number of lines")
	}

	out := make([]msearchEntry, 0, len(lines)/2)
	for i := 0; i < len(lines); i += 2 {
		header := lines[i]
		query := lines[i+1]

		indices := defaultIndices
		var hdr map[string]any
		if err := json.Unmarshal(header, &hdr); err == nil {
			if v, ok := hdr["index"].(string); ok && v != "" {
				indices = splitIndices(v)
			}
		}
		out = append(out, msearchEntry{
			Indices: indices,
			Body:    query,
		})
	}
	return out, nil
}
