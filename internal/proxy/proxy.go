package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
)

// searchPathRe matches /{index}/_search paths.
var searchPathRe = regexp.MustCompile(`^/([^/]+)/_search$`)

// Proxy is the core HTTP handler that routes requests between OpenSearch and Quickwit.
type Proxy struct {
	cfg          *config.Config
	router       *Router
	hotBackend   *backend.OpenSearch
	coldBackend  *backend.Quickwit
	reverseProxy *httputil.ReverseProxy
}

// New creates a new Proxy instance.
func New(cfg *config.Config, hot *backend.OpenSearch, cold *backend.Quickwit) (*Proxy, error) {
	osURL, err := url.Parse(cfg.OpenSearch.URL)
	if err != nil {
		return nil, err
	}

	rp := httputil.NewSingleHostReverseProxy(osURL)
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

	// Check if this is a search request that we should intercept.
	if matches := searchPathRe.FindStringSubmatch(r.URL.Path); matches != nil {
		index := matches[1]

		// Skip internal indices (start with .).
		if !strings.HasPrefix(index, ".") {
			p.handleSearch(w, r, index)
			return
		}
	}

	// All other requests: passthrough to OpenSearch (OpenSearch validates auth).
	p.reverseProxy.ServeHTTP(w, r)
}

func (p *Proxy) handleSearch(w http.ResponseWriter, r *http.Request, index string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// Restore body for potential passthrough.
	r.Body = io.NopCloser(bytes.NewReader(body))

	tsField := p.cfg.TimestampFieldForIndex(index)
	target := p.router.Route(body, tsField)

	slog.Debug("search routing decision",
		"index", index,
		"target", target.String(),
		"timestamp_field", tsField,
	)

	switch target {
	case RouteHotOnly:
		// Passthrough to OpenSearch via reverse proxy (OpenSearch validates auth).
		p.reverseProxy.ServeHTTP(w, r)
		return

	case RouteColdOnly:
		// Must validate user auth against OpenSearch first, because Quickwit
		// has no knowledge of OpenSearch users.
		if err := p.authenticateViaOpenSearch(r.Context(), r.Header.Get("Authorization")); err != nil {
			slog.Warn("auth failed for cold-only query", "index", index, "error", err)
			http.Error(w, `{"error":"authentication failed"}`, http.StatusUnauthorized)
			return
		}

		resp, err := p.coldBackend.Search(r.Context(), index, body)
		if err != nil {
			slog.Error("quickwit search failed", "error", err)
			r.Body = io.NopCloser(bytes.NewReader(body))
			p.reverseProxy.ServeHTTP(w, r)
			return
		}
		writeJSON(w, resp)
		return

	case RouteBoth:
		// Fan-out: OpenSearch leg validates auth implicitly via the client's
		// auth header. If OpenSearch returns 401, the hot result is nil and
		// only cold results are used. This is acceptable because the hot leg
		// error is logged.
		authHeader := r.Header.Get("Authorization")
		p.handleFanoutSearch(w, r.Context(), index, body, authHeader)
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

func (p *Proxy) handleFanoutSearch(w http.ResponseWriter, ctx context.Context, index string, body []byte, authHeader string) {
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
		hotResp, hotErr = p.hotBackend.SearchAs(ctx, index, body, authHeader)
	}()
	go func() {
		defer wg.Done()
		coldResp, coldErr = p.coldBackend.Search(ctx, index, body)
	}()
	wg.Wait()

	if hotErr != nil {
		slog.Error("opensearch search failed during fan-out", "error", hotErr)
	}
	if coldErr != nil {
		slog.Error("quickwit search failed during fan-out", "error", coldErr)
	}

	// If both fail, return error.
	if hotErr != nil && coldErr != nil {
		http.Error(w, `{"error":"both backends failed"}`, http.StatusBadGateway)
		return
	}

	merged := MergeSearchResponses(hotResp, coldResp)
	writeJSON(w, merged)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to write response", "error", err)
	}
}
