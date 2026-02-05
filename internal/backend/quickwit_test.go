package backend

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQuickwit_Search_Endpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/logs/search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SearchResponse{
			Took: 1,
			Hits: HitsResult{Total: HitsTotal{Value: 0, Relation: "eq"}},
		})
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", false)
	if _, err := qw.Search(context.Background(), "logs", []byte(`{"query":{"match_all":{}}}`)); err != nil {
		t.Fatalf("Search: %v", err)
	}
}

func TestQuickwit_BulkIngest_GzipCompression(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/logs/ingest" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Content-Type") != "application/x-ndjson" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"bad content-type"}`))
			return
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"missing gzip"}`))
			return
		}

		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer gz.Close()
		b, _ := io.ReadAll(gz)

		want := "{\"a\":1}\n{\"b\":2}\n"
		if string(b) != want {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"bad body"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", true)

	docs := []json.RawMessage{
		json.RawMessage(`{"_source":{"a":1}}`),
		json.RawMessage(`{"_source":{"b":2}}`),
	}
	if err := qw.BulkIngest(context.Background(), "logs", docs); err != nil {
		t.Fatalf("BulkIngest: %v", err)
	}
}

func TestQuickwit_Search_Non2xxReturnsHTTPStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/search") {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"nope"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", false)
	_, err := qw.Search(context.Background(), "logs", []byte(`{}`))
	if err == nil {
		t.Fatalf("expected error")
	}
	var httpErr *HTTPStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode=%d, want %d", httpErr.StatusCode, http.StatusUnauthorized)
	}
}
