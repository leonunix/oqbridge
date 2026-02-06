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

	qw := NewQuickwit(srv.URL, "", "", false, nil)
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

	qw := NewQuickwit(srv.URL, "", "", true, nil)

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

	qw := NewQuickwit(srv.URL, "", "", false, nil)
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

func TestQuickwit_IndexExists_Found(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/indexes/logs" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"index_id":"logs"}`))
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", false, nil)
	exists, err := qw.IndexExists(context.Background(), "logs")
	if err != nil {
		t.Fatalf("IndexExists: %v", err)
	}
	if !exists {
		t.Fatalf("expected index to exist")
	}
}

func TestQuickwit_IndexExists_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", false, nil)
	exists, err := qw.IndexExists(context.Background(), "logs")
	if err != nil {
		t.Fatalf("IndexExists: %v", err)
	}
	if exists {
		t.Fatalf("expected index to not exist")
	}
}

func TestQuickwit_CreateIndex_Success(t *testing.T) {
	var receivedBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/indexes" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", false, nil)
	if err := qw.CreateIndex(context.Background(), "logs", "@timestamp"); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	if receivedBody["index_id"] != "logs" {
		t.Fatalf("index_id=%v, want logs", receivedBody["index_id"])
	}
	docMapping := receivedBody["doc_mapping"].(map[string]interface{})
	if docMapping["mode"] != "dynamic" {
		t.Fatalf("mode=%v, want dynamic", docMapping["mode"])
	}
	if docMapping["timestamp_field"] != "@timestamp" {
		t.Fatalf("timestamp_field=%v, want @timestamp", docMapping["timestamp_field"])
	}
}

func TestQuickwit_CreateIndex_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"index already exists"}`))
	}))
	defer srv.Close()

	qw := NewQuickwit(srv.URL, "", "", false, nil)
	err := qw.CreateIndex(context.Background(), "logs", "@timestamp")
	if err == nil {
		t.Fatalf("expected error")
	}
	var httpErr *HTTPStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
}
