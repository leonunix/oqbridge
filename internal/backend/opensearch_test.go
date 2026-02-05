package backend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenSearch_Authenticate_StatusCodes(t *testing.T) {
	const token = "Basic dTpw" // u:p

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_plugins/_security/authinfo" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Header.Get("Authorization") {
		case token:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"user":"u"}`))
		case "force-500":
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"oops"}`))
		default:
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
		}
	}))
	defer srv.Close()

	os := NewOpenSearch(srv.URL, "", "")

	if err := os.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if err := os.Authenticate(context.Background(), "bad"); err == nil {
		t.Fatalf("expected error for unauthorized")
	} else {
		var httpErr *HTTPStatusError
		if !strings.Contains(err.Error(), "status") && !strings.Contains(err.Error(), "returned") {
			// Keep this loose; just make sure we get a useful error.
			t.Fatalf("unexpected error string: %v", err)
		}
		if !asHTTPStatusError(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected HTTPStatusError 401, got %#v", err)
		}
	}

	if err := os.Authenticate(context.Background(), "force-500"); err == nil {
		t.Fatalf("expected error for 500")
	} else {
		var httpErr *HTTPStatusError
		if !asHTTPStatusError(err, &httpErr) || httpErr.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected HTTPStatusError 500, got %#v", err)
		}
	}
}

func TestOpenSearch_SearchAs_AuthHeaderVsServiceAccount(t *testing.T) {
	const token = "Basic dXNlcjpwYXNz"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_search") {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		auth := r.Header.Get("Authorization")
		user, pass, ok := r.BasicAuth()

		switch {
		case auth == token:
			// ok
		case ok && user == "svc" && pass == "pw":
			// ok
		default:
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SearchResponse{
			Took: 1,
			Hits: HitsResult{Total: HitsTotal{Value: 1, Relation: "eq"}},
		})
	}))
	defer srv.Close()

	os := NewOpenSearch(srv.URL, "svc", "pw")
	body := []byte(`{"query":{"match_all":{}}}`)

	if _, err := os.SearchAs(context.Background(), "idx", body, token); err != nil {
		t.Fatalf("SearchAs with auth header: %v", err)
	}
	if _, err := os.SearchAs(context.Background(), "idx", body, ""); err != nil {
		t.Fatalf("SearchAs with service account: %v", err)
	}
}

func TestOpenSearch_SearchAs_Non2xxReturnsHTTPStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_search") {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"forbidden"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	os := NewOpenSearch(srv.URL, "", "")
	_, err := os.SearchAs(context.Background(), "idx", []byte(`{}`), "Basic whatever")
	if err == nil {
		t.Fatalf("expected error")
	}
	var httpErr *HTTPStatusError
	if !asHTTPStatusError(err, &httpErr) {
		t.Fatalf("expected HTTPStatusError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode=%d, want %d", httpErr.StatusCode, http.StatusForbidden)
	}
	if !strings.Contains(httpErr.Body, "forbidden") {
		t.Fatalf("Body=%q, want to contain %q", httpErr.Body, "forbidden")
	}
}

func TestOpenSearch_SlicedScroll_InsertsSliceClause(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_search") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.URL.Query().Get("scroll"); got != "10m" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"missing scroll"}`))
			return
		}

		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sliceAny, ok := m["slice"].(map[string]any)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"missing slice"}`))
			return
		}
		if int(sliceAny["id"].(float64)) != 1 || int(sliceAny["max"].(float64)) != 4 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"bad slice"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"_scroll_id":"sid","hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`))
	}))
	defer srv.Close()

	os := NewOpenSearch(srv.URL, "svc", "pw")

	res, err := os.SlicedScroll(context.Background(), "idx", []byte(`{"size":1,"query":{"match_all":{}}}`), "", &SlicedScrollConfig{
		SliceID:    1,
		SliceMax:   4,
		ScrollKeep: "10m",
	})
	if err != nil {
		t.Fatalf("SlicedScroll: %v", err)
	}
	if res.ScrollID != "sid" {
		t.Fatalf("ScrollID=%q, want %q", res.ScrollID, "sid")
	}
}

func asHTTPStatusError(err error, target **HTTPStatusError) bool {
	if err == nil {
		return false
	}
	var httpErr *HTTPStatusError
	if !errors.As(err, &httpErr) {
		return false
	}
	*target = httpErr
	return true
}
