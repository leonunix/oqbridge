package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const lockIndex = ".oqbridge-locks"

// OpenSearchLock implements distributed locking using OpenSearch documents.
// It uses op_type=create for atomic lock acquisition and optimistic
// concurrency control (_seq_no + _primary_term) for safe expired-lock cleanup.
type OpenSearchLock struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	owner    string
}

// NewOpenSearchLock creates a new OpenSearchLock.
// The lock uses the same OpenSearch cluster that stores hot data, so no
// additional infrastructure is required.
func NewOpenSearchLock(baseURL, username, password string, httpClient *http.Client) *OpenSearchLock {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	return &OpenSearchLock{
		baseURL:  baseURL,
		username: username,
		password: password,
		client:   httpClient,
		owner:    owner,
	}
}

type lockDoc struct {
	Owner      string    `json:"owner"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// Acquire attempts to acquire a lock for the given key with the specified TTL.
// It first tries to clean up any expired lock, then atomically creates a lock
// document using op_type=create (which returns 409 if the document already exists).
func (l *OpenSearchLock) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	// Try to clean up any expired lock first.
	if err := l.cleanupExpired(ctx, key); err != nil {
		slog.Debug("lock cleanup failed (non-fatal)", "key", key, "error", err)
	}

	acquired, err := l.tryCreate(ctx, key, ttl)
	if err != nil && isIndexMissing(err) {
		// Lock index doesn't exist yet (auto_create_index may be disabled).
		if createErr := l.ensureIndex(ctx); createErr != nil {
			return false, fmt.Errorf("creating lock index: %w", createErr)
		}
		return l.tryCreate(ctx, key, ttl)
	}
	return acquired, err
}

// Release releases the lock for the given key.
func (l *OpenSearchLock) Release(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s/_doc/%s?refresh=true", l.baseURL, lockIndex, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("creating release request: %w", err)
	}
	l.setAuth(req)

	resp, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing release request: %w", err)
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)

	// 404 is fine — lock may have already been released or expired.
	if resp.StatusCode >= 400 && resp.StatusCode != 404 {
		return fmt.Errorf("lock release failed: status=%d", resp.StatusCode)
	}
	return nil
}

// indexMissingError is returned when the lock index does not exist.
type indexMissingError struct {
	msg string
}

func (e *indexMissingError) Error() string { return e.msg }

func isIndexMissing(err error) bool {
	_, ok := err.(*indexMissingError)
	return ok
}

// tryCreate attempts to create a lock document atomically.
func (l *OpenSearchLock) tryCreate(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	doc := lockDoc{
		Owner:      l.owner,
		AcquiredAt: time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(ttl),
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return false, fmt.Errorf("marshaling lock doc: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_doc/%s?op_type=create&refresh=true", l.baseURL, lockIndex, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("creating lock request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	l.setAuth(req)

	resp, err := l.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("executing lock request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	switch {
	case resp.StatusCode == 409:
		// Document already exists — lock held by another instance.
		return false, nil
	case resp.StatusCode == 404:
		// Index does not exist.
		return false, &indexMissingError{msg: fmt.Sprintf("lock index %s does not exist", lockIndex)}
	case resp.StatusCode >= 400:
		return false, fmt.Errorf("lock acquire failed: status=%d body=%s", resp.StatusCode, string(respBody))
	default:
		return true, nil
	}
}

// cleanupExpired checks if the lock for key is expired and deletes it using
// optimistic concurrency control to avoid races with other instances.
func (l *OpenSearchLock) cleanupExpired(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s/_doc/%s", l.baseURL, lockIndex, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	l.setAuth(req)

	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode == 404 {
		return nil
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("get lock failed: status=%d", resp.StatusCode)
	}

	var result struct {
		Found    bool    `json:"found"`
		Source   lockDoc `json:"_source"`
		SeqNo   int64   `json:"_seq_no"`
		PrimTerm int64  `json:"_primary_term"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return err
	}

	if !result.Found {
		return nil
	}

	if time.Now().UTC().After(result.Source.ExpiresAt) {
		slog.Info("cleaning up expired migration lock",
			"key", key,
			"owner", result.Source.Owner,
			"expired_at", result.Source.ExpiresAt,
		)
		delURL := fmt.Sprintf("%s/%s/_doc/%s?if_seq_no=%d&if_primary_term=%d&refresh=true",
			l.baseURL, lockIndex, key, result.SeqNo, result.PrimTerm)
		delReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, delURL, nil)
		if err != nil {
			return err
		}
		l.setAuth(delReq)

		delResp, err := l.client.Do(delReq)
		if err != nil {
			return err
		}
		defer delResp.Body.Close()
		io.ReadAll(delResp.Body)
		// 409 is fine — another instance may have cleaned it up already.
	}
	return nil
}

// ensureIndex creates the lock index if it doesn't exist.
func (l *OpenSearchLock) ensureIndex(ctx context.Context) error {
	url := fmt.Sprintf("%s/%s", l.baseURL, lockIndex)
	body := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":1}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	l.setAuth(req)

	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	// 400 with "resource_already_exists_exception" is fine — another instance may
	// have created it concurrently.
	if resp.StatusCode == 400 && bytes.Contains(respBody, []byte("resource_already_exists_exception")) {
		return nil
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("creating lock index: status=%d body=%s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (l *OpenSearchLock) setAuth(req *http.Request) {
	if l.username != "" {
		req.SetBasicAuth(l.username, l.password)
	}
}
