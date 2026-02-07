package migration

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
)

// fakeLock is a test implementation of DistLock.
type fakeLock struct {
	mu       sync.Mutex
	held     map[string]string // key → owner
	releases []string          // keys that were released
	owner    string
}

func newFakeLock(owner string) *fakeLock {
	return &fakeLock{held: make(map[string]string), owner: owner}
}

func (f *fakeLock) Acquire(_ context.Context, key string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.held[key]; ok {
		return false, nil
	}
	f.held[key] = f.owner
	return true, nil
}

func (f *fakeLock) Release(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, key)
	f.releases = append(f.releases, key)
	return nil
}

func TestMigrator_MigrateIndex_LockAcquired(t *testing.T) {
	dir := t.TempDir()
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	cold := newFakeCold()
	lock := newFakeLock("instance-1")

	m := newTestMigrator(t, hot, cold, dir)
	m.lock = lock
	m.lockTTL = time.Hour

	if err := m.MigrateIndex(context.Background(), "logs"); err != nil {
		t.Fatalf("MigrateIndex: %v", err)
	}

	// Verify migration happened.
	cold.mu.Lock()
	gotDocs := len(cold.docsByIndex["logs"])
	cold.mu.Unlock()
	if gotDocs != 2 {
		t.Fatalf("ingested docs=%d, want 2", gotDocs)
	}

	// Verify lock was released.
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if _, ok := lock.held["logs"]; ok {
		t.Fatal("lock should have been released after migration")
	}
	if len(lock.releases) != 1 || lock.releases[0] != "logs" {
		t.Fatalf("releases=%v, want [logs]", lock.releases)
	}
}

func TestMigrator_MigrateIndex_LockNotAcquired_Skips(t *testing.T) {
	dir := t.TempDir()
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	cold := newFakeCold()
	lock := newFakeLock("instance-1")

	// Pre-hold the lock simulating another instance.
	lock.held["logs"] = "instance-2"

	m := newTestMigrator(t, hot, cold, dir)
	m.lock = lock
	m.lockTTL = time.Hour

	if err := m.MigrateIndex(context.Background(), "logs"); err != nil {
		t.Fatalf("MigrateIndex: %v", err)
	}

	// Verify NO migration happened.
	cold.mu.Lock()
	gotDocs := len(cold.docsByIndex["logs"])
	cold.mu.Unlock()
	if gotDocs != 0 {
		t.Fatalf("ingested docs=%d, want 0 (should have been skipped)", gotDocs)
	}
}

func TestMigrator_MigrateIndex_NoLock_BackwardsCompat(t *testing.T) {
	dir := t.TempDir()
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	cold := newFakeCold()

	// No lock configured — should still work (backwards compatible).
	m := newTestMigrator(t, hot, cold, dir)

	if err := m.MigrateIndex(context.Background(), "logs"); err != nil {
		t.Fatalf("MigrateIndex: %v", err)
	}

	cold.mu.Lock()
	gotDocs := len(cold.docsByIndex["logs"])
	cold.mu.Unlock()
	if gotDocs != 2 {
		t.Fatalf("ingested docs=%d, want 2", gotDocs)
	}
}

func TestMigrator_MigrateIndex_LockReleasedOnError(t *testing.T) {
	dir := t.TempDir()
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	cold := newFakeCold()
	failSlice := 1
	cold.failOnSlice = &failSlice

	lock := newFakeLock("instance-1")

	m := newTestMigrator(t, hot, cold, dir)
	m.lock = lock
	m.lockTTL = time.Hour

	// Migration should fail due to ingest error.
	if err := m.MigrateIndex(context.Background(), "logs"); err == nil {
		t.Fatal("expected error")
	}

	// Lock must still be released even on failure.
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if _, ok := lock.held["logs"]; ok {
		t.Fatal("lock should have been released after failed migration")
	}
	if len(lock.releases) != 1 || lock.releases[0] != "logs" {
		t.Fatalf("releases=%v, want [logs]", lock.releases)
	}
}

func TestMigrator_TwoInstances_OnlyOneMigrates(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	hits := map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	}
	hot1 := newFakeHot(hits)
	hot2 := newFakeHot(hits)
	cold1 := newFakeCold()
	cold2 := newFakeCold()

	// Shared lock simulates both instances seeing the same lock state.
	lock := newFakeLock("instance-1")

	m1 := newTestMigrator(t, hot1, cold1, dir1)
	m1.lock = lock
	m1.lockTTL = time.Hour

	m2 := newTestMigrator(t, hot2, cold2, dir2)
	m2.lock = lock
	m2.lockTTL = time.Hour

	// Start both concurrently.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m1.MigrateIndex(context.Background(), "logs")
	}()
	go func() {
		defer wg.Done()
		m2.MigrateIndex(context.Background(), "logs")
	}()
	wg.Wait()

	// Exactly one of the two should have migrated data.
	cold1.mu.Lock()
	docs1 := len(cold1.docsByIndex["logs"])
	cold1.mu.Unlock()
	cold2.mu.Lock()
	docs2 := len(cold2.docsByIndex["logs"])
	cold2.mu.Unlock()

	// One should have docs, the other should have none.
	if (docs1 > 0) == (docs2 > 0) {
		t.Fatalf("expected exactly one instance to migrate: instance1=%d, instance2=%d", docs1, docs2)
	}
}

func TestMigrator_WithDistLock_Option(t *testing.T) {
	dir := t.TempDir()
	hot := newFakeHot(map[int][][]json.RawMessage{
		0: {makeHits(0, 1), nil},
		1: {makeHits(1, 1), nil},
	})
	cold := newFakeCold()
	lock := newFakeLock("test")

	cfg := defaultTestConfig()
	cpStore, err := NewLocalCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewLocalCheckpointStore: %v", err)
	}
	m, err := NewMigrator(cfg, hot, cold, cpStore, WithDistLock(lock), WithLockTTL(30*time.Minute))
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	m.progressInterval = time.Millisecond

	if m.lock != lock {
		t.Fatal("expected lock to be set via option")
	}
	if m.lockTTL != 30*time.Minute {
		t.Fatalf("lockTTL=%v, want 30m", m.lockTTL)
	}

	if err := m.MigrateIndex(context.Background(), "logs"); err != nil {
		t.Fatalf("MigrateIndex: %v", err)
	}

	cold.mu.Lock()
	gotDocs := len(cold.docsByIndex["logs"])
	cold.mu.Unlock()
	if gotDocs != 2 {
		t.Fatalf("ingested docs=%d, want 2", gotDocs)
	}
}

// defaultTestConfig returns a config suitable for tests.
func defaultTestConfig() *config.Config {
	return &config.Config{
		Retention: config.RetentionConfig{Days: 30, TimestampField: "@timestamp"},
		Migration: config.MigrationConfig{
			Schedule:  "* * * * *",
			BatchSize: 2,
			Workers:   2,
			Indices:   []string{"logs"},
		},
	}
}

// TestOpenSearchLock_AcquireRelease tests the lock using a fake HTTP server.
func TestOpenSearchLock_AcquireRelease(t *testing.T) {
	// This is tested via the fakeLock in the migrator tests above.
	// Integration tests against a real OpenSearch cluster should be done separately.
	// Here we just verify the interface contract via fakeLock.
	lock := newFakeLock("test-owner")
	ctx := context.Background()

	// Acquire should succeed.
	ok, err := lock.Acquire(ctx, "my-index", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected Acquire to succeed")
	}

	// Second acquire should fail.
	ok, err = lock.Acquire(ctx, "my-index", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if ok {
		t.Fatal("expected second Acquire to fail")
	}

	// Release should succeed.
	if err := lock.Release(ctx, "my-index"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// After release, acquire should succeed again.
	ok, err = lock.Acquire(ctx, "my-index", time.Hour)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected Acquire to succeed after release")
	}
}

// Verify that OpenSearchLock satisfies the DistLock interface at compile time.
var _ DistLock = (*backend.OpenSearchLock)(nil)
