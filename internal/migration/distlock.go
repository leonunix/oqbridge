package migration

import (
	"context"
	"time"
)

// DistLock provides distributed locking for migration coordination.
// When multiple oqbridge-migrate instances run concurrently, a DistLock
// prevents them from migrating the same index simultaneously, avoiding
// duplicate data transfers to Quickwit.
type DistLock interface {
	// Acquire attempts to acquire a lock for the given key with the specified TTL.
	// Returns true if the lock was acquired, false if already held by another instance.
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// Release releases the lock for the given key.
	Release(ctx context.Context, key string) error
}
