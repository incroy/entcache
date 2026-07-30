package lrucache_test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/incroy/entcache"
	"github.com/incroy/entcache/lrucache"
)

func TestLRUCache(t *testing.T) {
	ctx := context.Background()
	c, err := lrucache.New(10)
	if err != nil {
		t.Fatalf("failed to create lrucache: %v", err)
	}

	entry := &entcache.Entry{
		Columns: []string{"id", "name"},
		Values:  [][]driver.Value{{int64(1), "alice"}},
	}

	// 1. Test Add and Get
	if err := c.Add(ctx, "user:1", entry, time.Minute); err != nil {
		t.Fatalf("failed to add entry: %v", err)
	}

	got, err := c.Get(ctx, "user:1")
	if err != nil {
		t.Fatalf("failed to get entry: %v", err)
	}
	if len(got.Columns) != 2 || got.Columns[0] != "id" {
		t.Errorf("unexpected columns: %v", got.Columns)
	}

	// 2. Test Del
	if err := c.Del(ctx, "user:1"); err != nil {
		t.Fatalf("failed to delete entry: %v", err)
	}
	if _, err := c.Get(ctx, "user:1"); err != entcache.ErrNotFound {
		t.Errorf("expected ErrNotFound after Del, got: %v", err)
	}

	// 3. Test Expiry
	if err := c.Add(ctx, "temp:1", entry, 10*time.Millisecond); err != nil {
		t.Fatalf("failed to add expiring entry: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := c.Get(ctx, "temp:1"); err != entcache.ErrNotFound {
		t.Errorf("expected ErrNotFound for expired entry, got: %v", err)
	}
}
