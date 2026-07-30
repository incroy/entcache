package lrucache_test

import (
	"context"
	"database/sql/driver"
	"sync"
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

func TestLRULockOrWait_Stampede(t *testing.T) {
	c := lrucache.MustNew(100)
	ctx := context.Background()
	key := "test:stampede:1"

	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	if err1 != nil {
		t.Fatalf("first LockOrWait failed: %v", err1)
	}
	if !won1 {
		t.Fatalf("expected first caller to win lock")
	}

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	if err2 != nil {
		t.Fatalf("second LockOrWait failed: %v", err2)
	}
	if won2 {
		t.Fatalf("expected second caller to lose lock")
	}

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(42)}},
	}

	var wg sync.WaitGroup
	var waiterEntry *entcache.Entry
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiterEntry, waiterErr = wait2(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	if err := c.Add(ctx, key, entry, time.Minute); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if release1 != nil {
		release1(ctx)
	}

	wg.Wait()

	if waiterErr != nil {
		t.Fatalf("waiter error: %v", waiterErr)
	}
	if waiterEntry == nil || len(waiterEntry.Values) == 0 || waiterEntry.Values[0][0] != int64(42) {
		t.Fatalf("waiter received wrong entry: %v", waiterEntry)
	}
	_ = wait1
}
