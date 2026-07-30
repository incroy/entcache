package contextcache_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/incroy/entcache"
	"github.com/incroy/entcache/contextcache"
)

func TestContextCache_BasicOperations(t *testing.T) {
	c := contextcache.New()
	ctx := contextcache.NewContext(context.Background())

	entry := &entcache.Entry{
		Columns: []string{"id", "name"},
		Values:  [][]driver.Value{{int64(1), "alice"}},
	}

	// 1. Get Miss
	_, err := c.Get(ctx, "user:1")
	if !errors.Is(err, entcache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 2. Add
	if err := c.Add(ctx, "user:1", entry, time.Minute); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// 3. Get Hit
	got, err := c.Get(ctx, "user:1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(got.Values) != 1 || got.Values[0][1] != "alice" {
		t.Fatalf("unexpected entry values: %v", got)
	}

	// 4. Del
	if err := c.Del(ctx, "user:1"); err != nil {
		t.Fatalf("Del failed: %v", err)
	}
	_, err = c.Get(ctx, "user:1")
	if !errors.Is(err, entcache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Del, got %v", err)
	}
}

func TestContextLockOrWait_Stampede(t *testing.T) {
	c := contextcache.New()
	ctx := contextcache.NewContext(context.Background())
	key := "user:10"

	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	if err1 != nil {
		t.Fatalf("first LockOrWait failed: %v", err1)
	}
	if !won1 {
		t.Fatalf("expected first call to win lock")
	}

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	if err2 != nil {
		t.Fatalf("second LockOrWait failed: %v", err2)
	}
	if won2 {
		t.Fatalf("expected second call to lose lock and become waiter")
	}

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(10)}},
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
		t.Fatalf("waiter returned error: %v", waiterErr)
	}
	if waiterEntry == nil || len(waiterEntry.Values) == 0 || waiterEntry.Values[0][0] != int64(10) {
		t.Fatalf("waiter received incorrect entry: %v", waiterEntry)
	}
	_ = wait1
}
