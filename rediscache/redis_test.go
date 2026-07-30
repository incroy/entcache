package rediscache_test

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/incroy/entcache"
	"github.com/incroy/entcache/rediscache"
)

func TestRedisCache(t *testing.T) {
	ctx := context.Background()
	rdb, mock := redismock.NewClientMock()
	c := rediscache.New(rdb)

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(42)}},
	}
	buf, _ := entry.MarshalBinary()

	// 1. Get Miss
	mock.ExpectGet("user:42").RedisNil()
	if _, err := c.Get(ctx, "user:42"); err != entcache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 2. Add
	mock.ExpectSet("user:42", buf, time.Minute).SetVal("OK")
	if err := c.Add(ctx, "user:42", entry, time.Minute); err != nil {
		t.Fatalf("failed to Add: %v", err)
	}

	// 3. Get Hit
	mock.ExpectGet("user:42").SetVal(string(buf))
	got, err := c.Get(ctx, "user:42")
	if err != nil {
		t.Fatalf("failed to Get: %v", err)
	}
	if len(got.Values) != 1 || got.Values[0][0] != int64(42) {
		t.Errorf("unexpected value in entry: %v", got.Values)
	}

	// 4. Del
	mock.ExpectDel("user:42").SetVal(1)
	if err := c.Del(ctx, "user:42"); err != nil {
		t.Fatalf("failed to Del: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisLockOrWait_Stampede(t *testing.T) {
	ctx := context.Background()
	rdb, mock := redismock.NewClientMock()
	c := rediscache.New(rdb)
	key := "test:redis:stampede"

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
		t.Fatalf("expected second call to lose lock")
	}

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(99)}},
	}
	buf, _ := entry.MarshalBinary()

	mock.ExpectSet(key, buf, time.Minute).SetVal("OK")
	mock.ExpectGet(key).SetVal(string(buf))

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
	if waiterEntry == nil || len(waiterEntry.Values) == 0 || waiterEntry.Values[0][0] != int64(99) {
		t.Fatalf("waiter received wrong entry: %v", waiterEntry)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	_ = wait1
}
