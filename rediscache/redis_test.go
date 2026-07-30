package rediscache_test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/incroy/entcache"
	"github.com/incroy/entcache/rediscache"
	"github.com/go-redis/redismock/v9"
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
