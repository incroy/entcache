package rediscache_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/incroy/entcache"
	"github.com/incroy/entcache/rediscache"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisCache(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()
	c := rediscache.New(rdb)

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(42)}},
	}

	// 1. Get Miss
	_, err = c.Get(ctx, "user:42")
	require.ErrorIs(t, err, entcache.ErrNotFound)

	// 2. Add
	err = c.Add(ctx, "user:42", entry, time.Minute)
	require.NoError(t, err)

	// 3. Get Hit
	got, err := c.Get(ctx, "user:42")
	require.NoError(t, err)
	require.Len(t, got.Values, 1)
	require.Equal(t, int64(42), got.Values[0][0])

	// 4. Del
	err = c.Del(ctx, "user:42")
	require.NoError(t, err)

	// 5. Get Miss again
	_, err = c.Get(ctx, "user:42")
	require.ErrorIs(t, err, entcache.ErrNotFound)
}

func TestRedisLockOrWait_Timeout(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()
	c := rediscache.New(rdb)
	key := "test:redis:timeout"

	// 1. Winner acquires lock
	won, wait, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)
	require.Nil(t, wait)
	require.NotNil(t, release)

	// 2. Waiter starts waiting
	var wg sync.WaitGroup
	wg.Add(1)
	var waiterErr error
	go func() {
		defer wg.Done()
		won2, wait2, _, err2 := c.LockOrWait(ctx, key)
		if err2 != nil {
			waiterErr = err2
			return
		}
		if won2 {
			waiterErr = errors.New("waiter incorrectly won the lock")
			return
		}
		_, waiterErr = wait2(ctx)
	}()

	// 3. Winner crashes (we don't call release, and heartbeat should die when we close something,
	// but we actually just need to wait for the internal lock TTL to expire if the heartbeat stops).
	// For testing, let's just delete the internal lock key directly to simulate expiration/crash.
	time.Sleep(100 * time.Millisecond) // let waiter subscribe
	s.Del("lock:" + key)

	wg.Wait()
	// Waiter should wake up (either by seeing the key deleted or polling) and return ErrRetryLocker
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)

	release(ctx) // cleanup
}

func TestRedisLockOrWait_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()
	c := rediscache.New(rdb)
	key := "test:redis:cancel"

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	waiterCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		won2, wait2, _, err2 := c.LockOrWait(waiterCtx, key)
		if err2 != nil {
			waiterErr = err2
			return
		}
		if won2 {
			waiterErr = errors.New("waiter incorrectly won the lock")
			return
		}
		_, waiterErr = wait2(waiterCtx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel() // cancel waiter

	wg.Wait()
	require.ErrorIs(t, waiterErr, context.Canceled)
	release(ctx)
}

func TestRedisCache_Close(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()
	c := rediscache.New(rdb)
	require.NoError(t, c.Close())
}
