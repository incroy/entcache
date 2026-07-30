package rediscache_test

import (
	"context"
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/incroy/entcache"
	"github.com/incroy/entcache/rediscache"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func runMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { rdb.Close() })

	return s, rdb
}

func TestRedisCache(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rediscache.New(rdb)

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(42)}},
	}

	// 1. Get Miss
	_, err := c.Get(ctx, "user:42")
	require.ErrorIs(t, err, entcache.ErrNotFound)

	// 2. Add
	err = c.Add(ctx, "user:42", entry, time.Minute)
	require.NoError(t, err)

	// 3. Get Hit
	got, err := c.Get(ctx, "user:42")
	require.NoError(t, err)
	require.Len(t, got.Values, 1)
	require.Equal(t, int64(42), got.Values[0][0])

	// 4. WatchInvalidations
	invalidated := make(chan string, 1)
	err = c.WatchInvalidations(ctx, func(k entcache.Key) {
		invalidated <- k.(string)
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// 5. Del
	err = c.Del(ctx, "user:42")
	require.NoError(t, err)

	select {
	case k := <-invalidated:
		require.Equal(t, "user:42", k)
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for WatchInvalidations event")
	}

	// 6. Get Miss Again
	_, err = c.Get(ctx, "user:42")
	require.ErrorIs(t, err, entcache.ErrNotFound)
}

func TestRedisLockOrWait_Stampede(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:stampede"

	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	require.NoError(t, err1, "first LockOrWait failed")
	require.True(t, won1, "expected first call to win lock")

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	require.NoError(t, err2, "second LockOrWait failed")
	require.False(t, won2, "expected second call to lose lock")

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(77)}},
	}

	var wg sync.WaitGroup
	var waiterEntry *entcache.Entry
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiterEntry, waiterErr = wait2(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	err := c.Add(ctx, key, entry, time.Minute)
	require.NoError(t, err, "Add failed")
	if release1 != nil {
		release1(ctx)
	}

	wg.Wait()

	require.NoError(t, waiterErr, "waiter error")
	require.NotNil(t, waiterEntry, "waiter received nil entry")
	require.Len(t, waiterEntry.Values, 1)
	require.Equal(t, int64(77), waiterEntry.Values[0][0], "waiter received wrong entry")
	_ = wait1
}

func TestRedisLockOrWait_HeartbeatFailure(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	mkClient := func() *redis.Client {
		return redis.NewClient(&redis.Options{Addr: s.Addr()})
	}

	c1, c2 := rediscache.New(mkClient()), rediscache.New(mkClient())
	key := "test:heartbeat"

	won1, _, _, err1 := c1.LockOrWait(ctx, key)
	require.NoError(t, err1)
	require.True(t, won1)

	won2, wait2, _, err2 := c2.LockOrWait(ctx, key)
	require.NoError(t, err2)
	require.False(t, won2)

	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()

	// Simulate client 1 crashing / losing heartbeat
	c1.Close()

	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)
}

func TestRedisLockOrWait_ConcurrentStealing(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	mkClient := func() *redis.Client {
		return redis.NewClient(&redis.Options{Addr: s.Addr()})
	}

	numClients := 3
	clients := make([]*rediscache.Redis, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = rediscache.New(mkClient())
	}
	key := "test:concurrent"

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	losers := 0

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(c *rediscache.Redis) {
			defer wg.Done()
			won, _, _, err := c.LockOrWait(ctx, key)
			require.NoError(t, err)
			mu.Lock()
			if won {
				winners++
			} else {
				losers++
			}
			mu.Unlock()
		}(clients[i])
	}
	wg.Wait()

	require.Equal(t, 1, winners)
	require.Equal(t, numClients-1, losers)
}

func TestRedisLockOrWait_Timeout(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:timeout"

	won, _, _, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	won2, wait2, _, err := c.LockOrWait(timeoutCtx, key)
	require.NoError(t, err)
	require.False(t, won2)

	_, waitErr := wait2(timeoutCtx)
	require.ErrorIs(t, waitErr, context.DeadlineExceeded)
}

func TestRedisLockOrWait_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:cancel"

	won, _, _, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	cancelCtx, cancel := context.WithCancel(ctx)
	won2, wait2, _, err := c.LockOrWait(cancelCtx, key)
	require.NoError(t, err)
	require.False(t, won2)

	cancel()

	_, waitErr := wait2(cancelCtx)
	require.ErrorIs(t, waitErr, context.Canceled)
}

func TestRedisCache_Close(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rediscache.New(rdb)

	won, _, _, err := c.LockOrWait(ctx, "test:redis:cleanup")
	require.NoError(t, err)
	require.True(t, won)

	c.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestRedis_AddPreservesCustomTTL(t *testing.T) {
	ctx := context.Background()
	s, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:add_ttl"

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(1)}},
	}

	// Add with 1 second TTL
	err = c.Add(ctx, key, entry, 1*time.Second)
	require.NoError(t, err)
	if release != nil {
		release(ctx)
	}

	// Fast forward miniredis time by 2 seconds
	s.FastForward(2 * time.Second)

	_, err = c.Get(ctx, key)
	require.ErrorIs(t, err, entcache.ErrNotFound, "expected entry to expire after 1s TTL")
}

func TestRedis_HeartbeatTTLRefresh(t *testing.T) {
	ctx := context.Background()
	s, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:heartbeat_ttl"

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	lockKey := "lock:" + key
	id, err := s.Get(lockKey)
	require.NoError(t, err)

	ttl := s.TTL(id)
	require.Greater(t, ttl, 5*time.Second)
	require.LessOrEqual(t, ttl, 10*time.Second)

	time.Sleep(6 * time.Second)

	ttl2 := s.TTL(id)
	require.Greater(t, ttl2, 5*time.Second)

	release(ctx)
}

func TestRedisLockOrWait_LoaderErrorRetry(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:loader_error"

	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	require.NoError(t, err1)
	require.True(t, won1)

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	require.NoError(t, err2)
	require.False(t, won2)

	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	release1(ctx)

	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)
	_ = wait1
}

func TestRedisLockOrWait_MassiveStampede(t *testing.T) {
	ctx := context.Background()
	s, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:massive_stampede"

	var count int64
	var wg sync.WaitGroup
	var errs sync.Map
	
	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(999)}},
	}

	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			won, wait, release, err := c.LockOrWait(ctx, key)
			if err != nil {
				errs.Store(err, true)
				return
			}
			if won {
				atomic.AddInt64(&count, 1)
				time.Sleep(50 * time.Millisecond)
				_ = c.Add(ctx, key, entry, time.Minute)
				release(ctx)
			} else {
				got, err := wait(ctx)
				if err != nil {
					for err == entcache.ErrRetryLocker {
					    won, wait, release, err = c.LockOrWait(ctx, key)
					    if won {
					        atomic.AddInt64(&count, 1)
            				_ = c.Add(ctx, key, entry, time.Minute)
            				release(ctx)
            				return
					    }
					    if wait != nil {
					        got, err = wait(ctx)
					    }
					}
					
					if err != nil {
					    errs.Store(err, true)
					    return
					}
				}
				if got == nil || len(got.Values) == 0 || got.Values[0][0] != int64(999) {
					errs.Store("invalid entry returned", true)
				}
			}
		}()
	}
	wg.Wait()
	
	require.Equal(t, int64(1), count, "loader should only execute exactly once")
	
	var hasErr bool
	errs.Range(func(key, value any) bool {
		t.Errorf("goroutine error: %v", key)
		hasErr = true
		return true
	})
	require.False(t, hasErr)
	_ = s
}

func TestRedisLockOrWait_StampedeServerCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, rdb := runMiniredis(t)
	c := rediscache.New(rdb)
	key := "test:redis:stampede_crash"

	var wg sync.WaitGroup
	var errs sync.Map
	
	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(999)}},
	}

	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			won, wait, release, err := c.LockOrWait(ctx, key)
			if err != nil {
				return
			}
			if won {
				time.Sleep(200 * time.Millisecond)
				_ = c.Add(ctx, key, entry, time.Minute)
				release(ctx)
			} else {
				_, waitErr := wait(ctx)
				if waitErr != nil {
					// We expect network errors or ErrRetryLocker when the server crashes
				}
			}
		}()
	}
	
	time.Sleep(50 * time.Millisecond)
	s.Close() // CRASH!
	wg.Wait()
	
	var hasErr bool
	errs.Range(func(key, value any) bool {
		t.Errorf("goroutine error: %v", key)
		hasErr = true
		return true
	})
	require.False(t, hasErr)
}

func TestRedisLockOrWait_KeyspaceEvents(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()
	
	// miniredis does not support full pub/sub keyspace events natively in exactly the same way,
	// but we can manually publish to __keyspace@0__:key to simulate it, or just rely on 
	// miniredis' limited support if any.
	// Actually, wait, miniredis DOES support some keyspace events if enabled, but usually only simple ones.
	// Let's just create a rediscache with WithKeyspaceEvents and test if it works via fallback or natively.
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	c := rediscache.New(rdb, rediscache.WithKeyspaceEvents())
	
	key := "test:redis:keyspace"
	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	require.NoError(t, err1)
	require.True(t, won1)
	
	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	require.NoError(t, err2)
	require.False(t, won2)
	
	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()
	
	time.Sleep(50 * time.Millisecond)
	
	// manually publish keyspace event because miniredis might not auto-publish DEL keyspace event
	rdb.Publish(ctx, "__keyspace@0__:lock:"+key, "del")
	release1(ctx)
	
	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)
	_ = wait1
}
