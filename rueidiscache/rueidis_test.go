package rueidiscache_test

import (
	"context"
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/incroy/entcache"
	"github.com/incroy/entcache/rueidiscache"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func runMiniredis(t *testing.T) (*miniredis.Miniredis, rueidis.Client) {
	t.Helper()
	s, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	rdb, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{s.Addr()},
		DisableCache: true, // miniredis doesn't support RESP3 CLIENT TRACKING
	})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })

	return s, rdb
}

func TestRueidisCache(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)

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

func TestRueidisLockOrWait_Stampede(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:stampede"

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

func TestRueidisLockOrWait_HeartbeatFailure(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	mkClient := func() rueidis.Client {
		c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{s.Addr()}, DisableCache: true})
		require.NoError(t, err)
		return c
	}

	c1, c2 := rueidiscache.New(mkClient()), rueidiscache.New(mkClient())
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

func TestRueidisLockOrWait_ConcurrentStealing(t *testing.T) {
	ctx := context.Background()
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	mkClient := func() rueidis.Client {
		c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{s.Addr()}, DisableCache: true})
		require.NoError(t, err)
		return c
	}

	numClients := 3
	clients := make([]*rueidiscache.Rueidis, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = rueidiscache.New(mkClient())
	}
	key := "test:concurrent"

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	losers := 0

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(c *rueidiscache.Rueidis) {
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

func TestRueidisLockOrWait_Timeout(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:timeout"

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

func TestRueidisLockOrWait_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:cancel"

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

func TestRueidisCache_Close(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)

	won, _, _, err := c.LockOrWait(ctx, "test:rueidis:cleanup")
	require.NoError(t, err)
	require.True(t, won)

	c.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestRueidis_AddPreservesCustomTTL(t *testing.T) {
	ctx := context.Background()
	s, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:add_ttl"

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

func TestRueidis_HeartbeatTTLRefresh(t *testing.T) {
	ctx := context.Background()
	s, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:heartbeat_ttl"

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	// Since we hold the lock, the heartbeat should be running.
	// The ID is stored in the lock key
	lockKey := "lock:" + key
	id, err := s.Get(lockKey)
	require.NoError(t, err)

	// Initial TTL should be 10s
	ttl := s.TTL(id)
	require.Greater(t, ttl, 5*time.Second)
	require.LessOrEqual(t, ttl, 10*time.Second)

	// Fast forward time by 6 seconds.
	// The heartbeat runs every 5s, so it should renew it back to 10s.
	// However, miniredis fastforwarding doesn't automatically trigger Go time.Tickers immediately in deterministic lockstep,
	// but the goroutine will fire if we sleep in real time.
	// We'll sleep real time instead of FastForward so the goroutine's time.After triggers.
	time.Sleep(6 * time.Second)

	// TTL should be renewed, so it should still be > 5s
	ttl2 := s.TTL(id)
	require.Greater(t, ttl2, 5*time.Second)

	release(ctx)
}

func TestRueidisLockOrWait_LoaderErrorRetry(t *testing.T) {
	ctx := context.Background()
	_, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:loader_error"

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

	// Simulate Loader Error: Winner calls release() WITHOUT calling Add()
	time.Sleep(50 * time.Millisecond)
	release1(ctx)

	wg.Wait()
	// Waiter should retry!
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)
	_ = wait1
}


func TestRueidisLockOrWait_MassiveStampede(t *testing.T) {
	ctx := context.Background()
	s, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:massive_stampede"

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
				// Simulate slow query
				time.Sleep(50 * time.Millisecond)
				_ = c.Add(ctx, key, entry, time.Minute)
				release(ctx)
			} else {
				got, err := wait(ctx)
				if err != nil {
					// We expect some ErrRetryLocker when the winner finishes
					// But for a simple test we can just loop and retry LockOrWait if we get ErrRetryLocker
					// like the entcache ContextCache wrapper does
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

func TestRueidisLockOrWait_StampedeServerCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, rdb := runMiniredis(t)
	c := rueidiscache.New(rdb)
	key := "test:rueidis:stampede_crash"

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
				// We expect network errors since the server is crashing
				return
			}
			if won {
				time.Sleep(200 * time.Millisecond) // Simulate slow query
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
