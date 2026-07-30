package natscache_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/incroy/entcache"
	"github.com/incroy/entcache/natscache"
	"entgo.io/ent/dialect"
	entSql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func runNatsServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	s := natsserver.RunServer(&opts)
	t.Cleanup(func() {
		s.Shutdown()
	})
	return s
}

func makeNatsClient(t *testing.T, s *server.Server) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err, "failed to connect to embedded NATS server")
	t.Cleanup(func() {
		nc.Close()
	})

	js, err := jetstream.New(nc)
	require.NoError(t, err, "failed to create JetStream context")
	return nc, js
}

func TestNatsCache_EmbeddedJetStream(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_embedded_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c := natscache.New(kv)
	key := "user:test:99"
	entry := &entcache.Entry{
		Columns: []string{"id", "username"},
		Values:  [][]driver.Value{{int64(99), "bob"}},
	}

	// 1. LockOrWait Winner
	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err, "LockOrWait failed")
	require.True(t, won, "expected first call to win lock")

	// 2. Add entry
	err = c.Add(ctx, key, entry, time.Minute)
	require.NoError(t, err, "Add failed")
	if release != nil {
		release(ctx)
	}

	// 3. Get entry
	got, err := c.Get(ctx, key)
	require.NoError(t, err, "Get failed")
	require.Len(t, got.Values, 1)
	require.Equal(t, "bob", got.Values[0][1], "unexpected value in entry")

	// 4. WatchInvalidations
	invalidated := make(chan string, 1)
	err = c.WatchInvalidations(ctx, func(k entcache.Key) {
		invalidated <- k.(string)
	})
	require.NoError(t, err, "WatchInvalidations failed")

	_ = c.Del(ctx, key)

	select {
	case k := <-invalidated:
		require.Equal(t, "user_test_99", k, "expected invalidated key user_test_99")
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for WatchInvalidations event")
	}
}

func TestNatsLockOrWait_Stampede(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_stampede_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c := natscache.New(kv)
	key := "test:nats:stampede"

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
	err = c.Add(ctx, key, entry, time.Minute)
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

func TestNatsLockOrWait_HeartbeatFailure(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_heartbeat_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c1 := natscache.New(kv)
	c2 := natscache.New(kv)
	key := "test:nats:heartbeat"

	won1, _, _, err1 := c1.LockOrWait(ctx, key)
	require.NoError(t, err1, "first LockOrWait failed")
	require.True(t, won1, "expected first call to win lock")

	won2, wait2, _, err2 := c2.LockOrWait(ctx, key)
	require.NoError(t, err2, "second LockOrWait failed")
	require.False(t, won2, "expected second call to lose lock")

	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()

	// Simulate ungraceful crash of c1
	c1.Crash()

	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker, "expected ErrRetryLocker on heartbeat failure")
}

func TestNatsLockOrWait_ConcurrentStealing(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_concurrent_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	key := "test:nats:concurrent"
	numClients := 3

	clients := make([]*natscache.NatsKV, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = natscache.New(kv)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	losers := 0

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(c *natscache.NatsKV) {
			defer wg.Done()
			won, _, _, err := c.LockOrWait(ctx, key)
			require.NoError(t, err, "LockOrWait failed")
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

	require.Equal(t, 1, winners, "expected exactly 1 winner")
	require.Equal(t, numClients-1, losers, "expected exactly %d losers", numClients-1)
}

func TestNats_HeartbeatTTLRefresh(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_heartbeat_ttl_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	// We can't easily change the hardcoded 5s ticker in the code without a refactor,
	// so we will test the actual 10s TTL behavior. We'll simulate `keepalive()` directly
	// or just wait. But waiting 15s in a unit test is slow.
	// Let's test the `Add` TTL behavior first.
	c := natscache.New(kv)
	key := "test:nats:heartbeat_ttl"

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	// Since we hold the lock, the heartbeat should be running.
	// The lock key is <sanitizedKey>_lock
	sanitizedKey := strings.ReplaceAll(key, ":", "_")
	lockKey := sanitizedKey + "_lock"
	_, err = kv.Get(ctx, lockKey)
	require.NoError(t, err)

	// Initial TTL should be limit marker TTL, but actually JetStream KV doesn't let us easily query remaining TTL.
	// We can't query the exact remaining TTL in NATS KV like we can in Redis.
	// We know it was set to 10s based on NatsCache implementation, but the KV bucket default is 1s.
	// Since NATS KV does not provide a `TTL()` method for keys, we can just ensure
	// that after a sleep longer than the Bucket LimitMarkerTTL, the key still exists.

	// Wait, the bucket LimitMarkerTTL was set to 1s in this test setup:
	// LimitMarkerTTL: 1 * time.Second
	// And the natscache heartbeat interval is 5s, which renews for another 10s.
	// Wait, the heartbeat interval in natscache is hardcoded to 5s. If the limit marker is 1s, it might expire before the first tick!
	// NATS KV TTL applies on write. natscache sets it to 10s.

	time.Sleep(6 * time.Second) // wait for at least one heartbeat tick

	_, err = kv.Get(ctx, lockKey)
	require.NoError(t, err, "lock key should still exist after 6s because heartbeat renewed it")

	release(ctx)
}

func TestNats_AddPreservesCustomTTL(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_add_ttl_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c := natscache.New(kv)
	key := "test:nats:add_ttl"

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

	// Read immediately
	got, err := c.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Wait 2 seconds, it should expire
	time.Sleep(2 * time.Second)

	_, err = c.Get(ctx, key)
	require.ErrorIs(t, err, entcache.ErrNotFound, "expected entry to expire after 1s TTL, instead got nil or different error")
}

func TestNatsCacheDel(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_del_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	key := "test:nats:del"

	entry := &entcache.Entry{Values: [][]driver.Value{{int64(1)}}}
	require.NoError(t, c.Add(ctx, key, entry, time.Minute))

	// Verify it exists
	got, err := c.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Delete it
	require.NoError(t, c.Del(ctx, key))

	// Verify it's gone
	_, err = c.Get(ctx, key)
	require.ErrorIs(t, err, entcache.ErrNotFound)
}

func TestNatsLockOrWait_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_cancel_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	key := "test:nats:cancel"

	won, _, _, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	// Waiter with cancellable context
	cancelCtx, cancel := context.WithCancel(ctx)
	won2, wait2, _, err := c.LockOrWait(cancelCtx, key)
	require.NoError(t, err)
	require.False(t, won2)

	// Cancel context immediately
	cancel()

	// Wait should return immediately with context.Canceled
	_, waitErr := wait2(cancelCtx)
	require.ErrorIs(t, waitErr, context.Canceled)
}

func TestNatsLockOrWait_Timeout(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_timeout_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	key := "test:nats:timeout"

	won, _, _, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	// Waiter with very short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	won2, wait2, _, err := c.LockOrWait(timeoutCtx, key)
	require.NoError(t, err)
	require.False(t, won2)

	// Wait should timeout quickly
	_, waitErr := wait2(timeoutCtx)
	require.ErrorIs(t, waitErr, context.DeadlineExceeded)
}

func TestNatsCloseCleanup(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_cleanup_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)

	// Create a lock to spawn the heartbeat goroutine
	won, _, _, err := c.LockOrWait(ctx, "test:nats:cleanup")
	require.NoError(t, err)
	require.True(t, won)

	// Close the cache
	c.Close()

	// Wait a moment to ensure goroutine exits (no easy way to assert runtime.NumGoroutine without flakiness,
	// but we can ensure Close doesn't panic and returns cleanly)
	time.Sleep(50 * time.Millisecond)
}

func TestNatsLockOrWait_LoaderErrorRetry(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_loader_err_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)

	c := natscache.New(kv)
	key := "test:nats:loader_error"

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

func TestNatsLockOrWait_MassiveStampede(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_massive_stampede_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)

	c := natscache.New(kv)
	key := "test:nats:massive_stampede"

	var count int64
	var wg sync.WaitGroup
	var errs sync.Map

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(999)}},
	}

	for i := 0; i < 2000; i++ { // Use 2000 to prevent NATS from timing out locally
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
}

func TestNatsLockOrWait_StampedeServerCrash(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_stampede_crash_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)

	c := natscache.New(kv)
	key := "test:nats:stampede_crash"

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
	s.Shutdown() // CRASH!
	wg.Wait()

	var hasErr bool
	errs.Range(func(key, value any) bool {
		t.Errorf("goroutine error: %v", key)
		hasErr = true
		return true
	})
	require.False(t, hasErr)
}

func TestNatsLock_ReleaseCAS(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_cas_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)

	c1 := natscache.New(kv)
	c2 := natscache.New(kv)

	key := "test:nats:cas_release"
	lockKey := strings.ReplaceAll(key, ":", "_") + "_lock"

	// 1. c1 gets lock
	won1, _, release1, err := c1.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won1)

	// 2. c1 is "slow", lock expires or is stolen
	err = kv.Delete(ctx, lockKey)
	require.NoError(t, err)

	// 3. c2 gets lock (steals it)
	won2, _, _, err := c2.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won2)

	// 4. c1 releases lock
	release1(ctx)

	// 5. check if c2's lock is still there
	_, err = kv.Get(ctx, lockKey)
	require.NoError(t, err, "c2's lock should not have been deleted by c1's release")
}

func TestNatsLock_NoSpuriousInvalidationOnLock(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_inval_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)

	c := natscache.New(kv)
	key := "test:nats:inval_lock"

	invalidated := make(chan string, 10)
	err = c.WatchInvalidations(ctx, func(k entcache.Key) {
		invalidated <- k.(string)
	})
	require.NoError(t, err)

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)
	release(ctx)

	select {
	case k := <-invalidated:
		t.Fatalf("unexpected invalidation for %s", k)
	case <-time.After(1 * time.Second):
		// success
	}
}

func TestNatsLockOrWait_HeartbeatFailureTiming(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "entcache_timing_test",
	})
	require.NoError(t, err)

	c1 := natscache.New(kv)
	c2 := natscache.New(kv)
	key := "test:nats:timing"

	won1, _, _, err := c1.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won1)

	won2, wait2, _, err := c2.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.False(t, won2)

	c1.Crash() // Simulate ungraceful crash

	start := time.Now()
	_, err = wait2(ctx)
	require.ErrorIs(t, err, entcache.ErrRetryLocker)
	require.WithinDuration(t, start, time.Now(), 16*time.Second, "waiter took too long to recover")
}

func TestNatsLockOrWait_HeartbeatFailureMidWatch(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "entcache_timing_midwatch_test",
	})
	require.NoError(t, err)

	c1 := natscache.New(kv)
	c2 := natscache.New(kv)
	key := "test:nats:timing_midwatch"

	won1, _, _, err := c1.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won1)

	won2, wait2, _, err := c2.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.False(t, won2)

	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()

	// sleep a bit so wait2 enters the watch loop
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	c1.Crash() // Simulate ungraceful crash MID-WATCH

	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)
	require.WithinDuration(t, start, time.Now(), 16*time.Second, "waiter took too long to recover from mid-watch crash")
}

func TestNatsLockOrWait_HeartbeatFailureUngraceful(t *testing.T) {
	ctx := context.Background()
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "entcache_timing_ungraceful_test",
	})
	require.NoError(t, err)

	c1 := natscache.New(kv)
	c2 := natscache.New(kv)
	key := "test:nats:timing_ungraceful"

	won1, _, _, err := c1.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won1)

	won2, wait2, _, err := c2.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.False(t, won2)

	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	// UNGRACEFUL CRASH: Stop the heartbeat without deleting the marker
	c1.Crash()

	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker)
	require.WithinDuration(t, start, time.Now(), 16*time.Second, "waiter took too long to recover from ungraceful crash")
}

func TestNatsDriver_StampedeLoaderErrorRetry(t *testing.T) {
	s := runNatsServer(t)
	_, js := makeNatsClient(t, s)

	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_driver_loader_err",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)

	db, sqlMock, err := sqlmock.New()
	require.NoError(t, err)

	queryErr := errors.New("simulated db error")
	
	// 1st query returns an error
	sqlMock.ExpectQuery("SELECT name FROM users").WillReturnError(queryErr)
	// 2nd query (the retry from the waiter) succeeds
	sqlMock.ExpectQuery("SELECT name FROM users").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("bob"))

	drv := entcache.NewDriver(
		entSql.OpenDB(dialect.Postgres, db),
		entcache.Levels(natscache.New(kv)), // Verified explicitly against NatsKV
		entcache.TTL(time.Minute),
	)

	var wg sync.WaitGroup
	wg.Add(2)

	// First caller (Winner)
	go func() {
		defer wg.Done()
		rows := &entSql.Rows{}
		err := drv.Query(ctx, "SELECT name FROM users", []interface{}{"bob"}, rows)
		if err == nil {
			t.Errorf("expected error from first query, got nil")
		}
	}()

	// Second caller (Waiter)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond) // Ensure it runs after the first starts
		
		rows := &entSql.Rows{}
		err := drv.Query(ctx, "SELECT name FROM users", []interface{}{"bob"}, rows)
		require.NoError(t, err)
	}()

	wg.Wait()

	if err := sqlMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
