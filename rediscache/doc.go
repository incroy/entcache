// Package rediscache provides a Redis cache backend for entcache using go-redis.
//
// It implements both the entcache.Cache and entcache.StampedeLocker interfaces,
// providing distributed caching capabilities with robust Cache Stampede Protection.
//
// When multiple concurrent requests attempt to fetch the same missing key, exactly
// one request acquires a lock and queries the database. The other requests wait
// seamlessly until the data is populated, preventing database overload.
//
// By default, StampedeLocker waiters poll for the lock state. If your Redis server
// is configured with "notify-keyspace-events KEA", you can optimize waiter latency 
// and network load by passing the WithKeyspaceEvents() option when initializing the cache.
//
// Invalidations for entcache multi-level caching are distributed through the
// "entcache:invalidations" Pub/Sub channel.
package rediscache
