// Package rueidiscache provides a Redis cache backend for entcache using Rueidis.
//
// It implements both the entcache.Cache and entcache.StampedeLocker interfaces,
// providing distributed caching capabilities with robust Cache Stampede Protection
// and high-performance RESP3 Client-Side Caching.
//
// When multiple concurrent requests attempt to fetch the same missing key, exactly
// one request acquires a lock and queries the database. The other requests wait
// seamlessly using client-side caching until the data is populated, preventing
// database overload.
//
// Invalidations for entcache multi-level caching are distributed through the
// "entcache:invalidations" Pub/Sub channel.
package rueidiscache
