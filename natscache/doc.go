// Package natscache provides a NATS JetStream KeyValue cache backend for entcache.
//
// It implements both the entcache.Cache and entcache.StampedeLocker interfaces,
// providing distributed caching capabilities with robust Cache Stampede Protection.
//
// When multiple concurrent requests attempt to fetch the same missing key, exactly
// one request acquires a lock and queries the database. The other requests wait
// seamlessly using NATS KV Watch capabilities until the data is populated, preventing
// database overload.
//
// Requirements:
// NATS JetStream KV buckets must be created with LimitMarkerTTL enabled for the
// internal stampede locks to automatically expire in case of client failure.
package natscache
