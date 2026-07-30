# entcache - Distributed Cache Driver for Ent ORM

[![Go Reference](https://pkg.go.dev/badge/github.com/incroy/entcache.svg)](https://pkg.go.dev/github.com/incroy/entcache)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**entcache** is a production-ready, modular **cache driver for ent**, the popular **Go ORM framework**. It drastically reduces database load by intercepting queries, caching results, and offering advanced distributed caching techniques like **Cache Stampede Protection (Dogpile Effect prevention)**, **Native RESP3 Client-Side Caching**, and **Mutation-Aware Cache Invalidation**.

Whether you're building a GraphQL server needing a **Context caching (GraphQL DataLoader pattern)** or a massive microservices architecture demanding **Multi-level caching** and **Distributed caching** with **Redis (go-redis)**, **Rueidis**, or **NATS JetStream KV**, `entcache` has you covered.

## Core Features

- **Distributed Cache Stampede Protection (Dogpile Effect prevention)**: When multiple concurrent requests experience a cache miss for the same key, exactly **one** request becomes the "winner" and fetches the data from the database. The remaining "waiters" block seamlessly.
- **Deadlock-Free Background Heartbeats**: The "winner" maintains a background heartbeat (`keepalive`) while the DB query executes. If the winner crashes, the lock natively expires, and a waiter automatically takes over.
- **Real-Time Cross-Node Cache Invalidation**: Waiters do not poll blindly. They utilize native Push/Watch capabilities (like NATS `Watch`) to receive an immediate event when the winner finishes, allowing them to resume instantaneously.
- **Native RESP3 Client-Side Caching**: `rueidiscache` leverages **Rueidis** RESP3 **Client-Side Caching** out of the box (in-memory client cache with server-driven invalidation) with no extra LRU required.
- **Accurate Payload TTLs**: Uses a strict 2-key architecture to isolate the stampede lock from the actual payload data, ensuring that caching entries retain exactly the user-defined TTL.
- **Multi-Level Caching**: Hierarchical cache structure (e.g. L1 LRU memory cache + L2 remote **Redis** or **NATS JetStream KV** store) for optimal latency and durability in **Go ORM caching**.
- **Mutation-Aware Cache Invalidation**: ent hooks automatically invalidate stale cache entries when entity mutations (create, update, delete) occur.
- **Zero Bloat Modular Cache Driver**: Clean modular sub-packages for `lrucache`, `natscache`, `rediscache`, and `rueidiscache`.

## Installation

Install core `entcache` along with your preferred cache backend sub-package:

```shell
# Core entcache package
go get github.com/incroy/entcache

# Optional modular backends
go get github.com/incroy/entcache/contextcache # Request-scoped in-memory cache
go get github.com/incroy/entcache/lrucache      # Hashicorp LRU v2
go get github.com/incroy/entcache/natscache     # NATS JetStream KV
go get github.com/incroy/entcache/rediscache    # go-redis/v9
go get github.com/incroy/entcache/rueidiscache  # Rueidis (Native RESP3 Client-Side Caching)
```

## High Level Design

On a high level, `entcache.Driver` decorates the `Query` method of the given driver, and for each call, generates a cache key (i.e. hash) from its arguments (statement and query parameters). After the query is executed, the driver records the raw values of the returned rows (`sql.Rows`) and stores them in the cache store with the generated cache key. Subsequent identical queries replay the recorded rows directly from cache without hitting the database, provided the entry has not expired or been evicted.

`entcache` provides three main caching architectures to optimize your application:

### 1. Context-Level Cache

Scoped to a single `context.Context` (e.g. `*http.Request`). The context carries an in-memory cache to eliminate duplicate database queries executed during the same request lifecycle.

![context-level-cache](https://github.com/ariga/entcache/raw/assets/internal/assets/ctxlevel.png)

```go
drv := entcache.NewDriver(sqlDrv, entcache.ContextLevel())
client := ent.NewClient(ent.Driver(drv))

// Wrap the request context
ctx = entcache.NewContext(ctx)
```

#### Solving the GraphQL N+1 Problem (DataLoader Pattern)

When building a GraphQL server, you often face the classic **N+1 Problem**. A naive resolver executes 1 query to fetch $N$ users, $N$ queries to fetch their todos, and another $N$ queries to fetch the owners of those todos.

```graphql
query($ids: [ID!]!) {
    nodes(ids: $ids) {
        ... on User {
            id
            name
            todos {
                id
                owner {
                    id
                    name
                }
            }
        }
    }
}
```

The Ent ORM optimizes this by batching execution into 3 queries. With `entcache`'s **Context caching (GraphQL DataLoader pattern)**, the number of queries is further reduced from 3 to **2**, because fetching users (Query 1) and fetching todo owners (Query 3) execute identical SQL statements. Query 3 is served directly from the context cache in-memory!

```go
srv.AroundResponses(func(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
    if op := graphql.GetOperationContext(ctx).Operation; op != nil && op.Operation == ast.Query {
        // Initialize the Context Cache for this specific GraphQL Request
        ctx = entcache.NewContext(ctx)
    }
    return next(ctx)
})
```

### 2. Driver-Level Cache

Embedded directly in `ent.Client` via `entcache.NewDriver`. The driver-level cache is process-scoped and shared across all goroutines in the application process.

![driver-level-cache](https://github.com/ariga/entcache/raw/assets/internal/assets/drvlevel.png)

```go
drv := entcache.NewDriver(
    sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(lrucache.MustNew(1000)),
)
client := ent.NewClient(ent.Driver(drv))
```

### 3. Multi-Level Cache

A cache hierarchy structures cache stores by access speed and capacity (e.g. L1 in-memory LRU + L2 remote NATS KV/Redis). Lookups cascade down the hierarchy: L1 → L2 → Database. This is ideal for distributed microservices.

![multi-level-cache](https://github.com/ariga/entcache/raw/assets/internal/assets/multilevel.png)

```go
drv := entcache.NewDriver(
    sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(
        lrucache.MustNew(256),  // Level 1: fast in-process memory
        natscache.New(kv),      // Level 2: durable NATS JetStream KV
    ),
)
client := ent.NewClient(ent.Driver(drv))
```

---

## Modular Cache Backends

### NATS JetStream KV (`natscache`)

Distributed cache backed by NATS JetStream KeyValue (`github.com/incroy/entcache/natscache`).

**Out-of-the-Box Features**:
- **Distributed Caching**: Stores cache data directly inside NATS JetStream KV, providing a durable, scalable, clustered backend.
- **Distributed Cache Stampede Protection**: Waiters block via native `Watch` events (`KeyValueDelete` / `KeyValuePurge`) allowing real-time cross-node invalidation and resumption.
- **Fault-Tolerant Heartbeats**: Background keepalives prevent deadlocks and ensure accurate TTLs using a 2-key architecture.

```go
import (
    "github.com/nats-io/nats.go/jetstream"
    "github.com/incroy/entcache/natscache"
)

nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)

// Important: LimitMarkerTTL MUST be enabled for the lock's per-key TTL to work.
kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
    Bucket:         "entcache",
    TTL:            10 * time.Minute,
    LimitMarkerTTL: time.Second, // Required for stampede protection heartbeats
})

drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(natscache.New(kv)),
)
```

### Rueidis (`rueidiscache`)

High-performance Redis cache powered by Rueidis (`github.com/incroy/entcache/rueidiscache`).

**Out-of-the-Box Features**:
- **Native RESP3 Client-Side Caching**: Natively activated out of the box! Rueidis caches keys in-memory on the client and receives server-driven invalidation tracking messages from Redis. No extra L1 LRU layer is required.
- **Stampede Protection**: Built-in channel wait locks prevent multiple concurrent database queries for the same key.

```go
import (
    "github.com/redis/rueidis"
    "github.com/incroy/entcache/rueidiscache"
)

c, err := rueidis.NewClient(rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
})
if err != nil {
    log.Fatal(err)
}

// Native Client-Side Caching is active out of the box!
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(rueidiscache.New(c)),
)
```

### Redis (`rediscache`)

Standard remote cache backed by `github.com/redis/go-redis/v9` (`github.com/incroy/entcache/rediscache`).

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/incroy/entcache/rediscache"
)

rdb := redis.NewClient(&redis.Options{Addr: ":6379"})
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(rediscache.New(rdb)),
)
```

### Hashicorp LRU (`lrucache`)

Thread-safe in-process LRU cache powered by `github.com/hashicorp/golang-lru/v2`. Fast, zero-network-overhead.

```go
import "github.com/incroy/entcache/lrucache"

drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(lrucache.MustNew(1000)),
)
```

---

## Mutation-Aware Invalidation

`entcache` supports automatic cache invalidation on entity mutations (create, update, delete). A `ChangeSet` records modified entity keys and evicts stale cache entries on subsequent queries.

```go
// Create a ChangeSet with GC interval.
cs := entcache.NewChangeSet(5 * time.Minute)
cs.Start()
defer cs.Stop()

// Create the cached driver with ChangeSet.
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.WithKeyTTL(time.Hour),      // longer TTL for Get-by-ID queries
    entcache.WithChangeSet(cs),
)

// Create the client and register the mutation hook.
client := ent.NewClient(ent.Driver(drv))
client.Use(entcache.DataChangeNotify(drv))

// Now, when a User is updated:
client.User.UpdateOneID(42).SetName("new-name").Save(ctx)

// The next Get for that user will bypass the cache and re-fetch from DB.
u, _ := client.User.Get(entcache.WithEntryKey(ctx, "User", 42), 42)
```

---

## Full Production Example (entpgx + NATS KV + Mutation Hook)

[entpgx](https://github.com/incroy/entpgx) provides a native `pgxpool.Pool`-based `dialect.Driver` that bypasses `database/sql` entirely. `entcache` wraps it seamlessly:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/incroy/entcache"
    "github.com/incroy/entcache/lrucache"
    "github.com/incroy/entcache/natscache"
    "github.com/incroy/entpgx"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"

    "myapp/ent"
)

func main() {
    ctx := context.Background()

    // 1. Database: native pgx pool via entpgx.
    pool, err := pgxpool.New(ctx, "postgres://localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }

    // 2. Remote Cache: NATS JetStream KV.
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        log.Fatal(err)
    }
    js, err := jetstream.New(nc)
    if err != nil {
        log.Fatal(err)
    }
    
    // Important: LimitMarkerTTL MUST be enabled for the lock's per-key TTL to work.
    kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
        Bucket:         "entcache",
        TTL:            10 * time.Minute,
        LimitMarkerTTL: time.Second, // Required for stampede protection heartbeats
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. ChangeSet for mutation-aware invalidation.
    cs := entcache.NewChangeSet(5 * time.Minute)
    cs.Start()
    defer cs.Stop()

    // 4. Wrap drivers: entpgx -> entcache -> ent.Client.
    pgxDrv := entpgx.NewDriver(pool)
    drv := entcache.NewDriver(pgxDrv,
        entcache.TTL(time.Minute),
        entcache.WithKeyTTL(30*time.Minute),
        entcache.WithChangeSet(cs),
        entcache.Levels(
            lrucache.MustNew(512),
            natscache.New(kv),
        ),
    )
    client := ent.NewClient(ent.Driver(drv))

    // 5. Register the mutation hook.
    client.Use(entcache.DataChangeNotify(drv))

    // 6. Run migrations (skip cache).
    if err := client.Schema.Create(entcache.Skip(ctx)); err != nil {
        log.Fatal(err)
    }

    // Queries are cached across L1 memory and L2 NATS KV.
    // Distributed stampede locking & real-time Watch invalidation work out of the box!
}
```

## API Reference

Full documentation is available at [pkg.go.dev/github.com/incroy/entcache](https://pkg.go.dev/github.com/incroy/entcache).

### Option Functions

| Function | Description |
|---|---|
| `TTL(d)` | Default cache TTL for hash-addressed queries |
| `WithKeyTTL(d)` | Separate TTL for key-addressed (Get-by-ID) queries |
| `Hash(fn)` | Custom hash function for cache key generation |
| `Levels(...)` | Configure one or more cache backends |
| `ContextLevel()` | Use context-scoped caching |
| `WithChangeSet(cs)` | Enable mutation-aware invalidation |

### Context Options (Per-Query Control)

| Function | Description |
|---|---|
| `Skip(ctx)` | Skip cache for a query |
| `Evict(ctx)` | Skip and invalidate cache entry for a query |
| `WithKey(ctx, key)` | Explicitly set cache key for a query |
| `WithTTL(ctx, ttl)` | Custom TTL for a query |
| `WithEntryKey(ctx, typ, id)` | Structured entity key (e.g. `"User:42"`) for Get-by-ID queries & ChangeSet invalidation |
| `SkipNotFound(ctx)` | Prevent caching when query result contains 0 rows |

### Cache Backends

| Sub-Package | Package Name | Constructor | Features |
|---|---|---|---|
| `github.com/incroy/entcache/contextcache` | `contextcache` | `contextcache.New()` | Per-request cache with channel stampede locking |
| `github.com/incroy/entcache/lrucache` | `lrucache` | `lrucache.New(size)` | Hashicorp LRU v2 in-process cache |
| `github.com/incroy/entcache/natscache` | `natscache` | `natscache.New(kv)` | NATS KV Create stampede lock & Watch invalidation |
| `github.com/incroy/entcache/rueidiscache` | `rueidiscache` | `rueidiscache.New(client)` | Native RESP3 Client-Side Caching & lock channels |
| `github.com/incroy/entcache/rediscache` | `rediscache` | `rediscache.New(client)` | standard `go-redis/v9` backend |
