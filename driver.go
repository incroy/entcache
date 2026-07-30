package entcache

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	_ "unsafe"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/mitchellh/hashstructure/v2"
)

type (
	// Options wraps the basic configuration cache options.
	Options struct {
		// TTL defines the period of time that an Entry
		// is valid in the cache (used for hash-addressed queries).
		TTL time.Duration

		// KeyTTL defines the period of time that a key-addressed Entry
		// (e.g. Get-by-ID queries) is valid in the cache. Key-addressed
		// queries can have a longer TTL because they are precisely
		// invalidated via the ChangeSet. If zero, TTL is used.
		KeyTTL time.Duration

		// Cache defines the GetAddDeleter (cache implementation)
		// for holding the cache entries.
		Cache AddGetDeleter

		// Hash defines an optional Hash function for converting
		// a query and its arguments to a cache key. If no Hash
		// function was provided, the DefaultHash is used.
		Hash func(query string, args []any) (Key, error)

		// ChangeSet holds the mutation change tracker. When set,
		// the Driver checks whether cached entries have been
		// invalidated by mutations before returning them.
		ChangeSet *ChangeSet

		// Logf function. If provided, the Driver will call it with
		// errors that can not be handled.
		Log func(...any)
	}

	// Option allows configuring the cache
	// driver using functional options.
	Option func(*Options)

	// A Driver is an SQL cached client. Users should use the
	// constructor below for creating new driver.
	Driver struct {
		dialect.Driver
		*Options
		stats Stats
	}
)

// NewDriver returns a new Driver given an existing driver and optional
// configuration functions.
func NewDriver(drv dialect.Driver, opts ...Option) *Driver {
	options := &Options{Hash: DefaultHash}
	for _, opt := range opts {
		opt(options)
	}
	d := &Driver{
		Driver:  drv,
		Options: options,
	}
	if inv, ok := d.Cache.(Invalidator); ok {
		_ = inv.WatchInvalidations(context.Background(), func(k Key) {
			if d.ChangeSet != nil {
				d.ChangeSet.Mark(k)
			}
		})
	}
	return d
}

// TTL configures the period of time that an Entry is valid in the cache.
func TTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.TTL = ttl
	}
}

// WithKeyTTL configures a separate TTL for key-addressed queries (e.g. Get-by-ID).
func WithKeyTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.KeyTTL = ttl
	}
}

// Hash configures an optional Hash function for converting query + args to cache key.
func Hash(hash func(query string, args []any) (Key, error)) Option {
	return func(o *Options) {
		o.Hash = hash
	}
}

// Levels configures the Driver to work with the given cache levels.
func Levels(levels ...Cache) Option {
	return func(o *Options) {
		if len(levels) == 1 {
			o.Cache = levels[0]
		} else {
			o.Cache = &multiLevel{levels: levels}
		}
	}
}

type contextLevelCache struct{}

func (c *contextLevelCache) Get(ctx context.Context, k Key) (*Entry, error) {
	m, ok := FromContext(ctx)
	if !ok {
		return nil, ErrNotFound
	}
	return m.Get(ctx, k)
}

func (c *contextLevelCache) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	m, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	return m.Add(ctx, k, e, ttl)
}

func (c *contextLevelCache) Del(ctx context.Context, k Key) error {
	m, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	return m.Del(ctx, k)
}

// ContextLevel configures the driver to work with context/request level cache.
func ContextLevel() Option {
	return func(o *Options) {
		o.Cache = &contextLevelCache{}
	}
}

// WithChangeSet configures the Driver to use the given ChangeSet for
// mutation-aware cache invalidation.
func WithChangeSet(cs *ChangeSet) Option {
	return func(o *Options) {
		o.ChangeSet = cs
	}
}

// Query implements the Querier interface for the driver.
func (d *Driver) Query(ctx context.Context, query string, args, v any) error {
	if d.Cache == nil {
		return d.Driver.Query(ctx, query, args, v)
	}
	if !strings.HasPrefix(query, "SELECT") && !strings.HasPrefix(query, "select") {
		return d.Driver.Query(ctx, query, args, v)
	}
	vr, ok := v.(*sql.Rows)
	if !ok {
		return fmt.Errorf("entcache: invalid type %T. expect *sql.Rows", v)
	}
	argv, ok := args.([]any)
	if !ok {
		return fmt.Errorf("entcache: invalid type %T. expect []interface{} for args", args)
	}
	opts, err := d.optionsFromContext(ctx, query, argv)
	if err != nil {
		return d.Driver.Query(ctx, query, args, v)
	}
	atomic.AddUint64(&d.stats.Gets, 1)

	// Check cache, with ChangeSet-aware invalidation.
	e, cacheErr := d.Cache.Get(ctx, opts.key)
	if cacheErr == nil && d.ChangeSet != nil && opts.ref {
		if d.ChangeSet.Changed(opts.key, time.Now().Add(-opts.ttl)) {
			_ = d.Cache.Del(ctx, opts.key)
			d.ChangeSet.Clear(opts.key)
			cacheErr = ErrNotFound
		}
	}

	if cacheErr == nil {
		atomic.AddUint64(&d.stats.Hits, 1)
		vr.ColumnScanner = &repeater{columns: e.Columns, values: e.Values}
		return nil
	}

	if cacheErr != ErrNotFound {
		return d.Driver.Query(ctx, query, args, v)
	}

	// Cache Miss: execute query with built-in stampede protection.
	return d.queryWithStampedeLock(ctx, query, args, vr, opts)
}

func (d *Driver) queryWithStampedeLock(ctx context.Context, query string, args any, vr *sql.Rows, opts ctxOptions) error {
	var releaseFunc func(context.Context)
	// If backend implements StampedeLocker
	if locker, ok := d.Cache.(StampedeLocker); ok {
	retry_lock:
		won, wait, release, err := locker.LockOrWait(ctx, opts.key)
		if err == nil {
			if !won {
				// Another node/caller is loading — block on wait() until populated
				e, err := wait(ctx)
				if errors.Is(err, ErrRetryLocker) {
					goto retry_lock
				}
				if err == nil && e != nil {
					atomic.AddUint64(&d.stats.Hits, 1)
					vr.ColumnScanner = &repeater{columns: e.Columns, values: e.Values}
					return nil
				}
				// Fallback to querying DB if wait failed or key was cleared
			} else if release != nil {
				releaseFunc = release
			}
		}
	}

	if err := d.Driver.Query(ctx, query, args, vr); err != nil {
		if releaseFunc != nil {
			releaseFunc(context.Background())
		}
		return err
	}
	vr.ColumnScanner = &recorder{
		ColumnScanner: vr.ColumnScanner,
		skipNotFound:  opts.skipNotFound,
		onClose: func(columns []string, values [][]driver.Value) {
			if releaseFunc != nil {
				defer releaseFunc(context.Background())
			}
			err := d.Cache.Add(ctx, opts.key, &Entry{Columns: columns, Values: values}, opts.ttl)
			if err != nil && d.Log != nil {
				atomic.AddUint64(&d.stats.Errors, 1)
				d.Log(fmt.Sprintf("entcache: failed storing entry %v in cache: %v", opts.key, err))
			}
			if d.ChangeSet != nil && opts.ref {
				d.ChangeSet.Clear(opts.key)
			}
		},
	}
	return nil
}

// Stats returns a copy of the cache statistics.
func (d *Driver) Stats() Stats {
	return Stats{
		Gets:   atomic.LoadUint64(&d.stats.Gets),
		Hits:   atomic.LoadUint64(&d.stats.Hits),
		Errors: atomic.LoadUint64(&d.stats.Errors),
	}
}

// QueryContext calls QueryContext of underlying driver.
func (d *Driver) QueryContext(ctx context.Context, query string, args ...any) (*stdsql.Rows, error) {
	drv, ok := d.Driver.(interface {
		QueryContext(context.Context, string, ...any) (*stdsql.Rows, error)
	})
	if !ok {
		return nil, fmt.Errorf("Driver.QueryContext is not supported")
	}
	return drv.QueryContext(ctx, query, args...)
}

// ExecContext calls ExecContext of underlying driver.
func (d *Driver) ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error) {
	drv, ok := d.Driver.(interface {
		ExecContext(context.Context, string, ...any) (stdsql.Result, error)
	})
	if !ok {
		return nil, fmt.Errorf("Driver.ExecContext is not supported")
	}
	return drv.ExecContext(ctx, query, args...)
}

var errSkip = errors.New("entcache: skip cache")

func (d *Driver) optionsFromContext(ctx context.Context, query string, args []any) (ctxOptions, error) {
	var opts ctxOptions
	if c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions); ok {
		opts = *c
	}
	if opts.key == nil {
		key, err := d.Hash(query, args)
		if err != nil {
			return opts, errSkip
		}
		opts.key = key
	}
	if opts.ttl == 0 {
		if opts.ref && d.KeyTTL > 0 {
			opts.ttl = d.KeyTTL
		} else {
			opts.ttl = d.TTL
		}
	}
	if opts.evict {
		if err := d.Cache.Del(ctx, opts.key); err != nil {
			return opts, err
		}
	}
	if opts.skip {
		return opts, errSkip
	}
	return opts, nil
}

// DefaultHash provides the default implementation for converting a query + args to a cache key.
func DefaultHash(query string, args []any) (Key, error) {
	key, err := hashstructure.Hash(struct {
		Q string
		A []any
	}{
		Q: query,
		A: args,
	}, hashstructure.FormatV2, nil)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// Stats represents the cache statistics of the driver.
type Stats struct {
	Gets   uint64
	Hits   uint64
	Errors uint64
}

type rawCopy struct {
	values []driver.Value
}

func (c *rawCopy) Scan(src interface{}) error {
	if b, ok := src.([]byte); ok {
		b1 := make([]byte, len(b))
		copy(b1, b)
		src = b1
	}
	c.values[0] = src
	c.values = c.values[1:]
	return nil
}

type recorder struct {
	sql.ColumnScanner
	values       [][]driver.Value
	columns      []string
	done         bool
	skipNotFound bool
	onClose      func([]string, [][]driver.Value)
}

func (r *recorder) Next() bool {
	hasNext := r.ColumnScanner.Next()
	r.done = !hasNext
	return hasNext
}

func (r *recorder) Scan(dest ...any) error {
	values := make([]driver.Value, len(dest))
	args := make([]any, len(dest))
	c := &rawCopy{values: values}
	for i := range args {
		args[i] = c
	}
	if err := r.ColumnScanner.Scan(args...); err != nil {
		return err
	}
	for i := range values {
		if err := convertAssign(dest[i], values[i]); err != nil {
			return err
		}
	}
	r.values = append(r.values, values)
	return nil
}

func (r *recorder) Columns() ([]string, error) {
	columns, err := r.ColumnScanner.Columns()
	if err != nil {
		return nil, err
	}
	r.columns = columns
	return columns, nil
}

func (r *recorder) Close() error {
	if err := r.ColumnScanner.Close(); err != nil {
		return err
	}
	if err := r.ColumnScanner.Err(); err == nil || r.done {
		if r.skipNotFound && len(r.values) == 0 {
			return nil
		}
		r.onClose(r.columns, r.values)
	}
	return nil
}

type repeater struct {
	columns []string
	values  [][]driver.Value
}

func (*repeater) Close() error {
	return nil
}
func (*repeater) ColumnTypes() ([]*stdsql.ColumnType, error) {
	return nil, fmt.Errorf("entcache.ColumnTypes is not supported")
}
func (r *repeater) Columns() ([]string, error) {
	return r.columns, nil
}
func (*repeater) Err() error {
	return nil
}
func (r *repeater) Next() bool {
	return len(r.values) > 0
}
func (r *repeater) NextResultSet() bool {
	return len(r.values) > 0
}

func (r *repeater) Scan(dest ...any) error {
	if !r.Next() {
		return stdsql.ErrNoRows
	}
	for i, src := range r.values[0] {
		if err := convertAssign(dest[i], src); err != nil {
			return err
		}
	}
	r.values = r.values[1:]
	return nil
}

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error
