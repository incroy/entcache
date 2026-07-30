package entcache

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/gob"
	"errors"
	"fmt"
	"time"
)

type (
	// Entry defines an entry to store in a cache.
	Entry struct {
		Columns []string
		Values  [][]driver.Value
	}

	// A Key defines a comparable Go value.
	// See http://golang.org/ref/spec#Comparison_operators
	Key any

	// AddGetDeleter defines the interface for getting,
	// adding and deleting entries from the cache.
	AddGetDeleter interface {
		Del(ctx context.Context, k Key) error
		Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error
		Get(ctx context.Context, k Key) (*Entry, error)
	}
)

func init() {
	// Register non builtin driver.Values.
	gob.Register(time.Time{})
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (e Entry) MarshalBinary() ([]byte, error) {
	entry := struct {
		C []string
		V [][]driver.Value
	}{
		C: e.Columns,
		V: e.Values,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (e *Entry) UnmarshalBinary(buf []byte) error {
	var entry struct {
		C []string
		V [][]driver.Value
	}
	if err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&entry); err != nil {
		return err
	}
	e.Values = entry.V
	e.Columns = entry.C
	return nil
}

// ErrNotFound is returned by Get when an Entry does not exist in the cache.
var ErrNotFound = errors.New("entcache: entry was not found")

// NewEntryKey constructs a structured cache key from an entity type name and ID.
// This produces keys like "User:42" that enable precise invalidation via ChangeSet.
func NewEntryKey(typ string, id any) Key {
	return fmt.Sprintf("%s:%v", typ, id)
}
