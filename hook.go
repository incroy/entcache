package entcache

import (
	"context"
	"fmt"

	"entgo.io/ent"
)

// DataChangeNotify returns an ent Hook that marks changed entity keys in
// the Driver's ChangeSet whenever a mutation (create, update, delete) is
// committed. This enables automatic cache invalidation for key-addressed
// queries.
//
// Usage:
//
//	drv := entcache.NewDriver(sqlDrv, entcache.WithChangeSet(cs))
//	client := ent.NewClient(ent.Driver(drv))
//	client.Use(entcache.DataChangeNotify(drv))
func DataChangeNotify(drv *Driver) ent.Hook {
	return func(next ent.Mutator) ent.Mutator {
		return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
			v, err := next.Mutate(ctx, m)
			if err != nil {
				return v, err
			}
			cs := drv.ChangeSet
			if cs == nil {
				return v, nil
			}
			typ := m.Type()
			// Collect affected IDs from the mutation.
			var keys []Key
			if ids, idErr := extractIDs(ctx, m); idErr == nil {
				for _, id := range ids {
					keys = append(keys, NewEntryKey(typ, id))
				}
			}
			if len(keys) > 0 {
				cs.Mark(keys...)
			}
			return v, nil
		})
	}
}

// ider is an interface for mutations that expose their affected IDs.
type ider interface {
	IDs(ctx context.Context) ([]int, error)
}

// extractIDs attempts to get affected entity IDs from a mutation.
// It checks for the IDs method (batch mutations) and falls back to
// the ID method (single mutations).
func extractIDs(ctx context.Context, m ent.Mutation) ([]any, error) {
	// Try IDs() first (available on UpdateOne/DeleteOne after execution).
	if mi, ok := m.(ider); ok {
		ids, err := mi.IDs(ctx)
		if err == nil {
			result := make([]any, len(ids))
			for i, id := range ids {
				result[i] = id
			}
			return result, nil
		}
	}
	// Fall back: try to get a single ID via the OldField approach.
	// In ent, the mutation may carry the ID for *One operations.
	if f, exists := m.Field("id"); exists {
		return []any{fmt.Sprint(f)}, nil
	}
	return nil, fmt.Errorf("entcache: unable to extract IDs from mutation %T", m)
}
