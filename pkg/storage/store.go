// Package storage is the local cluster state: an in-memory object store with
// Kubernetes-style resource versions, optimistic concurrency, finalizers and
// watch events. Every runtime instance in one process shares the same Store.
package storage

import (
	"context"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
)

// Store is the abstraction over local cluster state.
type Store interface {
	// Create inserts a new object. The object's resourceVersion is assigned by
	// the store and the assigned copy is returned.
	Create(ctx context.Context, obj api.Object) (api.Object, error)
	// Update replaces an object iff obj's resourceVersion matches the stored
	// one. On success the new resourceVersion is assigned on the returned copy.
	Update(ctx context.Context, obj api.Object) (api.Object, error)
	// Delete marks the object as deleting (sets deletionTimestamp) when it has
	// finalizers; when no finalizers remain the object is physically removed.
	// Deleting an already-deleting object is a no-op success.
	Delete(ctx context.Context, kind, name string) error
	// Get returns the current object (a deep copy) or ErrNotFound.
	Get(ctx context.Context, kind, name string) (api.Object, error)
	// List returns deep copies of all objects of the given kind (empty kind
	// means every kind).
	List(ctx context.Context, kind string) ([]api.Object, error)
	// Subscribe returns the current snapshot plus a channel of watch events
	// with resource versions strictly newer than the snapshot. Closing ctx
	// unsubscribes the watcher.
	Subscribe(ctx context.Context, kind string) ([]api.Object, <-chan api.Event, error)
	// CurrentResourceVersion returns the store's global resource version.
	CurrentResourceVersion() api.ResourceVersion
}

// NewMemoryStore builds an empty in-memory store.
func NewMemoryStore() Store {
	return &memoryStore{}
}
