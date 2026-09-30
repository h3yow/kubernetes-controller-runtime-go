package client

import (
	"context"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

// Client is a version-aware facade over the Store used by reconcilers. All
// writes carry the leader's fencing token, so the store rejects any write from
// a deposed leader.
type Client struct {
	store store.Store
	fence store.Fence
}

// New builds a read-only Client. Use WithFence for write-enabled sessions.
func New(st store.Store) *Client { return &Client{store: st} }

// WithFence returns a client bound to the given leader fence.
func (c *Client) WithFence(f store.Fence) *Client {
	return &Client{store: c.store, fence: f}
}

// Fence returns the bound fencing token.
func (c *Client) Fence() store.Fence { return c.fence }

// Get fetches the live object (current resource version).
func (c *Client) Get(ctx context.Context, namespace, name string) (*store.Object, error) {
	return c.store.Get(ctx, namespace, name)
}

// List returns all live objects.
func (c *Client) List(ctx context.Context) ([]*store.Object, error) {
	return c.store.List(ctx)
}

// Create inserts a new object and assigns its first resource version.
func (c *Client) Create(ctx context.Context, obj *store.Object) (*store.Object, error) {
	return c.store.Create(ctx, c.fence, obj)
}

// Update writes obj iff its resource version still matches the live object.
// On ErrConflict the caller must re-Get and re-evaluate — never blindly retry
// the same body.
func (c *Client) Update(ctx context.Context, obj *store.Object) (*store.Object, error) {
	return c.store.Update(ctx, c.fence, obj)
}

// StatusUpdate writes only the status sub-object while preserving spec. It
// re-reads the live object first because status writes must not clobber spec
// changes made concurrently.
func (c *Client) StatusUpdate(ctx context.Context, namespace, name string, status map[string]any) (*store.Object, error) {
	live, err := c.store.Get(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	live.Status = status
	return c.store.Update(ctx, c.fence, live)
}

// Delete requests deletion. When the object has finalizers the store tombstones
// it (sets DeletionTimestamp) and returns store.ErrHasFinalizers; the object
// stays readable until finalizers complete.
func (c *Client) Delete(ctx context.Context, namespace, name string) error {
	return c.store.Delete(ctx, c.fence, namespace, name)
}

// AddFinalizer ensures f is present, based on the latest live state.
func (c *Client) AddFinalizer(ctx context.Context, obj *store.Object, f string) (*store.Object, error) {
	for _, x := range obj.Finalizers {
		if x == f {
			return obj, nil
		}
	}
	obj.Finalizers = append(obj.Finalizers, f)
	return c.store.Update(ctx, c.fence, obj)
}

// RemoveFinalizer strips f from the object so a tombstoned object can finally
// be purged.
func (c *Client) RemoveFinalizer(ctx context.Context, obj *store.Object, f string) (*store.Object, error) {
	out := make([]string, 0, len(obj.Finalizers))
	for _, x := range obj.Finalizers {
		if x != f {
			out = append(out, x)
		}
	}
	obj.Finalizers = out
	return c.store.Update(ctx, c.fence, obj)
}
