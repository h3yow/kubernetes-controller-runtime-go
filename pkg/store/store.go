package store

import "context"

// Store is the shared local cluster-state store. All implementations must
// serialize concurrent callers and reject writes whose fencing token does not
// match the currently held lease (fencing against stale leaders).
type Store interface {
	// Get returns the live object. Deleted objects return ErrNotFound.
	Get(ctx context.Context, namespace, name string) (*Object, error)
	// List returns all live objects.
	List(ctx context.Context) ([]*Object, error)
	// Create inserts a new object.
	Create(ctx context.Context, f Fence, obj *Object) (*Object, error)
	// Update replaces an object iff obj.ResourceVersion matches the live version.
	Update(ctx context.Context, f Fence, obj *Object) (*Object, error)
	// Delete removes an object. Objects with finalizers are tombstoned
	// (DeletionTimestamp set) and ErrHasFinalizers is returned.
	Delete(ctx context.Context, f Fence, namespace, name string) error
	// ForceDelete is used by tests/reset only.
	ForceDelete(ctx context.Context, namespace, name string) error

	// AcquireLease tries to acquire or renew.
	AcquireLease(ctx context.Context, holder string, ttlMillis int64) (*LeaseState, error)
	// RenewLease renews only if holder+epoch still own it.
	RenewLease(ctx context.Context, holder string, epoch int64, ttlMillis int64) (*LeaseState, error)
	// ReleaseLease gives up leadership voluntarily.
	ReleaseLease(ctx context.Context, holder string, epoch int64) error
	// ReadLease returns the current lease or nil if none / expired.
	ReadLease(ctx context.Context) (*LeaseState, error)
}
