package reconcile

import (
	"context"
	"errors"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

// CleanupFunc performs the external resource teardown for a tombstoned
// object. It must be idempotent. Returning an error keeps the finalizer and
// schedules another attempt; nil removes the finalizer.
type CleanupFunc func(ctx context.Context, obj *store.Object) error

// FinalizerReconciler is a Reconciler implementing the standard Kubernetes
// finalizer state machine:
//
//	object missing          -> nothing to do
//	object alive            -> ensure finalizer present, then run Ensure
//	object tombstoned       -> run Cleanup; on success remove finalizer and
//	                           purge; on failure keep object and retry
//
// On ErrConflict it re-reads the latest state and retries a bounded number of
// times, so concurrent writers are resolved from the newest state rather than
// by blindly overwriting.
type FinalizerReconciler struct {
	Name    string
	Ensure  func(ctx context.Context, c *client.Client, obj *store.Object) (Result, error)
	Cleanup CleanupFunc
	// MaxConflictRetries bounds read-modify-write conflict retries.
	MaxConflictRetries int
	Logger             interfaceLogger
}

// interfaceLogger is a tiny logger surface so this package does not have to
// depend on slog directly here.
type interfaceLogger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

func (r *FinalizerReconciler) conflictRetries() int {
	if r.MaxConflictRetries <= 0 {
		return 5
	}
	return r.MaxConflictRetries
}

// Reconcile implements Reconciler.
func (r *FinalizerReconciler) Reconcile(ctx context.Context, c *client.Client, namespace, name string) (Result, error) {
	for attempt := 0; ; attempt++ {
		obj, err := c.Get(ctx, namespace, name)
		if errors.Is(err, store.ErrNotFound) {
			return Result{}, nil
		}
		if err != nil {
			return Result{}, err
		}

		var res Result
		if obj.BeingDeleted() {
			res, err = r.handleDelete(ctx, c, obj)
		} else {
			res, err = r.handleAlive(ctx, c, obj)
		}

		if errors.Is(err, store.ErrConflict) && attempt < r.conflictRetries() {
			// Another writer changed the object between our Get and write.
			// Re-evaluate from the newest state instead of overwriting.
			if r.Logger != nil {
				r.Logger.Warn("conflict, re-reading latest state",
					"key", obj.Key(), "rv", obj.ResourceVersion, "attempt", attempt+1)
			}
			continue
		}
		return res, err
	}
}

func (r *FinalizerReconciler) handleAlive(ctx context.Context, c *client.Client, obj *store.Object) (Result, error) {
	if !hasFinalizer(obj.Finalizers, r.Name) {
		updated, err := c.AddFinalizer(ctx, obj, r.Name)
		if err != nil {
			return Result{}, err
		}
		obj = updated
		if r.Logger != nil {
			r.Logger.Info("finalizer added", "key", obj.Key(), "rv", obj.ResourceVersion)
		}
	}
	if r.Ensure == nil {
		return Result{}, nil
	}
	return r.Ensure(ctx, c, obj)
}

func (r *FinalizerReconciler) handleDelete(ctx context.Context, c *client.Client, obj *store.Object) (Result, error) {
	if !hasFinalizer(obj.Finalizers, r.Name) {
		return Result{}, nil
	}
	if r.Cleanup == nil {
		// Nothing external to do: drop finalizer and purge.
		return Result{}, r.purge(ctx, c, obj)
	}
	if err := r.Cleanup(ctx, obj); err != nil {
		// Keep the object and finalizer; controller backoff-retries. Never
		// stuck (bounded backoff), never wrongly deleted.
		if r.Logger != nil {
			r.Logger.Warn("cleanup failed, keeping object for retry",
				"key", obj.Key(), "err", err)
		}
		return Result{}, err
	}
	if r.Logger != nil {
		r.Logger.Info("cleanup completed, removing finalizer", "key", obj.Key())
	}
	return Result{}, r.purge(ctx, c, obj)
}

func (r *FinalizerReconciler) purge(ctx context.Context, c *client.Client, obj *store.Object) error {
	// Re-read: deletion tombstoning (or a concurrent writer) may have advanced
	// the resource version beyond the object this reconcile started with.
	latest, err := c.Get(ctx, obj.Namespace, obj.Name)
	if err != nil {
		return err
	}
	if _, err := c.RemoveFinalizer(ctx, latest, r.Name); err != nil {
		return err
	}
	if err := c.Delete(ctx, latest.Namespace, latest.Name); err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

func hasFinalizer(fs []string, name string) bool {
	for _, f := range fs {
		if f == name {
			return true
		}
	}
	return false
}
