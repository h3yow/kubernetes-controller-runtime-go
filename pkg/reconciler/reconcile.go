package reconciler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
)

// FailureReason strings are the machine-distinguishable outcomes asserted in
// tests and printed in logs.
const (
	ReasonObjectGone        = "ObjectNotFound"
	ReasonCleanupInProgress = "ExternalCleanupRetrying"
	ReasonOCCConflict       = "OptimisticConflictRelist"
	ReasonEnsureFailed      = "EnsureExternalFailed"
)

// WidgetReconciler drives each Widget toward: external resource present +
// finalizer attached while alive; external resource deleted + finalizer
// removed (thus object gone) on deletion.
type WidgetReconciler struct {
	store storage.Store
	ext   *ExternalSystem
	log   *slog.Logger
}

// NewWidgetReconciler constructs the reconciler.
func NewWidgetReconciler(store storage.Store, ext *ExternalSystem, log *slog.Logger) *WidgetReconciler {
	if log == nil {
		log = slog.Default()
	}
	return &WidgetReconciler{store: store, ext: ext, log: log}
}

// Reconcile implements reconciler.Reconciler.
func (r *WidgetReconciler) Reconcile(ctx context.Context, key api.Key) (api.Result, error) {
	if key.Kind != KindWidget {
		return api.Result{}, nil
	}

	obj, err := r.store.Get(ctx, key.Kind, key.Name)
	if errors.Is(err, storage.ErrNotFound) {
		// Deleted physically (or never existed): nothing more to do. The
		// caller should Forget the key so no new reconcile is produced.
		r.log.Info("reconcile object gone", "key", key.String(), "reason", ReasonObjectGone)
		return api.Result{FailureReason: ReasonObjectGone}, nil
	}
	if err != nil {
		return api.Result{}, err
	}
	w := obj.(*Widget)

	if w.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, w)
	}
	return r.reconcileNormal(ctx, w)
}

// reconcileNormal ensures the external resource exists and the finalizer is
// attached. Every write starts from the object just read; a conflict means the
// world changed, so we surface it (no overwrite) and let the watch re-list.
func (r *WidgetReconciler) reconcileNormal(ctx context.Context, w *Widget) (api.Result, error) {
	key := api.Key{Kind: KindWidget, Name: w.Name}

	if err := r.ext.Ensure(w.Name); err != nil {
		r.log.Error("ensure external resource failed",
			"key", key.String(), "rv", w.ResourceVersion, "reason", ReasonEnsureFailed, "err", err)
		return api.Result{FailureReason: ReasonEnsureFailed}, err
	}

	if !hasFinalizer(w.Finalizers, FinalizerWidget) {
		updated := w.DeepCopy().(*Widget)
		updated.Finalizers = append(append([]string(nil), w.Finalizers...), FinalizerWidget)
		updated.Status.Phase = "Active"
		saved, err := r.store.Update(ctx, updated)
		if storage.IsConflict(err) {
			// Latest state wins: do not overwrite; the watch event for the
			// winning version re-enqueues the key for a fresh decision.
			r.log.Warn("finalizer add conflict, defer to latest watch",
				"key", key.String(), "basedOnRV", w.ResourceVersion, "reason", ReasonOCCConflict, "err", err)
			return api.Result{FailureReason: ReasonOCCConflict}, nil
		}
		if err != nil {
			return api.Result{}, err
		}
		r.log.Info("finalizer attached", "key", key.String(), "newRV", saved.GetMeta().ResourceVersion)
	} else if w.Status.Phase != "Active" {
		updated := w.DeepCopy().(*Widget)
		updated.Status.Phase = "Active"
		if _, err := r.store.Update(ctx, updated); storage.IsConflict(err) {
			r.log.Warn("status update conflict, defer to watch",
				"key", key.String(), "reason", ReasonOCCConflict, "err", err)
			return api.Result{FailureReason: ReasonOCCConflict}, nil
		} else if err != nil {
			return api.Result{}, err
		}
	}

	r.log.Info("reconciled normal",
		"key", key.String(), "rv", w.ResourceVersion, "externalExists", r.ext.Exists(w.Name))
	return api.Result{}, nil
}

// reconcileDelete runs cleanup while the object is deleting. The object MUST
// stay until cleanup succeeds. A cleanup failure keeps the finalizer in place
// and is retried (Requeue); success removes the finalizer, and the store then
// physically deletes the object.
func (r *WidgetReconciler) reconcileDelete(ctx context.Context, w *Widget) (api.Result, error) {
	key := api.Key{Kind: KindWidget, Name: w.Name}

	if !hasFinalizer(w.Finalizers, FinalizerWidget) {
		// Nothing we own to clean; nothing to remove.
		r.log.Info("delete: no owned finalizer", "key", key.String(), "reason", ReasonObjectGone)
		return api.Result{FailureReason: ReasonObjectGone}, nil
	}

	if err := r.ext.Cleanup(w.Name); err != nil {
		// Cleanup failed: KEEP the object and finalizer, request a retry. This
		// is the "cannot get stuck, cannot wrongly delete" guarantee: the key
		// is requeued with backoff and the external resource is still tracked.
		r.log.Warn("external cleanup failed; object retained for retry",
			"key", key.String(), "rv", w.ResourceVersion,
			"attempt", r.ext.CleanupAttempts(w.Name),
			"reason", ReasonCleanupInProgress, "err", err)
		return api.Result{Requeue: true, FailureReason: ReasonCleanupInProgress}, err
	}

	// Cleanup succeeded: remove our finalizer using the version we read.
	updated := w.DeepCopy().(*Widget)
	updated.Finalizers = removeFinalizer(w.Finalizers, FinalizerWidget)
	saved, err := r.store.Update(ctx, updated)
	if storage.IsConflict(err) {
		// Someone modified the deleting object. Re-list on the watch and
		// re-decide from the latest version instead of forcing finalizer off.
		r.log.Warn("finalizer removal conflict, defer to latest watch",
			"key", key.String(), "basedOnRV", w.ResourceVersion, "reason", ReasonOCCConflict, "err", err)
		return api.Result{FailureReason: ReasonOCCConflict}, nil
	}
	if err != nil {
		return api.Result{}, err
	}

	r.log.Info("cleanup done, finalizer removed, object physically deleted",
		"key", key.String(), "lastRV", saved.GetMeta().ResourceVersion,
		"externalExists", r.ext.Exists(w.Name))
	return api.Result{}, nil
}

func hasFinalizer(fs []string, want string) bool {
	for _, f := range fs {
		if f == want {
			return true
		}
	}
	return false
}

func removeFinalizer(fs []string, rm string) []string {
	out := fs[:0:0]
	for _, f := range fs {
		if f != rm {
			out = append(out, f)
		}
	}
	return out
}
