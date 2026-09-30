package reconciler

import (
	"context"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
)

// WidgetSpec is the desired state of a Widget.
type WidgetSpec struct {
	// FailCleanup, when true, makes the external resource cleanup fail until
	// FlipCleanupFailure is called on ExternalSystem. Used to exercise the
	// finalizer retry path deterministically.
	FailCleanup bool
}

// WidgetStatus is the observed state of a Widget.
type WidgetStatus struct {
	Phase string
}

// Widget is the demo resource reconciled by this runtime.
type Widget struct {
	api.ObjectMeta
	Spec   WidgetSpec
	Status WidgetStatus
}

const KindWidget = "Widget"

// FinalizerWidget is attached to widgets so external resources are cleaned up
// before the object is physically deleted.
const FinalizerWidget = "example.com/widget-cleanup"

func (w *Widget) Kind() string             { return KindWidget }
func (w *Widget) GetMeta() *api.ObjectMeta { return &w.ObjectMeta }
func (w *Widget) DeepCopy() api.Object {
	cp := *w
	cp.Finalizers = append([]string(nil), w.Finalizers...)
	if w.DeletionTimestamp != nil {
		t := *w.DeletionTimestamp
		cp.DeletionTimestamp = &t
	}
	return &cp
}

// Reconciler reacts to a single key. Defined here so the controller package can
// depend on the interface without import cycles.
type Reconciler interface {
	Reconcile(ctx context.Context, key api.Key) (api.Result, error)
}
