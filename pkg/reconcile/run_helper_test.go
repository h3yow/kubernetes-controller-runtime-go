package reconcile_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconcile"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

func runControllerWithStore(t *testing.T, st store.Store, id string, rec reconcile.Reconciler, log *slog.Logger) (cancel func()) {
	ctx, c := context.WithCancel(context.Background())
	done := make(chan struct{})
	ctrl := reconcile.New(reconcile.Config{
		Store:          st,
		Reconciler:     rec,
		InstanceID:     id,
		TTL:            time.Second,
		RenewPeriod:    400 * time.Millisecond,
		RetryPeriod:    20 * time.Millisecond,
		ResyncPeriod:   15 * time.Millisecond,
		BackoffInitial: 10 * time.Millisecond,
		BackoffMax:     100 * time.Millisecond,
		Logger:         log,
	})
	go func() { defer close(done); _ = ctrl.Run(ctx, 1) }()
	return func() { c(); <-done }
}
