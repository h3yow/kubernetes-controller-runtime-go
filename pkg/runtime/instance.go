// Package runtime composes leader election, the watch-driven deduplicating
// queue and a reconciler into one runnable controller-runtime instance. Run
// several Instances against the same Store to get competing controllers with
// a single writer at any time.
package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/controller"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/leaderelection"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconciler"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/workqueue"
)

// InstanceConfig builds one runtime instance.
type InstanceConfig struct {
	Identity      string
	LeaseName     string
	Kind          string
	Store         storage.Store
	Reconciler    reconciler.Reconciler
	Queue         workqueue.Interface
	Logger        *slog.Logger
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// Instance is one competing controller runtime.
type Instance struct {
	cfg        InstanceConfig
	elector    leaderelection.Elector
	controller *controller.Controller
}

// New constructs an instance.
func New(cfg InstanceConfig) *Instance {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 400 * time.Millisecond
	}
	if cfg.RenewDeadline == 0 {
		cfg.RenewDeadline = 200 * time.Millisecond
	}
	if cfg.RetryPeriod == 0 {
		cfg.RetryPeriod = 40 * time.Millisecond
	}
	if cfg.Queue == nil {
		cfg.Queue = workqueue.New(workqueue.DefaultConfig())
	}

	i := &Instance{cfg: cfg}
	ctrl := controller.New(controller.Config{
		Name:       cfg.Identity,
		Kind:       cfg.Kind,
		Store:      cfg.Store,
		Reconciler: cfg.Reconciler,
		Queue:      cfg.Queue,
		Logger:     cfg.Logger.WithGroup("ctrl"),
	})
	i.controller = ctrl

	i.elector = leaderelection.New(leaderelection.Config{
		LeaseName:     cfg.LeaseName,
		Identity:      cfg.Identity,
		LeaseDuration: cfg.LeaseDuration,
		RenewDeadline: cfg.RenewDeadline,
		RetryPeriod:   cfg.RetryPeriod,
		Store:         cfg.Store,
		Logger:        cfg.Logger.WithGroup("lease"),
		// On taking over, seed the queue from the latest state: while on
		// standby this instance's watcher suppressed event enqueues.
		OnStartedLeading: i.onStartedLeading,
		OnStoppedLeading: func() {},
	})
	return i
}

// Run starts controller workers + election and blocks until ctx ends.
func (i *Instance) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- i.controller.Run(ctx, i.elector.IsLeader)
	}()
	i.elector.Run(ctx)
	return <-errCh
}

func (i *Instance) onStartedLeading(leaderCtx context.Context) {
	if err := i.controller.Relist(leaderCtx); err != nil {
		i.cfg.Logger.Error("relist on acquire failed", "identity", i.cfg.Identity, "err", err)
	}
}

// IsLeader reports leadership of this instance.
func (i *Instance) IsLeader() bool { return i.elector.IsLeader() }

// Identity returns the instance identity.
func (i *Instance) Identity() string { return i.cfg.Identity }
