// Command demo runs two controller-runtime instances against one local store
// and walks through: leader election, reconciliation, clean failover, and a
// finalizer cleanup failure followed by recovery.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconciler"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/runtime"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/workqueue"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	st := storage.NewMemoryStore()
	ext := reconciler.NewExternalSystem()
	rec := reconciler.NewWidgetReconciler(st, ext, logger.WithGroup("reconcile"))

	queueCfg := workqueue.DefaultConfig()
	queueCfg.BaseDelay = 100 * time.Millisecond
	queueCfg.MaxDelay = 1 * time.Second

	mk := func(id string) (*runtime.Instance, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		inst := runtime.New(runtime.InstanceConfig{
			Identity:      id,
			LeaseName:     "widgets",
			Kind:          reconciler.KindWidget,
			Store:         st,
			Reconciler:    rec,
			Queue:         workqueue.New(queueCfg),
			Logger:        logger.WithGroup(id),
			LeaseDuration: 500 * time.Millisecond,
			RenewDeadline: 250 * time.Millisecond,
			RetryPeriod:   50 * time.Millisecond,
		})
		go func() {
			if err := inst.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("instance exited", "id", id, "err", err)
			}
		}()
		return inst, cancel
	}

	instA, cancelA := mk("instance-A")
	instB, cancelB := mk("instance-B")
	defer cancelB()

	// 1) Wait for a unique leader.
	time.Sleep(600 * time.Millisecond)
	logger.Info("STEP 1 leader elected", "A_leader", instA.IsLeader(), "B_leader", instB.IsLeader())

	// 2) Create a widget: the leader attaches the finalizer and creates the
	// external resource.
	w1 := &reconciler.Widget{}
	w1.Name = "widget-1"
	if _, err := st.Create(context.Background(), w1); err != nil {
		logger.Error("create widget-1", "err", err)
		os.Exit(1)
	}
	time.Sleep(500 * time.Millisecond)
	if o, err := st.Get(context.Background(), reconciler.KindWidget, "widget-1"); err == nil {
		logger.Info("STEP 2 reconciled", "rv", o.GetMeta().ResourceVersion,
			"finalizers", o.GetMeta().Finalizers, "externalExists", ext.Exists("widget-1"))
	}

	// 3) Clean failover: stop the current leader; standby takes over.
	if instA.IsLeader() {
		logger.Info("STEP 3 stopping leader A cleanly -> B should take over")
		cancelA()
	} else {
		logger.Info("STEP 3 stopping leader B cleanly -> A should take over")
		cancelB()
	}
	time.Sleep(700 * time.Millisecond)
	logger.Info("after failover", "A_leader", instA.IsLeader(), "B_leader", instB.IsLeader())

	// 4) Finalizer cleanup failure then recovery.
	w2 := &reconciler.Widget{}
	w2.Name = "widget-2"
	w2.Finalizers = []string{reconciler.FinalizerWidget}
	if _, err := st.Create(context.Background(), w2); err != nil {
		logger.Error("create widget-2", "err", err)
		os.Exit(1)
	}
	_ = ext.Ensure("widget-2")
	ext.SetCleanupFailure("widget-2", true)
	logger.Info("STEP 4 deleting widget-2 with external cleanup FAILING (retries, object retained)")
	if err := st.Delete(context.Background(), reconciler.KindWidget, "widget-2"); err != nil {
		logger.Error("delete widget-2", "err", err)
		os.Exit(1)
	}
	time.Sleep(1200 * time.Millisecond)
	if o, err := st.Get(context.Background(), reconciler.KindWidget, "widget-2"); err == nil {
		logger.Info("during failing cleanup", "present", true,
			"finalizers", o.GetMeta().Finalizers,
			"cleanupAttempts", ext.CleanupAttempts("widget-2"))
	}

	logger.Info("STEP 5 clearing injected failure -> cleanup succeeds, object physically deleted")
	ext.SetCleanupFailure("widget-2", false)
	time.Sleep(1000 * time.Millisecond)
	if _, err := st.Get(context.Background(), reconciler.KindWidget, "widget-2"); err != nil {
		logger.Info("final state", "widget-2", "gone", "err", err)
	} else {
		logger.Warn("final state", "widget-2", "still present (unexpected)")
	}
}
