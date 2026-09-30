// Package controller wires a store watch to a deduplicating work queue and a
// Reconciler. Watchers run on every instance for resilience, but workers only
// call the Reconciler while this instance holds the leader lease; that is the
// single-writer guarantee across runtime instances.
package controller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconciler"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/workqueue"
)

// Config builds a Controller.
type Config struct {
	Name       string
	Kind       string
	Store      storage.Store
	Reconciler reconciler.Reconciler
	Queue      workqueue.Interface
	Logger     *slog.Logger
}

// Controller watches Kind and reconciles keys while this instance is leader.
type Controller struct {
	cfg Config
}

// New builds a Controller.
func New(cfg Config) *Controller {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Controller{cfg: cfg}
}

// Run starts the watcher and workers. Workers stay alive for the whole process
// but only reconcile while isLeading() is true; Run returns when ctx ends.
func (c *Controller) Run(ctx context.Context, isLeading func() bool) error {
	log := c.cfg.Logger.With("controller", c.cfg.Name)

	// Initial list: enqueue every existing object once. Subscribe gives a
	// consistent snapshot + event stream under one store lock, so nothing is
	// missed between list and watch.
	snapshot, events, err := c.cfg.Store.Subscribe(ctx, c.cfg.Kind)
	if err != nil {
		return err
	}
	for _, o := range snapshot {
		c.cfg.Queue.Add(api.Key{Kind: o.Kind(), Name: o.GetMeta().Name})
	}
	log.Info("initial list enqueued", "count", len(snapshot))

	var wg sync.WaitGroup

	// Watcher: enqueue on every change; queue dedupes concurrent changes. The
	// watcher only enqueues while this instance is leader; a fresh Relist on
	// leadership acquisition seeds the queue with the current world.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				log.Info("watch event", "type", ev.Type, "key", ev.Key.String(),
					"rv", ev.Object.GetMeta().ResourceVersion, "leader", isLeading())
				if isLeading() {
					c.cfg.Queue.Add(ev.Key)
				}
			}
		}
	}()

	// Workers.
	const workers = 2
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.worker(ctx, id, isLeading, log)
		}(i)
	}

	<-ctx.Done()
	c.cfg.Queue.Shutdown()
	wg.Wait()
	return ctx.Err()
}

func (c *Controller) worker(ctx context.Context, id int, isLeading func() bool, log *slog.Logger) {
	for {
		key, shutdown := c.cfg.Queue.Get()
		if shutdown {
			return
		}
		c.handleKey(ctx, id, key, isLeading, log)
	}
}

// handleKey processes one key end to end and always calls Done, so the queue
// can release its in-flight slot and apply dirty coalescing.
func (c *Controller) handleKey(ctx context.Context, workerID int, key api.Key, isLeading func() bool, log *slog.Logger) {
	if !isLeading() {
		// Standby: this instance must not write. Drop the local queue item
		// (Done without re-add); when we acquire leadership, Relist seeds the
		// queue with the latest state and subsequent watch events flow again.
		log.Debug("standby skips key", "worker", workerID, "key", key.String())
		c.cfg.Queue.Done(key)
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
		return
	}

	log.Info("reconcile start", "worker", workerID, "key", key.String())
	res, err := c.cfg.Reconciler.Reconcile(ctx, key)
	c.cfg.Queue.Done(key)

	switch {
	case err != nil:
		// Transient failure (incl. external cleanup failure): exponential
		// backoff retry. Key state is kept; no Forget.
		log.Warn("reconcile failed, backing off",
			"worker", workerID, "key", key.String(),
			"attempt", c.cfg.Queue.NumRequeues(key),
			"reason", res.FailureReason, "err", err)
		c.cfg.Queue.AddRateLimited(key)
	case res.FailureReason == reconciler.ReasonObjectGone:
		// Converged to absent: stop tracking so no new reconcile is produced
		// until a fresh watch Add arrives.
		log.Info("reconcile settled: object gone", "worker", workerID, "key", key.String())
		c.cfg.Queue.Forget(key)
	case res.FailureReason == reconciler.ReasonOCCConflict:
		// We lost an optimistic-concurrency race. The watch event carrying the
		// winning version already re-added the key; a short safety requeue
		// covers the rare ordering case without overwriting anything.
		log.Warn("reconcile deferred after OCC conflict", "worker", workerID, "key", key.String())
		c.cfg.Queue.AddAfter(key, 5*time.Millisecond)
	case res.Requeue:
		log.Warn("requeue requested", "worker", workerID, "key", key.String(),
			"reason", res.FailureReason)
		c.cfg.Queue.AddRateLimited(key)
	case res.RequeueAfter > 0:
		c.cfg.Queue.Forget(key)
		c.cfg.Queue.AddAfter(key, res.RequeueAfter)
	default:
		// Success: reset backoff. No new reconcile until the next watch event.
		log.Info("reconcile success", "worker", workerID, "key", key.String())
		c.cfg.Queue.Forget(key)
	}
}

// ErrNotLeader is returned when an action is attempted by a standby instance.
var ErrNotLeader = errors.New("not the leader")

// Relist seeds the queue from the current store contents. Call it when
// leadership is acquired so the new leader catches up on changes it did not
// enqueue while on standby (the watcher suppresses events for non-leaders).
// The queue deduplicates, so calling it more than once is harmless.
func (c *Controller) Relist(ctx context.Context) error {
	objs, err := c.cfg.Store.List(ctx, c.cfg.Kind)
	if err != nil {
		return err
	}
	for _, o := range objs {
		c.cfg.Queue.Add(api.Key{Kind: o.Kind(), Name: o.GetMeta().Name})
	}
	c.cfg.Logger.Info("relist on leadership acquired", "controller", c.cfg.Name, "count", len(objs))
	return nil
}
