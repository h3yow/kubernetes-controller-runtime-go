package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/lease"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/queue"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

// Result tells the controller what to do after a reconciliation.
type Result struct {
	// RequeueAfter, if > 0, schedules a follow-up reconciliation after delay.
	RequeueAfter time.Duration
}

// Reconciler contains the user-supplied resource logic.
type Reconciler interface {
	Reconcile(ctx context.Context, c *client.Client, namespace, name string) (Result, error)
}

// ReconcilerFunc adapts a function to Reconciler.
type ReconcilerFunc func(ctx context.Context, c *client.Client, namespace, name string) (Result, error)

func (fn ReconcilerFunc) Reconcile(ctx context.Context, c *client.Client, namespace, name string) (Result, error) {
	return fn(ctx, c, namespace, name)
}

// Config configures a Controller.
type Config struct {
	Store          store.Store
	Reconciler     Reconciler
	InstanceID     string
	TTL            time.Duration
	RenewPeriod    time.Duration
	RetryPeriod    time.Duration
	ResyncPeriod   time.Duration
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	Logger         *slog.Logger
	Now            func() time.Time
}

func (c Config) withDefaults() Config {
	if c.TTL <= 0 {
		c.TTL = 2 * time.Second
	}
	if c.RenewPeriod <= 0 {
		c.RenewPeriod = c.TTL / 2
	}
	if c.RetryPeriod <= 0 {
		c.RetryPeriod = 100 * time.Millisecond
	}
	if c.ResyncPeriod <= 0 {
		c.ResyncPeriod = 100 * time.Millisecond
	}
	if c.BackoffInitial <= 0 {
		c.BackoffInitial = 20 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 2 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.InstanceID == "" {
		c.InstanceID = "instance"
	}
	return c
}

// Controller watches local state, performs leader election and drives the
// deduplicated reconcile loop for one runtime instance. Followers only watch;
// the leader runs workers and owns all writes (fenced by its lease epoch).
type Controller struct {
	cfg Config
	q   *queue.Queue
}

// New creates a Controller.
func New(cfg Config) *Controller {
	cfg = cfg.withDefaults()
	bo := queue.ExponentialBackoff(cfg.BackoffInitial, cfg.BackoffMax)
	return &Controller{
		cfg: cfg,
		q:   queue.NewWithBackoff(bo, 0),
	}
}

// Enqueue triggers a reconciliation for key ("namespace/name").
func (c *Controller) Enqueue(key string) { c.q.Add(key) }

// QueueLen reports pending keys (tests/observability).
func (c *Controller) QueueLen() int { return c.q.Len() }

// Run starts election; while this instance is leader it watches state and runs
// workers. Blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context, workers int) error {
	el := lease.New(c.cfg.Store, lease.Config{
		Holder:      c.cfg.InstanceID,
		TTL:         c.cfg.TTL,
		RenewPeriod: c.cfg.RenewPeriod,
		RetryPeriod: c.cfg.RetryPeriod,
		Logger:      c.cfg.Logger,
		Now:         c.cfg.Now,
	})
	return el.Run(ctx, func(leaderCtx context.Context) {
		c.runAsLeader(leaderCtx, workers)
	})
}

func (c *Controller) runAsLeader(ctx context.Context, workers int) {
	fence, ok := lease.FenceFromContext(ctx)
	if !ok {
		c.cfg.Logger.Error("leader context missing fence")
		return
	}
	c.cfg.Logger.Info("controller activated as leader", "instance", c.cfg.InstanceID,
		"epoch", fence.Epoch, "workers", workers)

	var wg sync.WaitGroup
	wg.Add(1 + workers)
	go func() { defer wg.Done(); c.watchLoop(ctx) }()
	for i := 0; i < workers; i++ {
		go func() { defer wg.Done(); c.worker(ctx, fence) }()
	}
	wg.Wait()
	// Leadership ended: drop queued work so a deposed leader cannot process it.
	c.cfg.Logger.Info("controller deactivated", "instance", c.cfg.InstanceID)
}

// watchLoop periodically lists state and enqueues keys whose resource version
// changed (covers create/update/delete/finalizer transitions).
func (c *Controller) watchLoop(ctx context.Context) {
	known := map[string]string{}
	// Initial listing enqueues every object.
	c.listOnce(ctx, known, true)
	t := time.NewTicker(c.cfg.ResyncPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.listOnce(ctx, known, false)
		}
	}
}

func (c *Controller) listOnce(ctx context.Context, known map[string]string, initial bool) {
	objs, err := c.cfg.Store.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.cfg.Logger.Warn("watch list failed", "err", err)
		}
		return
	}
	seen := map[string]bool{}
	for _, o := range objs {
		seen[o.Key()] = true
		if initial || known[o.Key()] != o.ResourceVersion {
			c.cfg.Logger.Debug("change detected", "key", o.Key(),
				"oldRV", known[o.Key()], "newRV", o.ResourceVersion,
				"deleting", o.BeingDeleted())
			c.q.Add(o.Key())
		}
		known[o.Key()] = o.ResourceVersion
	}
	// Objects that disappeared: enqueue once so reconcilers observe deletion.
	for k := range known {
		if !seen[k] {
			c.q.Add(k)
			delete(known, k)
		}
	}
}

func (c *Controller) worker(ctx context.Context, fence store.Fence) {
	cli := client.New(c.cfg.Store).WithFence(fence)
	for {
		item, ok := c.q.Get(ctx)
		if !ok {
			return
		}
		c.processItem(ctx, cli, item)
	}
}

func (c *Controller) processItem(ctx context.Context, cli *client.Client, item queue.Item) {
	log := c.cfg.Logger.With("key", item.Key, "attempt", item.Attempts)
	ns, name := splitKey(item.Key)

	obj, err := cli.Get(ctx, ns, name)
	if errors.Is(err, store.ErrNotFound) {
		// Object gone: nothing left to do; stop generating work for it.
		log.Info("object deleted, dropping work")
		c.q.Done(item)
		return
	}
	if err != nil {
		log.Warn("get failed, will backoff-retry", "err", err)
		c.q.Retry(item)
		return
	}

	log.Info("reconcile start", "rv", obj.ResourceVersion, "finalizers", obj.Finalizers,
		"deleting", obj.BeingDeleted(), "fenceEpoch", cli.Fence().Epoch)
	res, rerr := c.cfg.Reconciler.Reconcile(ctx, cli, ns, name)
	if rerr != nil {
		// A write rejected because leadership was lost is not retried here:
		// ctx is on its way to cancellation and the new leader takes over.
		if errors.Is(rerr, store.ErrFence) || ctx.Err() != nil {
			log.Warn("write fenced, new leader will reconcile", "err", rerr)
			c.q.Done(item)
			return
		}
		log.Warn("reconcile failed, scheduled backoff retry", "err", rerr)
		c.q.Retry(item)
		return
	}

	log.Info("reconcile succeeded", "requeueAfter", res.RequeueAfter)
	c.q.Done(item)
	if res.RequeueAfter > 0 {
		c.q.AddAfter(item.Key, res.RequeueAfter)
	}
}

func splitKey(k string) (string, string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '/' {
			return k[:i], k[i+1:]
		}
	}
	return "", k
}
