package lease

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

// Config controls elector timing.
type Config struct {
	Holder      string
	TTL         time.Duration
	RenewPeriod time.Duration
	RetryPeriod time.Duration
	Logger      *slog.Logger
	Now         func() time.Time
}

func (c Config) withDefaults() Config {
	if c.TTL <= 0 {
		c.TTL = 2 * time.Second
	}
	if c.RenewPeriod <= 0 {
		c.RenewPeriod = c.TTL / 2
	}
	if c.RetryPeriod <= 0 {
		c.RetryPeriod = 200 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Elector performs leader election on top of the store lease, renewing while
// leader and handing control back when leadership is lost.
type Elector struct {
	store store.Store
	cfg   Config
}

// New builds an Elector.
func New(st store.Store, cfg Config) *Elector {
	return &Elector{store: st, cfg: cfg.withDefaults()}
}

// Run blocks until ctx is cancelled, invoking onStartedLeading with a leader
// context (cancelled when leadership is lost) each time this instance wins.
// Only one callback runs at a time and a fence (Holder, Epoch) is passed via
// the leader context so followers never write.
func (e *Elector) Run(ctx context.Context, onStartedLeading func(leaderCtx context.Context)) error {
	cfg := e.cfg
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l, err := e.store.AcquireLease(ctx, cfg.Holder, cfg.TTL.Milliseconds())
		if err != nil {
			if !errors.Is(err, store.ErrLeaseHeld) {
				cfg.Logger.Warn("acquire lease failed", "holder", cfg.Holder, "err", err)
			}
			if !sleep(ctx, cfg.RetryPeriod) {
				return ctx.Err()
			}
			continue
		}

		cfg.Logger.Info("became leader", "holder", cfg.Holder, "epoch", l.Epoch,
			"ttl_ms", cfg.TTL.Milliseconds())
		leaderCtx, cancel := context.WithCancel(ctx)
		leaderCtx = withFence(leaderCtx, store.Fence{Holder: cfg.Holder, Epoch: l.Epoch})

		lost := make(chan struct{})
		go func() {
			defer close(lost)
			e.renewLoop(leaderCtx, l.Epoch, cfg)
		}()

		// Run the leader work while renewing.
		done := make(chan struct{})
		go func() {
			defer close(done)
			onStartedLeading(leaderCtx)
		}()

		<-lost
		cancel()
		<-done
		if ctx.Err() == nil {
			// Voluntary handoff (lost renew but instance is still running):
			// release so another instance can take over immediately. When the
			// parent context is cancelled the instance is shutting down hard;
			// skip release to faithfully model a crash, forcing TTL-based
			// takeover with a new fencing epoch.
			_ = e.store.ReleaseLease(context.Background(), cfg.Holder, l.Epoch)
		}
		cfg.Logger.Info("lost leadership", "holder", cfg.Holder, "epoch", l.Epoch,
			"released", ctx.Err() == nil)

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// renewLoop keeps renewing until leaderCtx ends or a renewal fails.
func (e *Elector) renewLoop(leaderCtx context.Context, epoch int64, cfg Config) {
	t := time.NewTicker(cfg.RenewPeriod)
	defer t.Stop()
	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-t.C:
			_, err := e.store.RenewLease(leaderCtx, cfg.Holder, epoch, cfg.TTL.Milliseconds())
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					cfg.Logger.Warn("renew lease failed, stepping down",
						"holder", cfg.Holder, "epoch", epoch, "err", err)
				}
				return
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type fenceKey struct{}

func withFence(ctx context.Context, f store.Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, f)
}

// FenceFromContext extracts the leader fencing token.
func FenceFromContext(ctx context.Context) (store.Fence, bool) {
	f, ok := ctx.Value(fenceKey{}).(store.Fence)
	return f, ok
}
