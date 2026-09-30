// Package leaderelection implements lease-based leader election on top of the
// local Store. At any moment at most one instance per LeaseName holds the
// lease: the leader keeps renewing it; a clean stop releases it immediately,
// and a dead/silent instance loses it after LeaseDuration, after which another
// instance acquires within at most LeaseDuration + RetryPeriod.
//
// Split-brain window: leadership writes are protected by optimistic
// concurrency on the Lease. If an old leader's renewal conflicts because
// another instance acquired an expired lease, the old leader cancels its work
// context before issuing any further writes (within one RetryPeriod).
package leaderelection

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
)

func apiRV(v int64) api.ResourceVersion { return api.ResourceVersion(v) }

// Config configures a candidate instance.
type Config struct {
	LeaseName        string
	Identity         string
	LeaseDuration    time.Duration
	RenewDeadline    time.Duration
	RetryPeriod      time.Duration
	Store            storage.Store
	Clock            func() time.Time
	Logger           *slog.Logger
	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
}

// Elector is one competing runtime instance.
type Elector interface {
	Run(ctx context.Context)
	IsLeader() bool
	Identity() string
}

// New builds an elector.
func New(cfg Config) Elector {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &elector{cfg: cfg}
}

type elector struct {
	cfg Config

	mu     sync.Mutex
	leader bool
}

func (e *elector) setLeader(v bool) {
	e.mu.Lock()
	e.leader = v
	e.mu.Unlock()
}

func (e *elector) IsLeader() bool {
	e.mu.Lock()
	v := e.leader
	e.mu.Unlock()
	return v
}

func (e *elector) Identity() string { return e.cfg.Identity }

// Run executes the acquire/renew state machine until ctx is cancelled.
func (e *elector) Run(ctx context.Context) {
	e.cfg.Logger.Info("elector starting", "identity", e.cfg.Identity, "lease", e.cfg.LeaseName)
	for {
		if ctx.Err() != nil {
			return
		}
		ok, rv := e.tryAcquire(ctx)
		if !ok {
			if !sleep(ctx, e.cfg.RetryPeriod) {
				return
			}
			continue
		}

		leaderCtx, cancel := context.WithCancel(ctx)
		e.setLeader(true)
		e.cfg.Logger.Info("leadership acquired",
			"identity", e.cfg.Identity, "lease", e.cfg.LeaseName, "leaseRV", rv)
		if e.cfg.OnStartedLeading != nil {
			e.cfg.OnStartedLeading(leaderCtx)
		}

		lost := e.renewLoop(ctx, leaderCtx, rv)

		// Stop all leader-side work before anyone else can consider us gone:
		// cancel first, then announce loss, then release the lease.
		cancel()
		e.setLeader(false)
		if e.cfg.OnStoppedLeading != nil {
			e.cfg.OnStoppedLeading()
		}
		e.cfg.Logger.Warn("leadership lost",
			"identity", e.cfg.Identity, "lease", e.cfg.LeaseName, "reason", lost)

		if ctx.Err() == nil {
			// Renewal failed (process still alive): do NOT release; let the
			// lease expire so takeover timing is governed by LeaseDuration,
			// matching Kubernetes behavior.
			if !sleep(ctx, e.cfg.RetryPeriod) {
				return
			}
		} else {
			// Clean shutdown: expire the lease right away so a standby takes
			// over immediately. OCC ensures only the real holder releases it.
			e.releaseForCleanShutdown(context.Background())
			return
		}
	}
}

// tryAcquire creates the lease or takes over an expired one. It reports
// success and the lease rv the caller must renew from.
func (e *elector) tryAcquire(ctx context.Context) (bool, int64) {
	now := e.cfg.Clock().UTC().UnixMilli()
	obj, err := e.cfg.Store.Get(ctx, KindLease, e.cfg.LeaseName)
	if errors.Is(err, storage.ErrNotFound) {
		l := &Lease{
			HolderIdentity: e.cfg.Identity,
			AcquireTime:    now,
			RenewTime:      now,
			LeaseDuration:  e.cfg.LeaseDuration.Milliseconds(),
		}
		l.Name = e.cfg.LeaseName
		created, cerr := e.cfg.Store.Create(ctx, l)
		if cerr != nil {
			// Lost the create race: someone else holds it now.
			e.cfg.Logger.Debug("acquire create lost race", "identity", e.cfg.Identity, "err", cerr)
			return false, 0
		}
		return true, int64(created.GetMeta().ResourceVersion)
	}
	if err != nil {
		return false, 0
	}

	cur := obj.(*Lease)
	expireAt := time.UnixMilli(cur.RenewTime).Add(time.Duration(cur.LeaseDuration) * time.Millisecond)
	isOurs := cur.HolderIdentity == e.cfg.Identity
	if !isOurs && e.cfg.Clock().Before(expireAt) {
		// Another live holder; wait and retry.
		return false, 0
	}

	// Either it is ours (re-acquire after a renew failure) or it has expired.
	next := *cur
	next.HolderIdentity = e.cfg.Identity
	next.AcquireTime = now
	next.RenewTime = now
	next.LeaseDuration = e.cfg.LeaseDuration.Milliseconds()
	updated, uerr := e.cfg.Store.Update(ctx, &next)
	if uerr != nil {
		// Conflict: a competing standby won the expired-lease race.
		e.cfg.Logger.Debug("acquire update conflict", "identity", e.cfg.Identity, "err", uerr)
		return false, 0
	}
	return true, int64(updated.GetMeta().ResourceVersion)
}

// renewLoop keeps extending the lease from rv until ctx cancels or a renewal
// fails. It returns a human-readable loss reason.
func (e *elector) renewLoop(runCtx, leaderCtx context.Context, rv int64) string {
	ticker := time.NewTicker(e.cfg.RetryPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return "shutdown"
		case <-leaderCtx.Done():
			return "leader-context-cancelled"
		case <-ticker.C:
			newRV, reason, ok := e.renewOnce(runCtx, rv)
			if !ok {
				return reason
			}
			rv = newRV
		}
	}
}

func (e *elector) renewOnce(parent context.Context, rv int64) (int64, string, bool) {
	ctx, cancel := context.WithTimeout(parent, e.cfg.RenewDeadline)
	defer cancel()

	obj, err := e.cfg.Store.Get(ctx, KindLease, e.cfg.LeaseName)
	if err != nil {
		return 0, "renew-get-failed:" + err.Error(), false
	}
	cur := obj.(*Lease)
	if cur.HolderIdentity != e.cfg.Identity {
		// Someone else is already the holder: fencing says stop immediately.
		return 0, "lease-taken-over", false
	}
	if cur.GetMeta().ResourceVersion != apiRV(rv) {
		return 0, "lease-rv-changed", false
	}

	next := *cur
	next.RenewTime = e.cfg.Clock().UTC().UnixMilli()
	next.LeaseDuration = e.cfg.LeaseDuration.Milliseconds()
	updated, err := e.cfg.Store.Update(ctx, &next)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return 0, "renew-deadline-exceeded", false
		}
		return 0, "renew-update-failed:" + err.Error(), false
	}
	return int64(updated.GetMeta().ResourceVersion), "", true
}

// releaseForCleanShutdown makes the lease look expired at once, but only if we
// are still its optimistic-concurrency holder.
func (e *elector) releaseForCleanShutdown(ctx context.Context) {
	obj, err := e.cfg.Store.Get(ctx, KindLease, e.cfg.LeaseName)
	if err != nil {
		return
	}
	cur := obj.(*Lease)
	if cur.HolderIdentity != e.cfg.Identity {
		return
	}
	next := *cur
	next.RenewTime = e.cfg.Clock().Add(-e.cfg.LeaseDuration - time.Second).UnixMilli()
	if _, err := e.cfg.Store.Update(ctx, &next); err != nil {
		e.cfg.Logger.Debug("clean shutdown release failed", "identity", e.cfg.Identity, "err", err)
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
