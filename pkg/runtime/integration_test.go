package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconciler"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/workqueue"
)

// instrumentedReconciler wraps the real one to assert the key-level
// "never reconciled concurrently" guarantee and to log key inputs/judgments.
type instrumentedReconciler struct {
	inner    reconciler.Reconciler
	logger   *slog.Logger
	mu       sync.Mutex
	inFlight map[api.Key]int
	maxSeen  int
	calls    int64
}

func newInstrumented(inner reconciler.Reconciler, logger *slog.Logger) *instrumentedReconciler {
	return &instrumentedReconciler{inner: inner, logger: logger, inFlight: map[api.Key]int{}}
}

func (r *instrumentedReconciler) Reconcile(ctx context.Context, key api.Key) (api.Result, error) {
	r.mu.Lock()
	r.inFlight[key]++
	if r.inFlight[key] > r.maxSeen {
		r.maxSeen = r.inFlight[key]
	}
	n := r.inFlight[key]
	r.mu.Unlock()
	atomic.AddInt64(&r.calls, 1)

	r.logger.Info("RECONCILE INPUT", "key", key.String(), "concurrentForKey", n)
	res, err := r.inner.Reconcile(ctx, key)
	r.logger.Info("RECONCILE JUDGMENT", "key", key.String(), "reason", res.FailureReason,
		"requeue", res.Requeue, "err", err)

	r.mu.Lock()
	r.inFlight[key]--
	r.mu.Unlock()
	return res, err
}

func (r *instrumentedReconciler) maxConcurrency() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxSeen
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func mkInstance(t *testing.T, id string, st storage.Store, rec reconciler.Reconciler) (*Instance, context.CancelFunc) {
	queueCfg := workqueue.DefaultConfig()
	queueCfg.BaseDelay = 10 * time.Millisecond
	queueCfg.MaxDelay = 200 * time.Millisecond
	inst := New(InstanceConfig{
		Identity:      id,
		LeaseName:     "widgets",
		Kind:          reconciler.KindWidget,
		Store:         st,
		Reconciler:    rec,
		Queue:         workqueue.New(queueCfg),
		Logger:        testLogger(),
		LeaseDuration: 300 * time.Millisecond,
		RenewDeadline: 150 * time.Millisecond,
		RetryPeriod:   30 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = inst.Run(ctx) }()
	return inst, cancel
}

func mkWidget(name string) *reconciler.Widget {
	w := &reconciler.Widget{}
	w.Name = name
	return w
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}

// TestConcurrentChangesConverge: many widgets created concurrently all end up
// with finalizer attached and external resources present, and no key is ever
// reconciled by two workers at once.
func TestConcurrentChangesConverge(t *testing.T) {
	st := storage.NewMemoryStore()
	ext := reconciler.NewExternalSystem()
	log := testLogger()
	rec := newInstrumented(reconciler.NewWidgetReconciler(st, ext, log), log)

	inst, cancel := mkInstance(t, "single", st, rec)
	_ = inst
	defer cancel()
	waitFor(t, time.Second, inst.IsLeader, "instance becomes leader")

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = st.Create(context.Background(), mkWidget("w"+itoa(i)))
		}(i)
	}
	wg.Wait()
	t.Logf("input: %d widgets created concurrently", n)

	waitFor(t, 3*time.Second, func() bool {
		objs, _ := st.List(context.Background(), reconciler.KindWidget)
		if len(objs) != n {
			return false
		}
		for _, o := range objs {
			if !hasFin(o.GetMeta().Finalizers) {
				return false
			}
			if !ext.Exists(o.GetMeta().Name) {
				return false
			}
		}
		return true
	}, "all widgets finalizer+external resource")

	if rec.maxConcurrency() > 1 {
		t.Fatalf("judgment: same key reconciled concurrently, max=%d", rec.maxConcurrency())
	}
	t.Logf("judgment: all %d converged; key max concurrency = %d; total calls=%d",
		n, rec.maxConcurrency(), atomic.LoadInt64(&rec.calls))
}

// TestInstanceFailoverReconciles: object created only after the leader dies is
// reconciled by the standby that takes over; no double leadership.
func TestInstanceFailoverReconciles(t *testing.T) {
	st := storage.NewMemoryStore()
	ext := reconciler.NewExternalSystem()
	log := testLogger()
	rec := newInstrumented(reconciler.NewWidgetReconciler(st, ext, log), log)

	a, cancelA := mkInstance(t, "a", st, rec)
	b, cancelB := mkInstance(t, "b", st, rec)
	defer cancelB()

	waitFor(t, time.Second, func() bool {
		return a.IsLeader() != b.IsLeader()
	}, "exactly one of a/b is leader")

	var leader, standby *Instance
	if a.IsLeader() {
		leader, standby = a, b
	} else {
		leader, standby = b, a
	}
	t.Logf("input: leader=%s, killing it cleanly; standby=%s should take over", leader.Identity(), standby.Identity())
	if leader == a {
		cancelA()
	} else {
		cancelB()
	}

	waitFor(t, 2*time.Second, func() bool {
		return standby.IsLeader()
	}, "standby becomes leader")
	t.Log("judgment: unique leader after failover")

	// Create the widget only after failover.
	if _, err := st.Create(context.Background(), mkWidget("after-failover")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		o, err := st.Get(context.Background(), reconciler.KindWidget, "after-failover")
		if err != nil {
			return false
		}
		return hasFin(o.GetMeta().Finalizers) && ext.Exists("after-failover")
	}, "new widget reconciled by new leader")
	t.Log("judgment: post-failover change converged under the new leader")
}

// TestCleanupFailureThenRecovery: a deleting widget with failing external
// cleanup is retained (finalizer kept, retry attempted); once failures stop it
// is physically deleted.
func TestCleanupFailureThenRecovery(t *testing.T) {
	st := storage.NewMemoryStore()
	ext := reconciler.NewExternalSystem()
	log := testLogger()
	rec := newInstrumented(reconciler.NewWidgetReconciler(st, ext, log), log)
	inst, cancel := mkInstance(t, "c", st, rec)
	defer cancel()
	waitFor(t, time.Second, inst.IsLeader, "leader")

	w := mkWidget("doomed")
	w.Finalizers = []string{reconciler.FinalizerWidget}
	if _, err := st.Create(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err := ext.Ensure("doomed"); err != nil {
		t.Fatal(err)
	}
	ext.SetCleanupFailure("doomed", true)
	t.Log("input: deleting doomed widget; external cleanup forced to FAIL")
	if err := st.Delete(context.Background(), reconciler.KindWidget, "doomed"); err != nil {
		t.Fatal(err)
	}

	// While failing: object must survive with finalizer and retries happen.
	time.Sleep(400 * time.Millisecond)
	o, err := st.Get(context.Background(), reconciler.KindWidget, "doomed")
	if err != nil {
		t.Fatalf("judgment: object must be retained during failing cleanup: %v", err)
	}
	if !hasFin(o.GetMeta().Finalizers) {
		t.Fatal("judgment: finalizer removed while cleanup failing -> wrong deletion")
	}
	if ext.CleanupAttempts("doomed") < 2 {
		t.Fatalf("judgment: expected retries, attempts=%d", ext.CleanupAttempts("doomed"))
	}
	t.Logf("judgment: retained with finalizer, cleanup attempts=%d (backoff retry works)",
		ext.CleanupAttempts("doomed"))

	ext.SetCleanupFailure("doomed", false)
	waitFor(t, 2*time.Second, func() bool {
		_, err := st.Get(context.Background(), reconciler.KindWidget, "doomed")
		return errors.Is(err, storage.ErrNotFound) && !ext.Exists("doomed")
	}, "object physically deleted and external resource gone")
	t.Log("judgment: after recovery, finalizer removed, object deleted, external gone")
}

// TestVersionConflictDoesNotOverwrite: a stale write is rejected; state still
// converges via watch re-list rather than blind overwrite.
func TestVersionConflictDoesNotOverwrite(t *testing.T) {
	st := storage.NewMemoryStore()
	ext := reconciler.NewExternalSystem()
	log := testLogger()
	rec := newInstrumented(reconciler.NewWidgetReconciler(st, ext, log), log)
	inst, cancel := mkInstance(t, "v", st, rec)
	defer cancel()
	waitFor(t, time.Second, inst.IsLeader, "leader")

	if _, err := st.Create(context.Background(), mkWidget("rv")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		o, err := st.Get(context.Background(), reconciler.KindWidget, "rv")
		return err == nil && hasFin(o.GetMeta().Finalizers)
	}, "initial reconcile converges")

	// Stale copy with rv=0 must be rejected.
	stale := mkWidget("rv")
	stale.GetMeta().ResourceVersion = 0
	_, err := st.Update(context.Background(), stale)
	if !storage.IsConflict(err) {
		t.Fatalf("judgment: stale update must conflict, got %v", err)
	}
	t.Log("judgment: stale rv=0 write rejected instead of overwrite")

	// Latest state remains consistent.
	cur, _ := st.Get(context.Background(), reconciler.KindWidget, "rv")
	if !hasFin(cur.GetMeta().Finalizers) {
		t.Fatal("latest state corrupted by stale write")
	}
}

func hasFin(fs []string) bool {
	for _, f := range fs {
		if f == reconciler.FinalizerWidget {
			return true
		}
	}
	return false
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
