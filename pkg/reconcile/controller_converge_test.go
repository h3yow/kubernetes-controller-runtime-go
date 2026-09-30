package reconcile_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconcile"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

func newFileStore(t *testing.T) *store.FileStore {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// runController starts a controller and returns a cancel func.
func runController(t *testing.T, st store.Store, id string, rec reconcile.Reconciler, log *slog.Logger) (cancel func()) {
	t.Helper()
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

func TestConvergesSpecToStatus(t *testing.T) {
	st := newFileStore(t)
	log := testLogger(t)

	rec := reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
		obj, err := c.Get(ctx, ns, name)
		if err != nil {
			return reconcile.Result{}, err
		}
		want, _ := obj.Spec["message"].(string)
		if cur, _ := obj.Status["echo"].(string); cur != want {
			if _, err := c.StatusUpdate(ctx, ns, name, map[string]any{"echo": want}); err != nil {
				return reconcile.Result{}, err
			}
			log.Info("decision: status drift", "key", obj.Key(), "rv", obj.ResourceVersion, "want", want, "cur", cur)
		}
		return reconcile.Result{}, nil
	})

	cancel := runController(t, st, "A", rec, log)
	defer cancel()

	time.Sleep(100 * time.Millisecond) // let election settle
	if _, err := st.AdminCreate(context.Background(), &store.Object{
		Kind: "Echo", Namespace: "ns", Name: "obj1", Spec: map[string]any{"message": "hello"},
	}); err != nil {
		t.Fatal(err)
	}

	if !waitFor(2*time.Second, func() bool {
		o, err := st.Get(context.Background(), "ns", "obj1")
		return err == nil && o.Status["echo"] == "hello"
	}) {
		t.Fatal("object never converged")
	}
	t.Log("initial spec reconciled into status=hello")

	// Concurrent rapid updates: controller must converge to the latest.
	for i := 0; i < 20; i++ {
		o, _ := st.Get(context.Background(), "ns", "obj1")
		o.Spec["message"] = string(rune('a' + i%26))
		_, _ = st.AdminCreate(context.Background(), o) // bump version
	}
	finalWant := string(rune('a' + 19%26)) // last write
	if !waitFor(2*time.Second, func() bool {
		o, err := st.Get(context.Background(), "ns", "obj1")
		return err == nil && o.Status["echo"] == finalWant
	}) {
		o, _ := st.Get(context.Background(), "ns", "obj1")
		t.Fatalf("did not converge to latest %q, got %v", finalWant, o.Status)
	}
	t.Logf("20 concurrent updates collapsed; final status=%q", finalWant)
}

func TestNoConcurrentReconcileSameObject(t *testing.T) {
	st := newFileStore(t)
	log := testLogger(t)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var calls atomic.Int32
	hold := make(chan struct{})
	firstStarted := make(chan struct{})
	var once sync.Once

	rec := reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		calls.Add(1)
		// Only the first call holds until the test releases it, so change
		// events pile up while exactly one reconcile is in flight.
		if calls.Load() == 1 {
			once.Do(func() { close(firstStarted) })
			select {
			case <-hold:
			case <-ctx.Done():
			}
		}
		inFlight.Add(-1)
		return reconcile.Result{}, nil
	})

	cancel := runController(t, st, "A", rec, log)
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	if _, err := st.AdminCreate(context.Background(), &store.Object{Kind: "W", Namespace: "ns", Name: "hot", Spec: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	<-firstStarted // first reconcile is now in flight and holding

	// Hammer updates while the first reconcile holds the object.
	for i := 0; i < 30; i++ {
		o, _ := st.Get(context.Background(), "ns", "hot")
		if o.Spec == nil {
			o.Spec = map[string]any{}
		}
		o.Spec["n"] = i
		_, _ = st.AdminCreate(context.Background(), o)
		time.Sleep(2 * time.Millisecond)
	}
	close(hold)

	// Wait until follow-up reconcile(s) drain and settle.
	if !waitFor(2*time.Second, func() bool { return calls.Load() >= 2 }) {
		t.Fatalf("expected follow-up reconcile(s), calls=%d", calls.Load())
	}
	time.Sleep(300 * time.Millisecond)
	if got := maxInFlight.Load(); got > 1 {
		t.Fatalf("same object reconciled concurrently, maxInFlight=%d", got)
	}
	if got := calls.Load(); got > 5 {
		t.Fatalf("dedup ineffective, too many reconcile calls: %d", got)
	}
	t.Logf("calls=%d maxConcurrentForSameKey=%d (30 in-flight events deduped)", calls.Load(), maxInFlight.Load())
}

func TestFailedReconcileBacksOffAndRetries(t *testing.T) {
	st := newFileStore(t)
	log := testLogger(t)

	var failures atomic.Int32
	var attempts atomic.Int32
	rec := reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
		obj, err := c.Get(ctx, ns, name)
		if err != nil {
			return reconcile.Result{}, err
		}
		n := attempts.Add(1)
		log.Info("reconcile attempt", "key", obj.Key(), "attempt", n, "failuresBefore", failures.Load())
		if attempts.Load() <= 3 {
			failures.Add(1)
			return reconcile.Result{}, errInjected
		}
		_, err = c.StatusUpdate(ctx, ns, name, map[string]any{"done": true})
		return reconcile.Result{}, err
	})

	cancel := runController(t, st, "A", rec, log)
	defer cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := st.AdminCreate(context.Background(), &store.Object{Kind: "W", Namespace: "ns", Name: "flaky"}); err != nil {
		t.Fatal(err)
	}

	if !waitFor(3*time.Second, func() bool {
		o, err := st.Get(context.Background(), "ns", "flaky")
		return err == nil && o.Status["done"] == true
	}) {
		t.Fatalf("did not converge after retries, attempts=%d failures=%d", attempts.Load(), failures.Load())
	}
	if attempts.Load() < 4 {
		t.Fatalf("expected at least 4 attempts (3 fail + success), got %d", attempts.Load())
	}
	t.Logf("succeeded after %d backoff retries; reason of failure was injectable error distinct from conflict/fence", attempts.Load()-1)
}
