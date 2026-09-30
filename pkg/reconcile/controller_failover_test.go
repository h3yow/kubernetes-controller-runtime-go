package reconcile_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconcile"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

func TestLeaderFailoverNoDoubleWrite(t *testing.T) {
	dir := t.TempDir() + "/state"
	log := testLogger(t)

	// Both instances share one recorder over one FileStore.
	openStore := func() (*store.FileStore, *recorder) {
		st, err := store.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		return st, newRecorder(st)
	}

	var writes atomic.Int32
	mkRec := func(id string) reconcile.Reconciler {
		return reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
			obj, err := c.Get(ctx, ns, name)
			if err != nil {
				return reconcile.Result{}, err
			}
			tag := "owner:" + id
			if cur := obj.Labels["owner"]; cur != tag {
				if obj.Labels == nil {
					obj.Labels = map[string]string{}
				}
				obj.Labels["owner"] = tag
				obj.Status["owner"] = tag
				if _, err := c.Update(ctx, obj); err != nil {
					return reconcile.Result{}, err
				}
				writes.Add(1)
				log.Info("write decision by active leader", "instance", id,
					"fenceEpoch", c.Fence().Epoch, "key", obj.Key())
			}
			return reconcile.Result{}, nil
		})
	}

	st1, rec1 := openStore()
	c1 := reconcile.New(reconcile.Config{
		Store: rec1, Reconciler: mkRec("A"), InstanceID: "A",
		TTL: 600 * time.Millisecond, RenewPeriod: 200 * time.Millisecond,
		RetryPeriod: 10 * time.Millisecond, ResyncPeriod: 15 * time.Millisecond, Logger: log,
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { defer close(done1); _ = c1.Run(ctx1, 1) }()

	time.Sleep(250 * time.Millisecond)
	if _, err := st1.AdminCreate(context.Background(), &store.Object{
		Kind: "W", Namespace: "ns", Name: "failover",
		Labels: map[string]string{}, Spec: map[string]any{}, Status: map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for A to reconcile once.
	if !waitFor(2*time.Second, func() bool {
		o, err := st1.Get(context.Background(), "ns", "failover")
		return err == nil && o.Labels["owner"] == "owner:A"
	}) {
		t.Fatal("A never reconciled")
	}
	epochA := currentEpoch(t, st1)
	t.Logf("A led with epoch %d", epochA)

	// A stops abruptly: cancel and wait for its Run to exit, but its context is
	// cancelled so the elector skips the voluntary ReleaseLease (crash
	// semantics). Close its handle only after it stopped touching the fd.
	cancel1()
	<-done1
	_ = st1.Close()

	// B starts and must take over after TTL expiry, with a strictly higher epoch.
	st2, rec2 := openStore()
	defer st2.Close()
	c2 := reconcile.New(reconcile.Config{
		Store: rec2, Reconciler: mkRec("B"), InstanceID: "B",
		TTL: 600 * time.Millisecond, RenewPeriod: 200 * time.Millisecond,
		RetryPeriod: 10 * time.Millisecond, ResyncPeriod: 15 * time.Millisecond, Logger: log,
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	startB := time.Now()
	go func() { _ = c2.Run(ctx2, 1) }()

	if !waitFor(3*time.Second, func() bool {
		o, err := st2.Get(context.Background(), "ns", "failover")
		return err == nil && o.Labels["owner"] == "owner:B"
	}) {
		t.Fatal("B did not take over and converge")
	}
	takeoverAfter := time.Since(startB)
	if takeoverAfter > 3*time.Second {
		t.Fatalf("takeover took too long: %v", takeoverAfter)
	}
	epochB := currentEpoch(t, st2)
	if epochB <= epochA {
		t.Fatalf("takeover epoch must increase: A=%d B=%d", epochA, epochB)
	}
	t.Logf("B took over after %v with epoch %d (> %d)", takeoverAfter.Round(time.Millisecond), epochB, epochA)

	// No write may be attributed to the old epoch after takeover.
	oldWrites := rec2.writesByEpoch()[epochA]
	if oldWrites != 0 {
		t.Fatalf("old epoch %d performed %d writes after losing lease (double write)", epochA, oldWrites)
	}
	if w := rec1.writesByEpoch()[epochA]; w == 0 {
		t.Fatal("expected A to have written while leader")
	}
	t.Logf("writes while A led: %d (epoch %d); writes attributed to stale epoch after takeover: 0",
		rec1.writesByEpoch()[epochA], epochA)
}

func currentEpoch(t *testing.T, st *store.FileStore) int64 {
	t.Helper()
	l, err := st.ReadLease(context.Background())
	if err != nil || l == nil {
		t.Fatalf("ReadLease: %v", err)
	}
	return l.Epoch
}

func TestConflictTriggersRereadNotOverwrite(t *testing.T) {
	st := newFileStore(t)
	log := testLogger(t)

	// Inject one conflict on Update for our key to simulate a concurrent writer.
	cs := &conflictOnce{Store: st, remaining: 1, targetKey: "ns/race"}

	var decisions atomic.Int32
	rec := reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
		obj, err := c.Get(ctx, ns, name)
		if err != nil {
			return reconcile.Result{}, err
		}
		want, _ := obj.Spec["shared"].(string)
		if cur, _ := obj.Status["observed"].(string); cur != want {
			obj.Status = map[string]any{"observed": want, "decidedFromRV": obj.ResourceVersion}
			if _, err := c.Update(ctx, obj); err != nil {
				decisions.Add(1)
				log.Warn("update rejected", "key", obj.Key(), "rv", obj.ResourceVersion, "err", err)
				return reconcile.Result{}, err
			}
			log.Info("wrote based on latest state", "key", obj.Key(), "rv", obj.ResourceVersion, "want", want)
		}
		return reconcile.Result{}, nil
	})

	cancel := runControllerWithStore(t, cs, "A", rec, log)
	defer cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := st.AdminCreate(context.Background(), &store.Object{
		Kind: "W", Namespace: "ns", Name: "race",
		Spec: map[string]any{"shared": "v1"}, Status: map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}

	// While the controller is retrying, an external writer sets a new value.
	if !waitFor(2*time.Second, func() bool { return cs.hits.Load() >= 1 }) {
		t.Fatal("expected injected conflict to be hit")
	}
	o, _ := st.Get(context.Background(), "ns", "race")
	o.Spec["shared"] = "v2"
	if _, err := st.AdminCreate(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	t.Log("concurrent writer advanced spec to v2 during conflict retry")

	if !waitFor(3*time.Second, func() bool {
		got, err := st.Get(context.Background(), "ns", "race")
		return err == nil && got.Status["observed"] == "v2"
	}) {
		got, _ := st.Get(context.Background(), "ns", "race")
		t.Fatalf("final state overwrote/stalemated: status=%v", got.Status)
	}
	got, _ := st.Get(context.Background(), "ns", "race")
	if got.Spec["shared"] != "v2" {
		t.Fatalf("concurrent spec change was clobbered: spec=%v", got.Spec)
	}
	t.Logf("conflict resolved from latest state: status.observed=v2, spec preserved=%v (conflict hits=%d)",
		got.Spec["shared"], cs.hits.Load())
}

func TestFinalizerCleanupFailureKeepsObjectAndRetries(t *testing.T) {
	st := newFileStore(t)
	log := testLogger(t)

	var cleanupCalls atomic.Int32
	var failUntil atomic.Int32 // fail the first N cleanup attempts
	failUntil.Store(2)

	fr := &reconcile.FinalizerReconciler{
		Name: "test.example.com/finalizer",
		Ensure: func(ctx context.Context, c *client.Client, obj *store.Object) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		},
		Cleanup: func(ctx context.Context, obj *store.Object) error {
			n := cleanupCalls.Add(1)
			log.Info("cleanup attempt", "key", obj.Key(), "attempt", n,
				"finalizers", obj.Finalizers, "deleting", obj.BeingDeleted())
			if n <= failUntil.Load() {
				return errInjected
			}
			return nil
		},
		Logger: log,
	}

	cancel := runController(t, st, "A", fr, log)
	defer cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := st.AdminCreate(context.Background(), &store.Object{
		Kind: "W", Namespace: "ns", Name: "fin",
		Finalizers: nil, Spec: map[string]any{}, Status: map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}

	// Finalizer must be added on first reconcile.
	if !waitFor(2*time.Second, func() bool {
		o, err := st.Get(context.Background(), "ns", "fin")
		return err == nil && len(o.Finalizers) == 1
	}) {
		t.Fatal("finalizer not added to alive object")
	}
	t.Log("alive object got finalizer")

	// Request deletion: object is tombstoned but retained while cleanup fails.
	fence := mustFence(t, st)
	if err := client.New(st).WithFence(fence).Delete(context.Background(), "ns", "fin"); err != store.ErrHasFinalizers {
		t.Fatalf("delete: want ErrHasFinalizers, got %v", err)
	}

	// While cleanup is failing the object must remain present, tombstoned,
	// with its finalizer. Poll to observe that intermediate state.
	sawRetained := false
	if !waitFor(2*time.Second, func() bool {
		o, err := st.Get(context.Background(), "ns", "fin")
		if err == nil && o.BeingDeleted() && len(o.Finalizers) == 1 {
			sawRetained = true
		}
		// keep waiting until object is eventually purged (proves retry path)
		_, gerr := st.Get(context.Background(), "ns", "fin")
		return gerr == store.ErrNotFound
	}) {
		t.Fatalf("object not purged after cleanup succeeded, cleanupCalls=%d", cleanupCalls.Load())
	}
	if !sawRetained {
		t.Fatal("never observed the retained-tombstone state during failing cleanup")
	}
	t.Log("observed: object retained with deletionTimestamp + finalizer while cleanup failed")
	if cleanupCalls.Load() < 3 {
		t.Fatalf("expected >=3 cleanup attempts (2 fail + success), got %d", cleanupCalls.Load())
	}
	t.Logf("object purged after %d cleanup attempts (first %d failed and retried with backoff)",
		cleanupCalls.Load(), failUntil.Load())
}

func mustFence(t *testing.T, st store.Store) store.Fence {
	t.Helper()
	l, err := st.ReadLease(context.Background())
	if err != nil || l == nil {
		t.Fatalf("no leader lease: %v", err)
	}
	return store.Fence{Holder: l.Holder, Epoch: l.Epoch}
}
