package store_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

func newTestStore(t *testing.T) *store.FileStore {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func acquire(t *testing.T, st store.Store, holder string) store.Fence {
	t.Helper()
	l, err := st.AcquireLease(context.Background(), holder, 10_000)
	if err != nil {
		t.Fatalf("AcquireLease(%s): %v", holder, err)
	}
	return store.Fence{Holder: holder, Epoch: l.Epoch}
}

func TestCreateGetUpdate_OptimisticConcurrency(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	f := acquire(t, st, "leader")

	obj, err := st.Create(ctx, f, &store.Object{Kind: "W", Namespace: "ns", Name: "a", Spec: map[string]any{"v": 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Logf("created rv=%s key=%s", obj.ResourceVersion, obj.Key())
	if obj.ResourceVersion != "1" {
		t.Fatalf("want rv 1, got %s", obj.ResourceVersion)
	}

	// Stale-version update must fail with ErrConflict and must not overwrite.
	stale := *obj
	stale.ResourceVersion = "0" // simulate a stale read
	stale.Spec = map[string]any{"v": 99}
	if _, err := st.Update(ctx, f, &stale); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale update: want ErrConflict, got %v", err)
	}
	t.Logf("stale rv=%s rejected as conflict (latest kept)", stale.ResourceVersion)

	// Latest-version update succeeds and bumps rv.
	live, err := st.Get(ctx, "ns", "a")
	if err != nil {
		t.Fatal(err)
	}
	live.Spec = map[string]any{"v": 2}
	updated, err := st.Update(ctx, f, live)
	if err != nil {
		t.Fatalf("update with latest rv: %v", err)
	}
	if updated.ResourceVersion != "2" {
		t.Fatalf("want rv 2, got %s", updated.ResourceVersion)
	}
	got, _ := st.Get(ctx, "ns", "a")
	if v, _ := got.Spec["v"].(float64); v != 2 {
		t.Fatalf("state not updated: %v", got.Spec)
	}
	t.Logf("fresh update accepted rv=%s, decision based on latest state", updated.ResourceVersion)
}

func TestWritesRequireFence(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// No lease at all: every write is fenced.
	if _, err := st.Create(ctx, store.Fence{}, &store.Object{Namespace: "ns", Name: "a"}); !errors.Is(err, store.ErrFence) {
		t.Fatalf("empty fence: want ErrFence, got %v", err)
	}

	f := acquire(t, st, "leader")
	if _, err := st.Create(ctx, f, &store.Object{Namespace: "ns", Name: "a"}); err != nil {
		t.Fatal(err)
	}

	// A different holder / stale epoch must be rejected (no double-write).
	if _, err := st.Create(ctx, store.Fence{Holder: "intruder", Epoch: 1},
		&store.Object{Namespace: "ns", Name: "b"}); !errors.Is(err, store.ErrFence) {
		t.Fatalf("foreign fence: want ErrFence, got %v", err)
	}
	if _, err := st.Create(ctx, store.Fence{Holder: "leader", Epoch: 999},
		&store.Object{Namespace: "ns", Name: "b"}); !errors.Is(err, store.ErrFence) {
		t.Fatalf("stale epoch: want ErrFence, got %v", err)
	}
	t.Log("writes from non-holder / stale epoch rejected")
}

func TestFenceRejectedAfterLeaseExpiry(t *testing.T) {
	st := newTestStore(t)
	base := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	clock := base
	st.SetClock(func() time.Time { return clock })
	ctx := context.Background()

	l, err := st.AcquireLease(ctx, "leader", 1000) // 1s TTL
	if err != nil {
		t.Fatal(err)
	}
	f := store.Fence{Holder: "leader", Epoch: l.Epoch}

	clock = base.Add(2 * time.Second) // lease expired
	if _, err := st.Create(ctx, f, &store.Object{Namespace: "ns", Name: "a"}); !errors.Is(err, store.ErrFence) {
		t.Fatalf("expired fence: want ErrFence, got %v", err)
	}
	t.Log("stale leader fence rejected after TTL expiry -> takeover is safe")

	// New leader acquires a higher epoch.
	l2, err := st.AcquireLease(ctx, "leader-2", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if l2.Epoch <= l.Epoch {
		t.Fatalf("epoch must increase: %d -> %d", l.Epoch, l2.Epoch)
	}
	t.Logf("new epoch %d > old %d", l2.Epoch, l.Epoch)
}

func TestFinalizerDeleteIsTombstoneThenPurge(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	f := acquire(t, st, "leader")

	var err error
	_, err = st.Create(ctx, f, &store.Object{Namespace: "ns", Name: "a", Finalizers: []string{"fin"}})
	if err != nil {
		t.Fatal(err)
	}
	// Delete with finalizer: tombstoned but still present.
	err = st.Delete(ctx, f, "ns", "a")
	if !errors.Is(err, store.ErrHasFinalizers) {
		t.Fatalf("want ErrHasFinalizers, got %v", err)
	}
	tomb, err := st.Get(ctx, "ns", "a")
	if err != nil {
		t.Fatalf("tombstoned object must remain readable: %v", err)
	}
	if tomb.DeletionTimestamp == nil {
		t.Fatal("DeletionTimestamp must be set")
	}
	t.Logf("tombstoned rv=%s deletionTimestamp=%d finalizers=%v",
		tomb.ResourceVersion, *tomb.DeletionTimestamp, tomb.Finalizers)

	// Repeated delete does not bump version and still returns the sentinel.
	err = st.Delete(ctx, f, "ns", "a")
	if !errors.Is(err, store.ErrHasFinalizers) {
		t.Fatalf("second delete: want ErrHasFinalizers, got %v", err)
	}
	tomb2, _ := st.Get(ctx, "ns", "a")
	if tomb2.ResourceVersion != tomb.ResourceVersion {
		t.Fatalf("idempotent tombstone must not bump rv: %s vs %s", tomb2.ResourceVersion, tomb.ResourceVersion)
	}

	// Remove finalizer and delete again -> purged.
	tomb.Finalizers = nil
	if _, err := st.Update(ctx, f, tomb); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, f, "ns", "a"); err != nil {
		t.Fatalf("final purge: %v", err)
	}
	if _, err := st.Get(ctx, "ns", "a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound after purge, got %v", err)
	}
	t.Log("object purged only after finalizer removed")
}

func TestLeaseRenewAndRelease(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	l, err := st.AcquireLease(ctx, "a", 10_000)
	if err != nil {
		t.Fatal(err)
	}
	// Same holder re-acquire returns same epoch.
	l2, err := st.AcquireLease(ctx, "a", 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if l2.Epoch != l.Epoch {
		t.Fatalf("re-acquire by same holder must not bump epoch: %d vs %d", l2.Epoch, l.Epoch)
	}
	// Other holder rejected while live.
	if _, err := st.AcquireLease(ctx, "b", 10_000); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	// Renew by owner works; foreign renew rejected.
	if _, err := st.RenewLease(ctx, "a", l.Epoch, 10_000); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if _, err := st.RenewLease(ctx, "b", l.Epoch, 10_000); !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("foreign renew: want ErrLeaseHeld, got %v", err)
	}
	t.Log("renew only accepted from current owner+epoch")
	// Release lets the other holder take over immediately.
	if err := st.ReleaseLease(ctx, "a", l.Epoch); err != nil {
		t.Fatal(err)
	}
	l3, err := st.AcquireLease(ctx, "b", 10_000)
	if err != nil {
		t.Fatalf("takeover after release: %v", err)
	}
	if l3.Epoch <= l.Epoch {
		t.Fatalf("takeover must increase epoch, got %d", l3.Epoch)
	}
	t.Logf("takeover succeeded with new epoch %d", l3.Epoch)
}

func TestDurableAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	f := acquire(t, st, "leader")
	if _, err := st.Create(context.Background(), f, &store.Object{Namespace: "ns", Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if _, err := st2.Get(context.Background(), "ns", "a"); err != nil {
		t.Fatalf("state did not survive reopen: %v", err)
	}
	t.Log("state persisted and readable after reopen")
}
