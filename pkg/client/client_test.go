package client_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

func openStore(t *testing.T) *store.FileStore {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(t.TempDir(), "s"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func leaderFence(t *testing.T, st store.Store) store.Fence {
	l, err := st.AcquireLease(context.Background(), "L", 60_000)
	if err != nil {
		t.Fatal(err)
	}
	return store.Fence{Holder: "L", Epoch: l.Epoch}
}

func TestFinalizerHelpers(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	f := leaderFence(t, st)
	c := client.New(st).WithFence(f)

	obj, err := c.Create(ctx, &store.Object{Kind: "W", Namespace: "ns", Name: "a", Spec: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created rv=%s; decision input: finalizers=%v", obj.ResourceVersion, obj.Finalizers)

	obj, err = c.AddFinalizer(ctx, obj, "f1")
	if err != nil {
		t.Fatal(err)
	}
	// Idempotent: adding the same finalizer is a no-op and must not bump rv.
	beforeRV := obj.ResourceVersion
	obj2, err := c.AddFinalizer(ctx, obj, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if obj2.ResourceVersion != beforeRV {
		t.Fatalf("idempotent AddFinalizer bumped rv %s -> %s", beforeRV, obj2.ResourceVersion)
	}
	t.Logf("add f1 ok rv=%s; duplicate add collapsed (no version bump)", obj2.ResourceVersion)

	// Stale object -> conflict, caller must re-Get (proven by refusing write).
	stale := *obj2
	stale.ResourceVersion = "0"
	if _, err := c.RemoveFinalizer(ctx, &stale, "f1"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want ErrConflict on stale remove, got %v", err)
	}
	t.Log("removeFinalizer with stale rv rejected as conflict (re-read required)")

	live, _ := c.Get(ctx, "ns", "a")
	live, err = c.RemoveFinalizer(ctx, live, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if len(live.Finalizers) != 0 {
		t.Fatalf("finalizer not removed: %v", live.Finalizers)
	}
	t.Log("removeFinalizer on latest rv succeeded -> object eligible for delete")
}

func TestWriteWithoutFenceRejected(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	// A client that never bound a fence cannot write.
	c := client.New(st)
	_, err := c.Create(ctx, &store.Object{Namespace: "ns", Name: "a"})
	if !errors.Is(err, store.ErrFence) {
		t.Fatalf("unfenced client write: want ErrFence, got %v", err)
	}
	t.Log("write through client without leader fence rejected")
}
