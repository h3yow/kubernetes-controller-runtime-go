package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconciler"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
)

func mkWidget(name string) *reconciler.Widget {
	w := &reconciler.Widget{}
	w.Name = name
	return w
}

func TestCreateUpdateConflictDelete(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStore()

	created, err := st.Create(ctx, mkWidget("w1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetMeta().ResourceVersion != 1 {
		t.Fatalf("want rv=1, got %d", created.GetMeta().ResourceVersion)
	}

	// Stale write (rv=0 against rv=1) must be rejected, never overwrite.
	stale := created.DeepCopy()
	stale.GetMeta().ResourceVersion = 0
	if _, err := st.Update(ctx, stale); !storage.IsConflict(err) {
		t.Fatalf("want conflict, got %v", err)
	}

	// An up-to-date write succeeds and bumps the global rv.
	cur, _ := st.Get(ctx, reconciler.KindWidget, "w1")
	cur.GetMeta().Finalizers = []string{reconciler.FinalizerWidget}
	updated, err := st.Update(ctx, cur)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.GetMeta().ResourceVersion != 2 {
		t.Fatalf("want rv=2, got %d", updated.GetMeta().ResourceVersion)
	}

	// Delete with a finalizer: object survives, timestamp is set.
	if err := st.Delete(ctx, reconciler.KindWidget, "w1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := st.Get(ctx, reconciler.KindWidget, "w1")
	if err != nil {
		t.Fatalf("object must survive deletion while finalizer present: %v", err)
	}
	if got.GetMeta().DeletionTimestamp == nil {
		t.Fatalf("deletionTimestamp not set")
	}

	// Finalizer removed -> next delete physically removes the object.
	got.GetMeta().Finalizers = nil
	if _, err := st.Update(ctx, got); err != nil {
		t.Fatalf("update finalizers: %v", err)
	}
	if err := st.Delete(ctx, reconciler.KindWidget, "w1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := st.Get(ctx, reconciler.KindWidget, "w1"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestWatchSnapshotAndEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := storage.NewMemoryStore()

	if _, err := st.Create(ctx, mkWidget("seed")); err != nil {
		t.Fatal(err)
	}

	snap, ch, err := st.Subscribe(ctx, reconciler.KindWidget)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 1 || snap[0].GetMeta().Name != "seed" {
		t.Fatalf("bad snapshot: %v", snap)
	}

	if _, err := st.Create(ctx, mkWidget("after-subscribe")); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Type != api.EventAdded || ev.Key.Name != "after-subscribe" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("watch event missed (list/watch gap)")
	}
}

func TestDeleteWithoutFinalizerIsImmediate(t *testing.T) {
	ctx := context.Background()
	st := storage.NewMemoryStore()
	if _, err := st.Create(ctx, mkWidget("w2")); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, reconciler.KindWidget, "w2"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, reconciler.KindWidget, "w2"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
}
