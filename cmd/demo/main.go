// Command demo runs two controller instances against a shared local store and
// creates/updates/deletes a sample object, printing the convergence trace.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconcile"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

const finalizer = "demo.example.com/cleanup"

func main() {
	dir := flag.String("dir", "./.demo-state", "shared state directory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	st, err := store.NewFileStore(*dir)
	must(err)
	defer st.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	rec := reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
		obj, err := c.Get(ctx, ns, name)
		if err != nil {
			return reconcile.Result{}, err
		}
		switch {
		case obj.BeingDeleted():
			if contains(obj.Finalizers, finalizer) {
				logger.Info("running external cleanup", "key", obj.Key())
				time.Sleep(50 * time.Millisecond)
				if _, err := c.RemoveFinalizer(ctx, obj, finalizer); err != nil {
					return reconcile.Result{}, err
				}
				// Finalizers cleared; the now-allowed delete purges the object.
				if err := c.Delete(ctx, ns, name); err != nil && err != store.ErrNotFound {
					return reconcile.Result{}, err
				}
				logger.Info("finalizer done, object purged", "key", obj.Key())
			}
			return reconcile.Result{}, nil
		default:
			want := "1"
			if v, ok := obj.Spec["replicas"]; ok {
				want = fmt.Sprint(v)
			}
			if cur := fmt.Sprint(obj.Status["readyReplicas"]); cur != want {
				if _, err := c.AddFinalizer(ctx, obj, finalizer); err != nil {
					return reconcile.Result{}, err
				}
				obj, err = c.Get(ctx, ns, name)
				if err != nil {
					return reconcile.Result{}, err
				}
				if _, err := c.StatusUpdate(ctx, ns, name, map[string]any{"readyReplicas": want}); err != nil {
					return reconcile.Result{}, err
				}
				logger.Info("status converged", "key", obj.Key(), "readyReplicas", want)
			}
		}
		return reconcile.Result{}, nil
	})

	c1 := reconcile.New(reconcile.Config{
		Store: st, Reconciler: rec, InstanceID: "instance-A",
		TTL: 2 * time.Second, RenewPeriod: time.Second, Logger: logger,
	})
	c2 := reconcile.New(reconcile.Config{
		Store: st, Reconciler: rec, InstanceID: "instance-B",
		TTL: 2 * time.Second, RenewPeriod: time.Second, Logger: logger,
	})
	go func() { must(c1.Run(ctx, 1)) }()
	go func() { must(c2.Run(ctx, 1)) }()

	time.Sleep(300 * time.Millisecond)
	// Create via a separate fenced client: acquire a one-shot lease is not
	// possible; controllers are leader, so simulate external writer by using
	// the current leader fence read from the lease.
	_, err = st.AdminCreate(ctx, &store.Object{Kind: "Widget", Namespace: "default", Name: "demo", Spec: map[string]any{"replicas": 3}})
	must(err)
	time.Sleep(500 * time.Millisecond)
	fmt.Println("--- deleting ---")
	fence, err := st.ReadLease(ctx)
	must(err)
	cli := client.New(st).WithFence(store.Fence{Holder: fence.Holder, Epoch: fence.Epoch})
	err = cli.Delete(ctx, "default", "demo")
	if err != nil && err != store.ErrHasFinalizers {
		must(err)
	}
	fmt.Println("delete accepted (finalizer pending), waiting for cleanup...")
	time.Sleep(800 * time.Millisecond)
	obj, err := st.Get(ctx, "default", "demo")
	fmt.Printf("final get: obj=%v err=%v\n", obj, err)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
