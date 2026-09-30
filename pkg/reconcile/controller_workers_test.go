package reconcile_test

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/client"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/reconcile"
)

func TestMultipleWorkersParallelDistinctKeysSerialSameKey(t *testing.T) {
	st := newFileStore(t)
	log := testLogger(t)

	var globalInFlight atomic.Int32
	var globalMax atomic.Int32
	var processedKeys atomic.Int32
	var perKeyMu sync.Mutex
	perKeyInFlight := map[string]int32{}
	perKeyMax := map[string]int32{}

	rec := reconcile.ReconcilerFunc(func(ctx context.Context, c *client.Client, ns, name string) (reconcile.Result, error) {
		key := ns + "/" + name
		n := globalInFlight.Add(1)
		for {
			m := globalMax.Load()
			if n <= m || globalMax.CompareAndSwap(m, n) {
				break
			}
		}
		perKeyMu.Lock()
		if perKeyInFlight[key] == 0 {
			processedKeys.Add(1)
		}
		perKeyInFlight[key]++
		if perKeyInFlight[key] > perKeyMax[key] {
			perKeyMax[key] = perKeyInFlight[key]
		}
		perKeyMu.Unlock()

		time.Sleep(20 * time.Millisecond) // encourage overlap across keys

		perKeyMu.Lock()
		perKeyInFlight[key]--
		perKeyMu.Unlock()
		globalInFlight.Add(-1)
		return reconcile.Result{}, nil
	})

	// Need a multi-worker variant of the helper; configure directly.
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := reconcile.New(reconcile.Config{
		Store: st, Reconciler: rec, InstanceID: "A",
		TTL: time.Second, RenewPeriod: 400 * time.Millisecond, RetryPeriod: 20 * time.Millisecond,
		ResyncPeriod: 10 * time.Millisecond, BackoffInitial: 10 * time.Millisecond, Logger: log,
	})
	done := make(chan struct{})
	go func() { defer close(done); _ = ctrl.Run(ctx, 4) }()

	time.Sleep(100 * time.Millisecond)
	for _, n := range []string{"k1", "k2", "k3", "k4"} {
		if _, err := st.AdminCreate(context.Background(), &store.Object{
			Kind: "W", Namespace: "ns", Name: n, Spec: map[string]any{}, Status: map[string]any{},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until all 4 distinct keys were processed at least once and any
	// overlapping window has been observed; workers sleep 20ms per reconcile.
	if !waitFor(2*time.Second, func() bool {
		return processedKeys.Load() == 4 && globalMax.Load() >= 2
	}) {
		t.Fatalf("did not process all keys with overlap: processed=%d globalMax=%d",
			processedKeys.Load(), globalMax.Load())
	}
	// Stop the controller before reading the per-key maps (avoid test race).
	cancel()
	<-done

	perKeyMu.Lock()
	defer perKeyMu.Unlock()
	for k, mx := range perKeyMax {
		if mx > 1 {
			t.Fatalf("key %s processed concurrently (max=%d)", k, mx)
		}
	}
	t.Logf("4 workers: globalMaxInFlight=%d (distinct keys parallel); per-key max=1 (never concurrent)",
		globalMax.Load())
}
