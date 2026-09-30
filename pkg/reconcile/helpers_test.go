package reconcile_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

// testLogger writes structured logs through t.Log so key inputs and decisions
// appear in unit-test output.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Writer(testWriter{t}), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// recorder wraps a Store and records the fencing epoch of every successful
// write, so tests can assert which leader epoch performed each mutation.
type recorder struct {
	store.Store
	mu      sync.Mutex
	writeOf map[int64]int64 // epoch -> number of successful writes
	order   []int64         // epochs of successful writes, in order
}

func newRecorder(inner store.Store) *recorder {
	return &recorder{Store: inner, writeOf: map[int64]int64{}}
}

func (r *recorder) record(f store.Fence) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeOf[f.Epoch]++
	r.order = append(r.order, f.Epoch)
}

func (r *recorder) writesByEpoch() map[int64]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[int64]int64{}
	for k, v := range r.writeOf {
		out[k] = v
	}
	return out
}

func (r *recorder) epochOrder() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.order...)
}

func (r *recorder) Create(ctx context.Context, f store.Fence, obj *store.Object) (*store.Object, error) {
	o, err := r.Store.Create(ctx, f, obj)
	if err == nil {
		r.record(f)
	}
	return o, err
}

func (r *recorder) Update(ctx context.Context, f store.Fence, obj *store.Object) (*store.Object, error) {
	o, err := r.Store.Update(ctx, f, obj)
	if err == nil {
		r.record(f)
	}
	return o, err
}

func (r *recorder) Delete(ctx context.Context, f store.Fence, ns, name string) error {
	err := r.Store.Delete(ctx, f, ns, name)
	// Tombstoning is a successful state mutation even though ErrHasFinalizers
	// is returned; attribute it to the writer.
	if err == nil || err == store.ErrHasFinalizers {
		r.record(f)
	}
	return err
}

// conflictOnce makes the first N Update calls carrying a specific key fail
// with ErrConflict, simulating a concurrent writer.
type conflictOnce struct {
	store.Store
	mu        sync.Mutex
	remaining int
	targetKey string
	hits      atomic.Int32
}

func (c *conflictOnce) Update(ctx context.Context, f store.Fence, obj *store.Object) (*store.Object, error) {
	c.mu.Lock()
	if c.remaining > 0 && (c.targetKey == "" || obj.Key() == c.targetKey) {
		c.remaining--
		c.mu.Unlock()
		c.hits.Add(1)
		return nil, store.ErrConflict
	}
	c.mu.Unlock()
	return c.Store.Update(ctx, f, obj)
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// errInjected is a user-logic failure, distinct from store conflict/fence.
var errInjected = errors.New("injected reconcile failure")
