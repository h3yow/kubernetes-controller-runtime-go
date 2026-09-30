package queue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/queue"
)

func TestDeduplicatesAdds(t *testing.T) {
	q := queue.New()
	defer q.Shutdown()
	q.Add("ns/a")
	q.Add("ns/a")
	q.Add("ns/a")
	if got := q.Len(); got != 1 {
		t.Fatalf("three Adds of same key -> Len=%d, want 1", got)
	}
	ctx := context.Background()
	it, ok := q.Get(ctx)
	if !ok {
		t.Fatal("Get failed")
	}
	if it.Key != "ns/a" {
		t.Fatalf("got %s", it.Key)
	}
	t.Logf("3x Add collapsed into one item %s", it.Key)
	// While processing, Len must be 0 (key is in the processing set).
	if got := q.Len(); got != 0 {
		t.Fatalf("in-flight key counted as waiting: Len=%d", got)
	}
	q.Done(it)
}

func TestReaddedWhileProcessingDeliveredOnce(t *testing.T) {
	q := queue.New()
	defer q.Shutdown()
	ctx := context.Background()
	q.Add("ns/a")
	it, _ := q.Get(ctx)

	// Many updates arrive while reconcile is in flight: must collapse to 1.
	q.Add("ns/a")
	q.Add("ns/a")
	q.Add("ns/a")
	q.Done(it)

	it2, ok := q.Get(ctx)
	if !ok {
		t.Fatal("expected follow-up item")
	}
	if it2.Key != "ns/a" {
		t.Fatalf("got %s", it2.Key)
	}
	// No third delivery.
	q.Done(it2)
	select {
	case extra := <-func() chan queue.Item {
		ch := make(chan queue.Item, 1)
		go func() {
			if x, ok := q.Get(ctx); ok {
				ch <- x
			}
		}()
		return ch
	}():
		t.Fatalf("unexpected extra delivery %v", extra)
	case <-time.After(50 * time.Millisecond):
	}
	t.Log("updates during in-flight reconcile produced exactly one follow-up")
}

func TestRetryBackoffThenSuccess(t *testing.T) {
	var n atomic.Int32
	bo := func(attempts int) time.Duration {
		n.Store(int32(attempts))
		return 5 * time.Millisecond
	}
	q := queue.NewWithBackoff(bo, 0)
	defer q.Shutdown()
	ctx := context.Background()
	q.Add("k")
	first, _ := q.Get(ctx)
	if first.Attempts != 0 {
		t.Fatalf("first attempt=%d", first.Attempts)
	}
	q.Retry(first) // failure #1 -> backoff
	start := time.Now()
	second, ok := q.Get(ctx)
	if !ok {
		t.Fatal("expected retried item")
	}
	elapsed := time.Since(start)
	if second.Attempts != 1 {
		t.Fatalf("retry attempt=%d, want 1", second.Attempts)
	}
	if elapsed < 4*time.Millisecond {
		t.Fatalf("retry delivered too fast: %v (backoff not honored)", elapsed)
	}
	t.Logf("retry attempt=%d delivered after %v backoff", second.Attempts, elapsed)
	q.Done(second)
}

func TestMaxAttemptsDeadLetter(t *testing.T) {
	q := queue.NewWithBackoff(queue.ExponentialBackoff(time.Millisecond, time.Millisecond), 2)
	defer q.Shutdown()
	ctx := context.Background()
	q.Add("k")
	var last Item
	for i := 0; i < 3; i++ {
		it, ok := q.Get(ctx)
		if !ok {
			t.Fatal("expected item")
		}
		q.Retry(it)
		last = it
	}
	_ = last
	// After maxAttempts the key is dropped; Get must time out.
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, ok := q.Get(ctx2); ok {
		t.Fatal("expected queue to be drained after max attempts")
	}
	t.Log("exhausted key dead-lettered instead of looping forever")
}

// Item alias to keep test helper short.
type Item = queue.Item
