package workqueue

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
)

var k = api.Key{Kind: "Widget", Name: "x"}

func TestDedupeWhileQueued(t *testing.T) {
	q := New(DefaultConfig())
	defer q.Shutdown()
	q.Add(k)
	q.Add(k)
	q.Add(k)
	if q.Len() != 1 {
		t.Fatalf("want len 1, got %d", q.Len())
	}
	got, shutdown := q.Get()
	if shutdown || got != k {
		t.Fatalf("unexpected get: %v %v", got, shutdown)
	}
	q.Add(k)
	q.Add(k)
	if q.Len() != 0 {
		t.Fatalf("in-flight adds must not queue a second item, len=%d", q.Len())
	}
	q.Done(k)
	if q.Len() != 1 {
		t.Fatalf("coalesced requeue missing, len=%d", q.Len())
	}
}

func TestNoConcurrentSameKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseDelay = time.Millisecond
	q := New(cfg)

	var mu sync.Mutex
	inFlight := map[api.Key]int{}
	maxSeen := 0
	var processed int64

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				key, sd := q.Get()
				if sd {
					return
				}
				mu.Lock()
				inFlight[key]++
				if inFlight[key] > maxSeen {
					maxSeen = inFlight[key]
				}
				mu.Unlock()

				time.Sleep(2 * time.Millisecond)

				mu.Lock()
				inFlight[key]--
				processed++
				mu.Unlock()

				q.Done(key)
			}
		}()
	}

	// Hammer the same key; each in-flight Add marks dirty and guarantees
	// exactly one follow-up round, so processing stays busy.
	stop := make(chan struct{})
	var pwg sync.WaitGroup
	pwg.Add(1)
	go func() {
		defer pwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				q.Add(k)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	time.Sleep(60 * time.Millisecond)
	close(stop)
	pwg.Wait()
	q.Shutdown() // drains any in-flight + dirty requeue
	wg.Wait()

	if processed == 0 {
		t.Fatal("nothing processed")
	}
	if maxSeen > 1 {
		t.Fatalf("same key processed concurrently, max=%d, rounds=%d", maxSeen, processed)
	}
}

func TestExponentialBackoff(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseDelay = 10 * time.Millisecond
	cfg.MaxDelay = time.Second
	q := New(cfg).(*rateLimitingQueue)
	defer q.Shutdown()

	if d := q.backoffLocked(1); d != 10*time.Millisecond {
		t.Fatalf("attempt1 delay = %v", d)
	}
	if d := q.backoffLocked(2); d != 20*time.Millisecond {
		t.Fatalf("attempt2 delay = %v", d)
	}
	if d := q.backoffLocked(3); d != 40*time.Millisecond {
		t.Fatalf("attempt3 delay = %v", d)
	}
	if d := q.backoffLocked(100); d != time.Second {
		t.Fatalf("capped delay = %v", d)
	}
}

func TestMaxAttemptsDropsKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseDelay = 100 * time.Millisecond
	cfg.MaxAttempts = 2
	q := New(cfg)
	defer q.Shutdown()

	q.AddRateLimited(k)
	q.AddRateLimited(k)
	if q.Len() != 1 {
		t.Fatalf("want 1 delayed, got %d", q.Len())
	}
	q.AddRateLimited(k)
	if q.Len() != 1 {
		t.Fatalf("dropped key must not queue again, len=%d", q.Len())
	}
	if q.NumRequeues(k) != 2 {
		t.Fatalf("attempts should stay capped at 2, got %d", q.NumRequeues(k))
	}
	q.Forget(k)
	if q.NumRequeues(k) != 0 {
		t.Fatalf("forget should reset attempts, got %d", q.NumRequeues(k))
	}
}

func TestForgetResetsAttempts(t *testing.T) {
	q := New(DefaultConfig())
	defer q.Shutdown()
	q.AddRateLimited(k)
	q.AddRateLimited(k)
	if q.NumRequeues(k) != 2 {
		t.Fatalf("want 2 attempts, got %d", q.NumRequeues(k))
	}
	q.Forget(k)
	if q.NumRequeues(k) != 0 {
		t.Fatalf("attempts not reset")
	}
}
