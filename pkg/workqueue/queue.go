// Package workqueue is a deduplicating, per-key exponential-backoff queue:
// a key is processed by at most one worker at a time, concurrent changes while
// processing coalesce into exactly one follow-up requeue, and failures are
// retried with per-key exponential backoff.
package workqueue

import (
	"sync"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
)

// Config tunes the queue's exponential backoff.
type Config struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int // 0 = unlimited
	Clock       func() time.Time
}

// DefaultConfig returns deterministic defaults (no jitter) suitable for tests.
func DefaultConfig() Config {
	return Config{
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		MaxAttempts: 0,
		Clock:       time.Now,
	}
}

// Interface is the queue used by controllers.
type Interface interface {
	Add(key api.Key)
	AddAfter(key api.Key, d time.Duration)
	Get() (key api.Key, shutdown bool)
	Done(key api.Key)
	Forget(key api.Key)
	AddRateLimited(key api.Key)
	NumRequeues(key api.Key) int
	Len() int
	Shutdown()
	ShuttingDown() bool
}

// New builds a queue with the given config.
func New(cfg Config) Interface {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	q := &rateLimitingQueue{
		cfg:      cfg,
		queued:   map[api.Key]struct{}{},
		inFlight: map[api.Key]struct{}{},
		dirty:    map[api.Key]struct{}{},
		delayed:  map[api.Key]time.Time{},
		attempts: map[api.Key]int{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

type rateLimitingQueue struct {
	mu   sync.Mutex
	cond *sync.Cond
	cfg  Config

	// fifo holds keys ready for Get, in arrival order.
	fifo []api.Key
	// queued mirrors fifo membership for O(1) dedupe.
	queued map[api.Key]struct{}
	// inFlight holds keys handed out via Get but not yet Done.
	inFlight map[api.Key]struct{}
	// dirty marks keys changed while queued/inFlight; they must be processed
	// once more after the current round finishes.
	dirty map[api.Key]struct{}
	// delayed maps keys to their earliest ready time (AddAfter/backoff).
	delayed map[api.Key]time.Time
	// attempts counts failed attempts per key for backoff.
	attempts map[api.Key]int

	shuttingDown bool
}

func (q *rateLimitingQueue) Add(key api.Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shuttingDown {
		return
	}
	q.addLocked(key)
}

// addLocked enqueues immediately unless the key is currently in flight; in
// that case it only sets dirty so Done coalesces the change into one requeue.
func (q *rateLimitingQueue) addLocked(key api.Key) {
	if _, inflight := q.inFlight[key]; inflight {
		q.dirty[key] = struct{}{}
		return
	}
	if _, ok := q.queued[key]; ok {
		// Already waiting: the waiting item covers this change.
		q.dirty[key] = struct{}{}
		return
	}
	delete(q.delayed, key)
	q.queued[key] = struct{}{}
	q.fifo = append(q.fifo, key)
	q.cond.Broadcast()
}

func (q *rateLimitingQueue) AddAfter(key api.Key, d time.Duration) {
	q.mu.Lock()
	if q.shuttingDown {
		q.mu.Unlock()
		return
	}
	// If the key is in flight, the in-flight round plus a dirty requeue is
	// enough; no separate timer is needed.
	if _, inflight := q.inFlight[key]; inflight {
		q.dirty[key] = struct{}{}
		q.mu.Unlock()
		return
	}
	if _, ok := q.queued[key]; ok {
		q.mu.Unlock()
		return
	}
	readyAt := q.cfg.Clock().Add(d)
	if prev, ok := q.delayed[key]; !ok || readyAt.Before(prev) {
		q.delayed[key] = readyAt
	}
	q.mu.Unlock()

	time.AfterFunc(d, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		at, ok := q.delayed[key]
		if !ok || q.shuttingDown {
			return
		}
		if q.cfg.Clock().Before(at) {
			return // superseded by an earlier schedule
		}
		delete(q.delayed, key)
		q.addLocked(key)
	})
}

func (q *rateLimitingQueue) Get() (api.Key, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.fifo) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.fifo) == 0 {
		return api.Key{}, true
	}
	key := q.fifo[0]
	q.fifo = q.fifo[1:]
	delete(q.queued, key)
	q.inFlight[key] = struct{}{}
	return key, false
}

func (q *rateLimitingQueue) Done(key api.Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, key)
	// Coalesce every Add that happened during processing into exactly one
	// follow-up round.
	if _, changed := q.dirty[key]; changed {
		delete(q.dirty, key)
		if _, ok := q.queued[key]; !ok {
			q.queued[key] = struct{}{}
			q.fifo = append(q.fifo, key)
			q.cond.Broadcast()
		}
	}
}

func (q *rateLimitingQueue) Forget(key api.Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Success or object-gone: stop backing this key off. A future Add starts
	// the state machine over from zero.
	delete(q.dirty, key)
	delete(q.delayed, key)
	delete(q.attempts, key)
}

func (q *rateLimitingQueue) AddRateLimited(key api.Key) {
	q.mu.Lock()
	if q.shuttingDown {
		q.mu.Unlock()
		return
	}
	attempt := q.attempts[key] + 1
	q.attempts[key] = attempt
	if q.cfg.MaxAttempts > 0 && attempt > q.cfg.MaxAttempts {
		// Retry budget exhausted: do not enqueue anything new. The attempt
		// counter stops growing; the last legitimately scheduled retry (if
		// any) is left untouched. A Forget + Add restarts the state machine.
		q.attempts[key] = attempt - 1
		q.mu.Unlock()
		return
	}
	delay := q.backoffLocked(attempt)
	q.mu.Unlock()
	q.AddAfter(key, delay)
}

func (q *rateLimitingQueue) backoffLocked(attempt int) time.Duration {
	d := q.cfg.BaseDelay
	// 2^(attempt-1), capped to avoid overflow.
	for i := 1; i < attempt && d < q.cfg.MaxDelay; i++ {
		d *= 2
		if d > q.cfg.MaxDelay {
			d = q.cfg.MaxDelay
		}
	}
	if d > q.cfg.MaxDelay {
		d = q.cfg.MaxDelay
	}
	return d
}

func (q *rateLimitingQueue) NumRequeues(key api.Key) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.attempts[key]
}

func (q *rateLimitingQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.fifo) + len(q.delayed)
}

func (q *rateLimitingQueue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *rateLimitingQueue) ShuttingDown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shuttingDown
}
