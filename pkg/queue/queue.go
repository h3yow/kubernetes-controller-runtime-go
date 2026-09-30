package queue

import (
	"container/heap"
	"context"
	"time"
)

// Item is a unit of work identified by a stable key.
type Item struct {
	Key        string
	EnqueuedAt time.Time
	// Attempts is the number of consecutive failures so far.
	Attempts int
}

type reqKind int

const (
	reqAdd reqKind = iota
	reqAddAfter
	reqRetry
	reqDone
	reqGiveBack
	reqLen
	reqShutdown
)

type request struct {
	kind  reqKind
	key   string
	item  Item
	at    time.Time
	reply chan int
}

type waiter struct {
	ctx   context.Context
	reply chan Item
}

// Queue is a deduplicating work queue with per-key failure backoff. The same
// key is never reconciled concurrently: while a key is processing it is kept
// in the processing set; any Add during that window marks it dirty and it is
// delivered exactly once more after Done.
type Queue struct {
	requests chan request
	getting  chan waiter
	giveback chan request
	stopped  chan struct{}

	delay       func(attempts int) time.Duration
	maxAttempts int
}

// New creates a Queue with default exponential backoff and unlimited retries.
func New() *Queue {
	return NewWithBackoff(defaultBackoff, 0)
}

// NewWithBackoff creates a Queue whose per-key failure delay is computed by
// backoffFn(attempts). maxAttempts <= 0 means unlimited retries.
func NewWithBackoff(backoffFn func(attempts int) time.Duration, maxAttempts int) *Queue {
	q := &Queue{
		requests:    make(chan request, 1024),
		getting:     make(chan waiter, 64),
		giveback:    make(chan request, 64),
		stopped:     make(chan struct{}),
		delay:       backoffFn,
		maxAttempts: maxAttempts,
	}
	go q.loop()
	return q
}

// ExponentialBackoff returns base*2^(attempts-1) capped at max.
func ExponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(attempts int) time.Duration {
		if attempts <= 1 {
			return base
		}
		d := base
		for i := 1; i < attempts; i++ {
			d *= 2
			if d >= max {
				return max
			}
		}
		return d
	}
}

func defaultBackoff(attempts int) time.Duration {
	return ExponentialBackoff(10*time.Millisecond, 5*time.Second)(attempts)
}

// Add enqueues a key (collapses if already dirty or processing).
func (q *Queue) Add(key string) {
	q.requests <- request{kind: reqAdd, key: key}
}

// AddAfter re-enqueues a key after delay. Used for RequeueAfter.
func (q *Queue) AddAfter(key string, delay time.Duration) {
	q.requests <- request{kind: reqAddAfter, key: key, at: time.Now().Add(delay)}
}

// Retry records a failure and schedules the key with exponential backoff.
// When maxAttempts is configured and exceeded, the key is dropped (dead letter).
func (q *Queue) Retry(item Item) {
	q.requests <- request{kind: reqRetry, item: item}
}

// Done marks an in-flight item finished; if the key was re-added while in
// flight it is requeued exactly once.
func (q *Queue) Done(item Item) {
	q.requests <- request{kind: reqDone, item: item}
}

// Len returns the number of keys waiting (ready + scheduled, not processing).
func (q *Queue) Len() int {
	reply := make(chan int, 1)
	q.requests <- request{kind: reqLen, reply: reply}
	return <-reply
}

// Get blocks until an item is available or ctx is done.
func (q *Queue) Get(ctx context.Context) (Item, bool) {
	reply := make(chan Item)
	select {
	case q.getting <- waiter{ctx: ctx, reply: reply}:
	case <-ctx.Done():
		return Item{}, false
	}
	select {
	case it, ok := <-reply:
		return it, ok
	case <-ctx.Done():
		// The delivery goroutine may win the race and park the item in reply;
		// hand it back so it is not lost in the processing set.
		select {
		case it := <-reply:
			q.giveItemBack(it)
		default:
		}
		return Item{}, false
	}
}

// giveItemBack returns an item whose handoff was lost. It is non-blocking: if
// the loop has already stopped the item is simply dropped (the queue is going
// away anyway), so a producer goroutine can never wedge.
func (q *Queue) giveItemBack(it Item) {
	r := request{kind: reqGiveBack, item: it}
	select {
	case q.giveback <- r:
	case <-q.stopped: // loop gone; safe to drop
	}
}

// Shutdown stops the queue; blocked Get calls return with ok=false.
func (q *Queue) Shutdown() {
	q.requests <- request{kind: reqShutdown}
}

// delayedItem is an entry in the delay heap.
type delayedItem struct {
	item Item
	at   time.Time
}

type delayHeap []delayedItem

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)        { *h = append(*h, x.(delayedItem)) }
func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// loop is the single goroutine owning all queue bookkeeping.
func (q *Queue) loop() {
	var (
		ready      []Item              // FIFO of keys ready to deliver
		waiters    []waiter            // blocked Get calls
		dirty      = map[string]bool{} // queued or scheduled
		processing = map[string]Item{} // currently with a worker
		scheduled  delayHeap
		wakeup     *time.Timer
		wakeCh     <-chan time.Time
	)
	heap.Init(&scheduled)

	resetWake := func() {
		if wakeup != nil {
			wakeup.Stop()
			wakeup = nil
			wakeCh = nil
		}
		if scheduled.Len() > 0 {
			wakeup = time.NewTimer(time.Until(scheduled[0].at))
			wakeCh = wakeup.C
		}
	}

	// deliver hands ready heads to registered waiters. The handoff races the
	// waiter's ctx in a short-lived goroutine; a lost race returns the item via
	// reqGiveBack, so the loop can never wedge on a departed receiver.
	deliver := func() {
		for len(ready) > 0 && len(waiters) > 0 {
			it := ready[0]
			ready = ready[1:]
			w := waiters[0]
			waiters = waiters[1:]
			delete(dirty, it.Key)
			processing[it.Key] = it
			go func() {
				select {
				case w.reply <- it:
				case <-w.ctx.Done():
					q.giveItemBack(it)
				case <-q.stopped:
					q.giveItemBack(it)
				}
			}()
		}
	}

	enqueueReady := func(key string, attempts int) {
		// Key stays in dirty until actually handed to a waiter.
		ready = append(ready, Item{Key: key, EnqueuedAt: time.Now(), Attempts: attempts})
	}

	for {
		select {
		case w := <-q.getting:
			waiters = append(waiters, w)
			deliver()

		case r := <-q.giveback:
			// Handoff lost (consumer gave up / shutdown): undo processing.
			if cur, busy := processing[r.item.Key]; busy && cur == r.item {
				delete(processing, r.item.Key)
				dirty[r.item.Key] = true
				ready = append([]Item{r.item}, ready...)
			}

		case now := <-wakeCh:
			for scheduled.Len() > 0 && !scheduled[0].at.After(now) {
				d := heap.Pop(&scheduled).(delayedItem)
				if dirty[d.item.Key] {
					enqueueReady(d.item.Key, d.item.Attempts)
				}
			}
			resetWake()
			deliver()

		case r := <-q.requests:
			switch r.kind {
			case reqAdd, reqAddAfter:
				if _, busy := processing[r.key]; busy {
					// Collapse: remember that a newer version exists; it is
					// delivered exactly once after Done.
					dirty[r.key] = true
					break
				}
				if !dirty[r.key] {
					dirty[r.key] = true
					if r.kind == reqAdd {
						enqueueReady(r.key, 0)
					} else {
						heap.Push(&scheduled, delayedItem{
							item: Item{Key: r.key, EnqueuedAt: time.Now()},
							at:   r.at,
						})
						resetWake()
					}
				}

			case reqRetry:
				it := r.item
				delete(processing, it.Key)
				attempts := it.Attempts + 1
				switch {
				case q.maxAttempts > 0 && attempts > q.maxAttempts:
					// Dead letter: drop. A concurrent fresh update still wins.
					if dirty[it.Key] {
						enqueueReady(it.Key, 0)
					}
				case dirty[it.Key]:
					// A fresh update arrived during processing; serve it now.
					enqueueReady(it.Key, 0)
				default:
					dirty[it.Key] = true
					it.Attempts = attempts
					heap.Push(&scheduled, delayedItem{item: it, at: time.Now().Add(q.delay(attempts))})
					resetWake()
				}

			case reqDone:
				cur, busy := processing[r.item.Key]
				if busy {
					delete(processing, r.item.Key)
					if dirty[r.item.Key] {
						enqueueReady(r.item.Key, cur.Attempts)
					}
				}

			case reqLen:
				r.reply <- len(dirty)

			case reqShutdown:
				close(q.stopped)
				for _, w := range waiters {
					close(w.reply)
				}
				return
			}
			deliver()
		}
	}
}
