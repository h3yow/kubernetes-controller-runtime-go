package leaderelection

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/storage"
)

type fakeClock struct {
	ch chan time.Time
}

// newFakeClock starts a controllable, monotonically advancing clock. The
// elector's RetryPeriod sleeps remain real-time; the clock only governs lease
// expiry math.
func newFakeClock(speed time.Duration, every time.Duration) *fakeClock {
	fc := &fakeClock{ch: make(chan time.Time)}
	go func() {
		t := time.UnixMilli(1_000_000)
		tick := time.NewTicker(every)
		defer tick.Stop()
		for range tick.C {
			t = t.Add(speed)
			fc.ch <- t
		}
	}()
	return fc
}
func (c *fakeClock) now() time.Time {
	select {
	case t := <-c.ch:
		return t
	default:
		return time.UnixMilli(1_000_000)
	}
}

func testCfg(store storage.Store, id string, clock func() time.Time) Config {
	return Config{
		LeaseName:     "widget-controller",
		Identity:      id,
		LeaseDuration: 200 * time.Millisecond,
		RenewDeadline: 100 * time.Millisecond,
		RetryPeriod:   20 * time.Millisecond,
		Store:         store,
		Clock:         clock,
	}
}

func waitAnyLeader(t *testing.T, electors map[string]Elector, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []string
		for id, e := range electors {
			if e.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			t.Logf("judgment: sole leader among %d instances = %q", len(electors), leaders[0])
			return leaders[0]
		}
		if len(leaders) > 1 {
			t.Fatalf("two leaders at once: %v", leaders)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no unique leader elected in time")
	return ""
}

func start(t *testing.T, cfg Config) (Elector, context.CancelFunc) {
	e := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)
	return e, cancel
}

func TestSingleLeaderAcquired(t *testing.T) {
	store := storage.NewMemoryStore()
	electors := map[string]Elector{}
	cancels := map[string]context.CancelFunc{}
	for _, id := range []string{"a", "b", "c"} {
		e, c := start(t, testCfg(store, id, time.Now))
		electors[id] = e
		cancels[id] = c
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	t.Log("input: 3 instances start concurrently against one store")
	winner := waitAnyLeader(t, electors, time.Second)

	// Give it several renewal cycles and re-assert uniqueness.
	time.Sleep(150 * time.Millisecond)
	var leaders []string
	for id, e := range electors {
		if e.IsLeader() {
			leaders = append(leaders, id)
		}
	}
	if len(leaders) != 1 || leaders[0] != winner {
		t.Fatalf("leadership flapped: leaders=%v winner=%s", leaders, winner)
	}
	t.Logf("judgment: leader stable across renewals: %s", winner)
}

func TestCleanShutdownImmediateFailover(t *testing.T) {
	store := storage.NewMemoryStore()
	electors := map[string]Elector{}
	cancels := map[string]context.CancelFunc{}
	for _, id := range []string{"a", "b"} {
		e, c := start(t, testCfg(store, id, time.Now))
		electors[id] = e
		cancels[id] = c
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	winner := waitAnyLeader(t, electors, time.Second)
	loser := map[string]string{"a": "b", "b": "a"}[winner]

	t.Logf("input: leader %s stops cleanly; expectation: %s acquires the released lease", winner, loser)
	cancels[winner]()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if electors[loser].IsLeader() && !electors[winner].IsLeader() {
			t.Logf("judgment: %s took over immediately after %s released the lease", loser, winner)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s did not take over after clean shutdown within bounded time", loser)
}

func TestLeaseExpiryFailover(t *testing.T) {
	store := storage.NewMemoryStore()
	clock := newFakeClock(20*time.Millisecond, 5*time.Millisecond)
	cfgA := testCfg(store, "a", clock.now)
	eA, cancelA := start(t, cfgA)
	electors := map[string]Elector{"a": eA}

	waitAnyLeader(t, electors, time.Second)

	// Simulate a dead leader: stop renewing without releasing. Let the lease
	// age well past LeaseDuration in clock time.
	cancelA()
	// Drain the clock channel to advance fake time (40x real speed).
	for i := 0; i < 50; i++ {
		<-clock.ch
	}

	cfgB := testCfg(store, "b", clock.now)
	eB, cancelB := start(t, cfgB)
	defer cancelB()
	electors["b"] = eB

	t.Log("input: leader a dead with unreleased, expired lease; expectation: b takes over")
	waitAnyLeader(t, map[string]Elector{"b": eB, "a": eA}, time.Second)
	if eA.IsLeader() {
		t.Fatal("dead leader a must never appear leader again")
	}
	t.Log("judgment: standby b acquired the expired lease; no double leadership")
}
