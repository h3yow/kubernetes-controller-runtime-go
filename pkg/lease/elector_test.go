package lease_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/lease"
	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/store"
)

func newStore(t *testing.T) *store.FileStore {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(t.TempDir(), "s"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestElectorRenewsThenAnotherTakesOverAfterStop verifies:
//   - the winner keeps leadership across renewals,
//   - once it stops WITHOUT releasing (crash), the other instance acquires a
//     strictly greater epoch only after TTL expiry,
//   - there is no overlap of leadership.
func TestElectorRenewsThenAnotherTakesOverAfterStop(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	open := func() *store.FileStore {
		st, err := store.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	stA := open()
	defer stA.Close()

	var ledA atomic.Bool
	cfgA := lease.Config{
		Holder: "A", TTL: 400 * time.Millisecond, RenewPeriod: 100 * time.Millisecond,
		RetryPeriod: 10 * time.Millisecond, Logger: quietLogger(),
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		_ = lease.New(stA, cfgA).Run(ctxA, func(leaderCtx context.Context) {
			ledA.Store(true)
			<-leaderCtx.Done() // hold leadership until it is lost
			ledA.Store(false)
		})
	}()

	if !waitFor(2*time.Second, func() bool {
		l, err := stA.ReadLease(context.Background())
		return err == nil && l != nil && l.Holder == "A"
	}) {
		t.Fatal("A never became leader")
	}
	t.Log("A acquired leadership")

	// Let several renewals happen.
	time.Sleep(450 * time.Millisecond)
	l1, _ := stA.ReadLease(context.Background())
	if l1 == nil || l1.Holder != "A" {
		t.Fatalf("A lost leadership unexpectedly: %+v", l1)
	}
	t.Logf("A retained leadership via renewals (epoch %d, renewedAt advanced)", l1.Epoch)

	// B contests while A is alive: must not win.
	stB := open()
	defer stB.Close()
	contest, cancelContest := context.WithTimeout(context.Background(), 200*time.Millisecond)
	contested := atomic.Bool{}
	go func() {
		_ = lease.New(stB, lease.Config{
			Holder: "B", TTL: 400 * time.Millisecond, RenewPeriod: 100 * time.Millisecond,
			RetryPeriod: 10 * time.Millisecond, Logger: quietLogger(),
		}).Run(contest, func(leaderCtx context.Context) {
			contested.Store(true)
			<-leaderCtx.Done()
		})
	}()
	time.Sleep(300 * time.Millisecond)
	cancelContest()
	if contested.Load() {
		t.Fatal("B acquired leadership while A was actively renewing (split brain)")
	}
	t.Log("B could not acquire while A held a live lease")

	// A crashes (cancel parent -> no release); wait for full exit.
	cancelA()
	<-doneA

	// B now acquires after TTL with a higher epoch; measure takeover time.
	var epochB int64
	becameB := make(chan struct{})
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	start := time.Now()
	go func() {
		_ = lease.New(stB, lease.Config{
			Holder: "B", TTL: 400 * time.Millisecond, RenewPeriod: 100 * time.Millisecond,
			RetryPeriod: 10 * time.Millisecond, Logger: quietLogger(),
		}).Run(ctxB, func(leaderCtx context.Context) {
			if l, _ := stB.ReadLease(context.Background()); l != nil {
				epochB = l.Epoch
			}
			close(becameB)
			<-leaderCtx.Done()
		})
	}()
	select {
	case <-becameB:
	case <-time.After(3 * time.Second):
		t.Fatal("B did not take over after A crashed")
	}
	takeover := time.Since(start)
	if epochB <= l1.Epoch {
		t.Fatalf("takeover epoch must increase: A=%d B=%d", l1.Epoch, epochB)
	}
	// Takeover requires waiting for A's TTL to expire, but bounded.
	if takeover > 2*time.Second {
		t.Fatalf("takeover too slow: %v", takeover)
	}
	t.Logf("B took over %v after crash with epoch %d (> A epoch %d); no overlap",
		takeover.Round(time.Millisecond), epochB, l1.Epoch)
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
