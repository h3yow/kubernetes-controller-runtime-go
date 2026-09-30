package reconciler

import (
	"errors"
	"sync"
	"time"
)

// ErrCleanupFailed is the distinguishable error returned when external cleanup
// is deliberately failing (used to exercise finalizer retry).
var ErrCleanupFailed = errors.New("external cleanup failed")

// externalResource is the simulated cloud-side resource owned by a Widget.
type externalResource struct {
	createdAt time.Time
}

type externalState struct {
	mu        sync.Mutex
	resources map[string]*externalResource
	failNext  map[string]bool // names whose cleanup must fail until cleared
	attempts  map[string]int  // cleanup attempts per name (asserted in tests)
}

// ExternalSystem is the simulation of an external (non-store) world. It is
// safe for concurrent use.
type ExternalSystem struct {
	state *externalState
}

// NewExternalSystem returns an empty simulated external system.
func NewExternalSystem() *ExternalSystem {
	return &ExternalSystem{state: &externalState{
		resources: map[string]*externalResource{},
		failNext:  map[string]bool{},
		attempts:  map[string]int{},
	}}
}

// Ensure creates the external resource if missing.
func (e *ExternalSystem) Ensure(name string) error {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	if _, ok := e.state.resources[name]; !ok {
		e.state.resources[name] = &externalResource{createdAt: time.Now()}
	}
	return nil
}

// Cleanup removes the external resource. When SetCleanupFailure(name,true) is
// active it returns ErrCleanupFailed and leaves the resource in place, so the
// finalizer must remain and cleanup can be attempted again.
func (e *ExternalSystem) Cleanup(name string) error {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	e.state.attempts[name]++
	if e.state.failNext[name] {
		return ErrCleanupFailed
	}
	delete(e.state.resources, name)
	return nil
}

// Exists reports whether the external resource is present.
func (e *ExternalSystem) Exists(name string) bool {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	_, ok := e.state.resources[name]
	return ok
}

// SetCleanupFailure toggles forced cleanup failure for one widget.
func (e *ExternalSystem) SetCleanupFailure(name string, fail bool) {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	if fail {
		e.state.failNext[name] = true
	} else {
		delete(e.state.failNext, name)
	}
}

// CleanupAttempts reports how many cleanups were attempted for name.
func (e *ExternalSystem) CleanupAttempts(name string) int {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	return e.state.attempts[name]
}
