package storage

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
)

// watchBufferSize bounds how many events a slow watcher may buffer before
// writes block. Tests consume watch channels promptly.
const watchBufferSize = 1024

type watcher struct {
	kind string
	ch   chan api.Event
}

type memoryStore struct {
	mu       sync.RWMutex
	objects  map[api.Key]api.Object
	rv       api.ResourceVersion
	watchers map[*watcher]struct{}
}

func (s *memoryStore) initLocked() {
	if s.objects == nil {
		s.objects = map[api.Key]api.Object{}
		s.watchers = map[*watcher]struct{}{}
	}
}

func (s *memoryStore) Create(ctx context.Context, obj api.Object) (api.Object, error) {
	meta := obj.GetMeta()
	key := api.Key{Kind: obj.Kind(), Name: meta.Name}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()

	if _, ok := s.objects[key]; ok {
		return nil, ErrAlreadyExists
	}

	stored := obj.DeepCopy()
	s.rv++
	stored.GetMeta().ResourceVersion = s.rv
	s.objects[key] = stored

	s.publishLocked(api.Event{Type: api.EventAdded, Key: key, Object: stored.DeepCopy()})
	return stored.DeepCopy(), nil
}

func (s *memoryStore) Update(ctx context.Context, obj api.Object) (api.Object, error) {
	meta := obj.GetMeta()
	key := api.Key{Kind: obj.Kind(), Name: meta.Name}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()

	cur, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	curRV := cur.GetMeta().ResourceVersion
	if meta.ResourceVersion != curRV {
		// Stale write: refuse so the caller must re-read and re-decide.
		return nil, &ConflictError{Key: key, Wanted: meta.ResourceVersion, Current: curRV}
	}

	stored := obj.DeepCopy()
	s.rv++
	stored.GetMeta().ResourceVersion = s.rv

	// Kubernetes semantics: once an object carries a deletionTimestamp, the
	// update that removes its last finalizer physically removes it. This keeps
	// cleanup finalizers authoritative: nothing is removed while one remains,
	// and no extra Delete call is needed after the last one succeeds.
	m2 := stored.GetMeta()
	if m2.DeletionTimestamp != nil && len(m2.Finalizers) == 0 {
		delete(s.objects, key)
		s.publishLocked(api.Event{Type: api.EventDeleted, Key: key, Object: stored.DeepCopy()})
		// Return rv-bearing copy; it is no longer readable via Get.
		return stored.DeepCopy(), nil
	}

	s.objects[key] = stored
	s.publishLocked(api.Event{Type: api.EventUpdated, Key: key, Object: stored.DeepCopy()})
	return stored.DeepCopy(), nil
}

func (s *memoryStore) Delete(ctx context.Context, kind, name string) error {
	key := api.Key{Kind: kind, Name: name}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()

	cur, ok := s.objects[key]
	if !ok {
		// Deletion of a gone object is idempotent: no work, no new reconcile.
		return nil
	}
	m := cur.GetMeta()
	if m.DeletionTimestamp != nil {
		// Already marked deleting; finalizer removal drives the physical delete.
		return nil
	}

	if len(m.Finalizers) == 0 {
		// Nothing to clean up: physical delete immediately.
		delete(s.objects, key)
		s.publishLocked(api.Event{Type: api.EventDeleted, Key: key, Object: cur.DeepCopy()})
		return nil
	}

	now := time.Now().UTC()
	m.DeletionTimestamp = &now
	s.rv++
	m.ResourceVersion = s.rv
	// Object stays in the store with a deletion timestamp until every
	// finalizer has performed its cleanup and removed itself.
	s.publishLocked(api.Event{Type: api.EventUpdated, Key: key, Object: cur.DeepCopy()})
	return nil
}

func (s *memoryStore) Get(ctx context.Context, kind, name string) (api.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[api.Key{Kind: kind, Name: name}]
	if !ok {
		return nil, ErrNotFound
	}
	return obj.DeepCopy(), nil
}

func (s *memoryStore) List(ctx context.Context, kind string) ([]api.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]api.Object, 0, len(s.objects))
	for k, obj := range s.objects {
		if kind != "" && k.Kind != kind {
			continue
		}
		out = append(out, obj.DeepCopy())
	}
	return out, nil
}

// Note on the list/watch boundary: the snapshot and the watcher registration
// happen under the same write lock, and every mutation publishes while holding
// that lock. A watcher therefore never misses an event newer than its snapshot
// nor receives one older than it.
func (s *memoryStore) Subscribe(ctx context.Context, kind string) ([]api.Object, <-chan api.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()

	snapshot := make([]api.Object, 0, len(s.objects))
	for k, obj := range s.objects {
		if kind != "" && k.Kind != kind {
			continue
		}
		snapshot = append(snapshot, obj.DeepCopy())
	}

	w := &watcher{kind: kind, ch: make(chan api.Event, watchBufferSize)}
	s.watchers[w] = struct{}{}

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.watchers, w)
		s.mu.Unlock()
		// Drain is unnecessary for in-process consumers; closing signals end.
		// (Not closing ch avoids spurious zero-value events for consumers that
		// keep selecting; the watcher context cancellation is the signal.)
	}()

	return snapshot, w.ch, nil
}

func (s *memoryStore) CurrentResourceVersion() api.ResourceVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rv
}

// publishLocked fans an event out to matching watchers. Called with mu held.
func (s *memoryStore) publishLocked(ev api.Event) {
	for w := range s.watchers {
		if w.kind != "" && w.kind != ev.Object.Kind() {
			continue
		}
		// Buffered channel; a full buffer means a stalled watcher, which must
		// apply backpressure rather than lose events.
		w.ch <- ev
	}
}
