package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// ErrLeaseHeld means acquisition was attempted while another live holder owns
// the lease.
var ErrLeaseHeld = errors.New("lease already held")

const (
	stateFile = "state.json"
	lockFile  = "state.lock"
)

// snapshot is the on-disk representation of all shared state.
type snapshot struct {
	Objects        map[string]*Object `json:"objects"`
	Lease          *LeaseState        `json:"lease,omitempty"`
	EpochCounter   int64              `json:"epochCounter"`
	VersionCounter int64              `json:"versionCounter"`
}

// FileStore is a shared-state implementation backed by a JSON snapshot file on
// disk. A POSIX advisory lock serializes different runtime processes, and an
// in-process mutex serializes goroutines; every write is validated against the
// live lease fencing token.
type FileStore struct {
	mu  sync.Mutex
	dir string
	fd  *os.File
	now func() time.Time
}

// NewFileStore opens (creating if needed) the shared state under dir.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	fd, err := os.OpenFile(filepath.Join(dir, lockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	s := &FileStore{dir: dir, fd: fd, now: func() time.Time { return time.Now() }}
	if err := s.withLock(func(sn *snapshot) error { return nil }); err != nil {
		_ = fd.Close()
		return nil, err
	}
	return s, nil
}

// Close releases OS resources.
func (s *FileStore) Close() error { return s.fd.Close() }

// SetClock overrides the wall clock (test helper).
func (s *FileStore) SetClock(now func() time.Time) { s.now = now }

func (s *FileStore) millis() int64 { return s.now().UTC().UnixMilli() }

// withLock runs fn while holding the cross-process lock and a fresh snapshot.
func (s *FileStore) withLock(fn func(sn *snapshot) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Keep the *os.File alive across the syscall: Fd() hands the runtime fd to
	// us but does not on its own prevent the finalizer from closing it, which
	// would surface as "bad file descriptor" under GC pressure.
	fd := int(s.fd.Fd())
	defer runtime.KeepAlive(s.fd)
	if err = flockBlock(fd, syscall.LOCK_EX); err != nil {
		return fmt.Errorf("acquire flock: %w", err)
	}
	defer func() {
		if uerr := flockBlock(fd, syscall.LOCK_UN); uerr != nil && err == nil {
			err = fmt.Errorf("release flock: %w", uerr)
		}
	}()

	sn, err := s.loadLocked()
	if err != nil {
		return err
	}
	// Always persist: some mutations return a sentinel error while still being
	// valid state changes (e.g. Delete tombstones an object then returns
	// ErrHasFinalizers). Fence/conflict paths never mutate, so this is safe.
	fnerr := fn(sn)
	if serr := s.saveLocked(sn); serr != nil {
		return serr
	}
	return fnerr
}

func (s *FileStore) loadLocked() (*snapshot, error) {
	sn := &snapshot{Objects: map[string]*Object{}}
	b, err := os.ReadFile(filepath.Join(s.dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return sn, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	if len(b) == 0 {
		return sn, nil
	}
	if err := json.Unmarshal(b, sn); err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}
	if sn.Objects == nil {
		sn.Objects = map[string]*Object{}
	}
	return sn, nil
}

func (s *FileStore) saveLocked(sn *snapshot) error {
	b, err := json.MarshalIndent(sn, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := filepath.Join(s.dir, stateFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, stateFile)); err != nil {
		return fmt.Errorf("commit state: %w", err)
	}
	return nil
}

func keyOf(namespace, name string) string { return namespace + "/" + name }

func clone(o *Object) *Object {
	if o == nil {
		return nil
	}
	cp := *o
	if o.Finalizers != nil {
		cp.Finalizers = append([]string(nil), o.Finalizers...)
	}
	cp.Labels = cloneMap(o.Labels)
	cp.Spec = cloneAnyMap(o.Spec)
	cp.Status = cloneAnyMap(o.Status)
	return &cp
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		// Fall back to a shallow copy; values are expected to be JSON-able.
		out := make(map[string]any, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return m
	}
	return out
}

// checkFence verifies the write token against the live lease.
func (s *FileStore) checkFence(sn *snapshot, f Fence) error {
	if !f.valid() {
		return ErrFence
	}
	l := sn.Lease
	if l == nil || l.Holder != f.Holder || l.Epoch != f.Epoch || l.Expired(s.millis()) {
		return ErrFence
	}
	return nil
}

func (s *FileStore) nextVersion(sn *snapshot) string {
	sn.VersionCounter++
	return fmt.Sprintf("%d", sn.VersionCounter)
}

// Reset empties all state (test helper); also clears the lease.
func (s *FileStore) Reset(ctx context.Context) error {
	return s.withLock(func(sn *snapshot) error {
		sn.Objects = map[string]*Object{}
		sn.Lease = nil
		sn.EpochCounter = 0
		sn.VersionCounter = 0
		return nil
	})
}

func (s *FileStore) Get(ctx context.Context, namespace, name string) (*Object, error) {
	var out *Object
	err := s.withLock(func(sn *snapshot) error {
		if o, ok := sn.Objects[keyOf(namespace, name)]; ok {
			out = clone(o)
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *FileStore) List(ctx context.Context) ([]*Object, error) {
	var out []*Object
	err := s.withLock(func(sn *snapshot) error {
		for _, o := range sn.Objects {
			out = append(out, clone(o))
		}
		return nil
	})
	return out, err
}

func (s *FileStore) Create(ctx context.Context, f Fence, obj *Object) (*Object, error) {
	var out *Object
	err := s.withLock(func(sn *snapshot) error {
		if err := s.checkFence(sn, f); err != nil {
			return err
		}
		k := keyOf(obj.Namespace, obj.Name)
		if _, exists := sn.Objects[k]; exists {
			return ErrConflict
		}
		in := clone(obj)
		in.ResourceVersion = s.nextVersion(sn)
		sn.Objects[k] = in
		out = clone(in)
		return nil
	})
	return out, err
}

func (s *FileStore) Update(ctx context.Context, f Fence, obj *Object) (*Object, error) {
	var out *Object
	err := s.withLock(func(sn *snapshot) error {
		if err := s.checkFence(sn, f); err != nil {
			return err
		}
		k := keyOf(obj.Namespace, obj.Name)
		live, ok := sn.Objects[k]
		if !ok {
			return ErrNotFound
		}
		if obj.ResourceVersion != live.ResourceVersion {
			return ErrConflict
		}
		in := clone(obj)
		in.ResourceVersion = s.nextVersion(sn)
		sn.Objects[k] = in
		out = clone(in)
		return nil
	})
	return out, err
}

func (s *FileStore) Delete(ctx context.Context, f Fence, namespace, name string) error {
	return s.withLock(func(sn *snapshot) error {
		if err := s.checkFence(sn, f); err != nil {
			return err
		}
		k := keyOf(namespace, name)
		live, ok := sn.Objects[k]
		if !ok {
			return ErrNotFound
		}
		if len(live.Finalizers) > 0 {
			// Tombstone: keep the object so finalizers can run.
			if live.DeletionTimestamp == nil {
				ts := s.millis()
				live.DeletionTimestamp = &ts
				live.ResourceVersion = s.nextVersion(sn)
			}
			return ErrHasFinalizers
		}
		delete(sn.Objects, k)
		return nil
	})
}

func (s *FileStore) ForceDelete(ctx context.Context, namespace, name string) error {
	return s.withLock(func(sn *snapshot) error {
		delete(sn.Objects, keyOf(namespace, name))
		return nil
	})
}

func (s *FileStore) AcquireLease(ctx context.Context, holder string, ttlMillis int64) (*LeaseState, error) {
	var out *LeaseState
	err := s.withLock(func(sn *snapshot) error {
		now := s.millis()
		l := sn.Lease
		if l != nil && !l.Expired(now) {
			if l.Holder == holder {
				out = &*l
				return nil
			}
			return ErrLeaseHeld
		}
		sn.EpochCounter++
		l = &LeaseState{
			Holder:     holder,
			Epoch:      sn.EpochCounter,
			AcquiredAt: now,
			RenewedAt:  now,
			ExpiresAt:  now + ttlMillis,
		}
		sn.Lease = l
		out = &*l
		return nil
	})
	return out, err
}

func (s *FileStore) RenewLease(ctx context.Context, holder string, epoch int64, ttlMillis int64) (*LeaseState, error) {
	var out *LeaseState
	err := s.withLock(func(sn *snapshot) error {
		l := sn.Lease
		if l == nil || l.Holder != holder || l.Epoch != epoch {
			return ErrLeaseHeld
		}
		now := s.millis()
		l.RenewedAt = now
		l.ExpiresAt = now + ttlMillis
		sn.Lease = l
		out = &*l
		return nil
	})
	return out, err
}

func (s *FileStore) ReleaseLease(ctx context.Context, holder string, epoch int64) error {
	return s.withLock(func(sn *snapshot) error {
		l := sn.Lease
		if l != nil && l.Holder == holder && l.Epoch == epoch {
			sn.Lease = nil
		}
		return nil
	})
}

func (s *FileStore) ReadLease(ctx context.Context) (*LeaseState, error) {
	var out *LeaseState
	err := s.withLock(func(sn *snapshot) error {
		if sn.Lease != nil && !sn.Lease.Expired(s.millis()) {
			l := *sn.Lease
			out = &l
		}
		return nil
	})
	return out, err
}

// AdminCreate upserts an object bypassing the fence, simulating an external
// control-plane write (e.g. kubectl). An existing object gets a fresh, higher
// resource version; a new one starts at 1. Test/demo helper only.
func (s *FileStore) AdminCreate(ctx context.Context, obj *Object) (*Object, error) {
	var out *Object
	err := s.withLock(func(sn *snapshot) error {
		in := clone(obj)
		in.ResourceVersion = s.nextVersion(sn)
		sn.Objects[keyOf(in.Namespace, in.Name)] = in
		out = clone(in)
		return nil
	})
	return out, err
}

// AdminDelete hard-deletes bypassing the fence/finalizers. Test helper.
func (s *FileStore) AdminDelete(ctx context.Context, namespace, name string) error {
	return s.withLock(func(sn *snapshot) error {
		delete(sn.Objects, keyOf(namespace, name))
		return nil
	})
}
