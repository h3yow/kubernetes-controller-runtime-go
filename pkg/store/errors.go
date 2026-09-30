package store

import "errors"

var (
	// ErrNotFound is returned when an object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrConflict is returned when a write carries a stale resource version.
	ErrConflict = errors.New("resource version conflict")
	// ErrFence is returned when a write carries a stale/empty fencing token.
	ErrFence = errors.New("fencing token rejected")
	// ErrHasFinalizers is returned when deleting an object still carrying finalizers.
	ErrHasFinalizers = errors.New("object has finalizers")
)
