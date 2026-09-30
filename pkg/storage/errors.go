package storage

import (
	"errors"
	"fmt"

	"github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"
)

// ErrNotFound means the object is not present in the store (including objects
// that were physically removed after finalizer completion).
var ErrNotFound = errors.New("object not found")

// ErrAlreadyExists means Create targeted an existing key.
var ErrAlreadyExists = errors.New("object already exists")

// ConflictError indicates an optimistic concurrency failure: the supplied
// resourceVersion no longer matches the stored one. Callers must re-read the
// latest state and re-decide rather than overwrite blindly.
type ConflictError struct {
	Key     api.Key
	Wanted  api.ResourceVersion
	Current api.ResourceVersion
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("optimistic concurrency conflict on %s: wanted rv=%d, current rv=%d",
		e.Key, e.Wanted, e.Current)
}

// IsConflict reports whether err is a *ConflictError.
func IsConflict(err error) bool {
	var c *ConflictError
	return errors.As(err, &c)
}
