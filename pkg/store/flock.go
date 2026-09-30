package store

import (
	"errors"
	"syscall"
	"time"
)

// errLockTimeout means the advisory lock could not be acquired in time.
var errLockTimeout = errors.New("timed out acquiring state lock")

// flockBlock acquires/releases an advisory flock. For acquisition it retries
// EWOULDBLOCK up to a deadline so a wedged holder cannot block callers
// forever. Unlock is retried briefly. It honors no context (file locks are
// process-scoped) but bounds the wait.
func flockBlock(fd int, how int) error {
	if how&syscall.LOCK_UN != 0 {
		return syscall.Flock(fd, syscall.LOCK_UN)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := syscall.Flock(fd, how|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if err != syscall.EWOULDBLOCK {
			return err
		}
		if time.Now().After(deadline) {
			return errLockTimeout
		}
		time.Sleep(2 * time.Millisecond)
	}
}
