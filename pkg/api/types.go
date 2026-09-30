package api

import "time"

// ResourceVersion is the optimistic concurrency token of a stored object.
type ResourceVersion int64

// ObjectMeta mirrors the subset of Kubernetes object metadata needed here.
type ObjectMeta struct {
	Name              string
	ResourceVersion   ResourceVersion
	Finalizers        []string
	DeletionTimestamp *time.Time
}

// Object is implemented by every resource type stored in the local store.
type Object interface {
	GetMeta() *ObjectMeta
	Kind() string
	DeepCopy() Object
}

// Key identifies an object within the cluster (kind + name).
type Key struct {
	Kind string
	Name string
}

// String returns "kind/name".
func (k Key) String() string {
	return k.Kind + "/" + k.Name
}

// EventType marks what happened to an object.
type EventType string

const (
	EventAdded   EventType = "ADDED"
	EventUpdated EventType = "UPDATED"
	EventDeleted EventType = "DELETED"
)

// Event is a single store change delivered to a watcher.
type Event struct {
	Type   EventType
	Object Object
	Key    Key
}

// Result is what a reconciler returns. FailureReasons produced here are the
// machine-distinguishable reason strings printed in logs and asserted in tests.
type Result struct {
	Requeue       bool
	RequeueAfter  time.Duration
	FailureReason string
}

const (
	ReasonRequeue        = "RequeueRequested"
	ReasonTransientError = "TransientError"
)
