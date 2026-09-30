package store

// Object is a Kubernetes-style resource object with version-based optimistic
// concurrency control and finalizers.
type Object struct {
	Kind              string            `json:"kind"`
	Namespace         string            `json:"namespace"`
	Name              string            `json:"name"`
	ResourceVersion   string            `json:"resourceVersion"`
	Finalizers        []string          `json:"finalizers,omitempty"`
	DeletionTimestamp *int64            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Spec              map[string]any    `json:"spec"`
	Status            map[string]any    `json:"status"`
}

// Key returns the stable identity of an object: namespace/name.
func (o *Object) Key() string {
	return o.Namespace + "/" + o.Name
}

// BeingDeleted reports whether the object has been tombstoned.
func (o *Object) BeingDeleted() bool {
	return o.DeletionTimestamp != nil
}

// Fence is the lease fencing token attached to every write. The store only
// accepts writes whose (Holder, Epoch) matches the live lease, so a stale
// leader that failed to renew can never write again.
type Fence struct {
	Holder string `json:"holder"`
	Epoch  int64  `json:"epoch"`
}

func (f Fence) valid() bool { return f.Holder != "" && f.Epoch > 0 }

// LeaseState records the current leader lease in shared state.
type LeaseState struct {
	Holder     string `json:"holder"`
	Epoch      int64  `json:"epoch"`
	AcquiredAt int64  `json:"acquiredAt"`
	RenewedAt  int64  `json:"renewedAt"`
	ExpiresAt  int64  `json:"expiresAt"`
}

// Expired reports whether the lease is past its deadline at nowMillis.
func (l *LeaseState) Expired(nowMillis int64) bool {
	return l == nil || nowMillis >= l.ExpiresAt
}
