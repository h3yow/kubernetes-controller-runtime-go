package leaderelection

import "github.com/h3yow/kubernetes-controller-runtime-go/pkg/api"

// KindLease is the store kind used for leader election leases.
const KindLease = "Lease"

// Lease is the stored leader-election record.
type Lease struct {
	api.ObjectMeta
	HolderIdentity string
	AcquireTime    int64
	RenewTime      int64
	LeaseDuration  int64
}

func (l *Lease) Kind() string             { return KindLease }
func (l *Lease) GetMeta() *api.ObjectMeta { return &l.ObjectMeta }
func (l *Lease) DeepCopy() api.Object {
	cp := *l
	cp.Finalizers = append([]string(nil), l.Finalizers...)
	return &cp
}
