package admissionadvisor

import (
	"context"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

type ProtectedWorkloadPlan struct {
	ProtectedWorkload    workload.Reference
	ProtectedFlavors     []kueue.ResourceFlavorReference
	ProtectedPriority    int32
	ProtectedWorkerCount int32
	ProtectedTotalGPU    int64
}

func (p ProtectedWorkloadPlan) Enabled() bool {
	return p.ProtectedWorkload != "" && len(p.ProtectedFlavors) > 0
}

func (p ProtectedWorkloadPlan) ProtectsFlavor(flavor kueue.ResourceFlavorReference) bool {
	for _, candidate := range p.ProtectedFlavors {
		if candidate == flavor {
			return true
		}
	}
	return false
}

type Advice struct {
	WorkloadScores map[workload.Reference]float64
	FlavorRankings map[workload.Reference][]kueue.ResourceFlavorReference
	GuardPlan      ProtectedWorkloadPlan
	Source         string
}

type Client interface {
	Enabled() bool
	AdviseAdmission(ctx context.Context, workloads []workload.Info, snapshot *schdcache.Snapshot) (Advice, error)
}

type noopClient struct{}

func NewNoopClient() Client {
	return noopClient{}
}

func (noopClient) Enabled() bool {
	return false
}

func (noopClient) AdviseAdmission(_ context.Context, _ []workload.Info, _ *schdcache.Snapshot) (Advice, error) {
	return Advice{}, nil
}
