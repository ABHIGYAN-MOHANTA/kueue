package admissionadvisor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/resources"
	utilpriority "sigs.k8s.io/kueue/pkg/util/priority"
	"sigs.k8s.io/kueue/pkg/workload"
)

const (
	defaultAdvisorURL = "http://127.0.0.1:5050"
	gpuResourceName   = corev1.ResourceName("admirl.ai/gpu")
)

type candidateAction struct {
	ActionID               string  `json:"action_id"`
	WorkloadID             string  `json:"workload_id"`
	WorkloadProfile        string  `json:"workload_profile"`
	FlavorName             string  `json:"flavor_name"`
	QueueName              string  `json:"queue_name"`
	ClusterQueue           string  `json:"cluster_queue"`
	FairshareGroup         string  `json:"fairshare_group"`
	Priority               int32   `json:"priority"`
	WaitSeconds            float64 `json:"wait_seconds"`
	RuntimeSeconds         float64 `json:"runtime_seconds"`
	WorkerCount            int32   `json:"worker_count"`
	TotalGPU               int64   `json:"total_gpu"`
	PerWorkerGPU           int64   `json:"per_worker_gpu"`
	TopologyAware          bool    `json:"topology_aware"`
	TopologyPreference     string  `json:"topology_preference"`
	FlavorDomain           string  `json:"flavor_domain"`
	ImmediateFit           bool    `json:"immediate_fit"`
	Provisionable          bool    `json:"provisionable"`
	AvailableGPU           int64   `json:"available_gpu"`
	TotalGPUCapacity       int64   `json:"total_gpu_capacity"`
	FairshareDebt          float64 `json:"fairshare_debt"`
	RequeueCount           int     `json:"requeue_count"`
	QueueClass             string  `json:"queue_class"`
	FlavorGPUSize          int64   `json:"flavor_gpu_size"`
	OversizeGPU            int64   `json:"oversize_gpu"`
	CompetingOlderPressure float64 `json:"competing_older_pressure"`
	ElasticEnabled         bool    `json:"elastic_enabled"`
	MinWorkerCount         int32   `json:"min_worker_count"`
	PreferredWorkerCount   int32   `json:"preferred_worker_count"`
	MaxWorkerCount         int32   `json:"max_worker_count"`
	ScaleTag               string  `json:"scale_tag"`
	ScaleFraction          float64 `json:"scale_fraction"`
}

type admissionRequest struct {
	RequestMode string            `json:"request_mode"`
	Time        float64           `json:"time"`
	Candidates  []candidateAction `json:"candidates"`
}

type admissionResponse struct {
	RankedWorkloads      []string            `json:"ranked_workloads"`
	WorkloadScores       map[string]float64  `json:"workload_scores"`
	FlavorRankings       map[string][]string `json:"flavor_rankings"`
	PairScores           map[string]float64  `json:"pair_scores"`
	ProtectedWorkload    string              `json:"protected_workload"`
	ProtectedFlavors     []string            `json:"protected_flavors"`
	ProtectedPriority    int32               `json:"protected_priority"`
	ProtectedWorkerCount int32               `json:"protected_worker_count"`
	ProtectedTotalGPU    int64               `json:"protected_total_gpu"`
	Source               string              `json:"source"`
}

type httpClient struct{}

func NewHTTPClientFromEnv() Client {
	return httpClient{}
}

func EnabledFromEnv() bool {
	value := strings.TrimSpace(strings.ToLower(firstNonEmptyEnv(
		"KUEUE_ADMISSION_ADVISOR_ENABLE",
		"ADMIRL_KUEUE_ENABLE",
	)))
	return value == "1" || value == "true" || value == "yes"
}

func (httpClient) Enabled() bool {
	return EnabledFromEnv()
}

func advisorURLFromEnv() string {
	value := strings.TrimSpace(firstNonEmptyEnv(
		"KUEUE_ADMISSION_ADVISOR_URL",
		"ADMIRL_KUEUE_MODEL_SERVER_URL",
	))
	if value == "" {
		return defaultAdvisorURL
	}
	return strings.TrimRight(value, "/")
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func runtimeSecondsForWorkload(wl *workload.Info) float64 {
	return runtimeSecondsForFlavor(wl, "")
}

func runtimeSecondsForFlavor(wl *workload.Info, flavorName kueue.ResourceFlavorReference) float64 {
	parseSeconds := func(raw string) (float64, bool) {
		value := strings.TrimSpace(raw)
		if value == "" {
			return 0, false
		}
		if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed > 0 {
			return parsed, true
		}
		if parsed, err := time.ParseDuration(value + "s"); err == nil && parsed.Seconds() > 0 {
			return parsed.Seconds(), true
		}
		return 0, false
	}
	if flavorName != "" {
		if parsed, ok := parseSeconds(workloadAnnotation(wl, "admirl.ai/scaled-runtime-"+string(flavorName))); ok {
			return parsed
		}
	}
	if parsed, ok := parseSeconds(workloadAnnotation(wl, "admirl.ai/scaled-runtime-seconds")); ok {
		return parsed
	}
	if parsed, ok := parseSeconds(workloadAnnotation(wl, "admirl.ai/runtime-seconds")); ok {
		return parsed
	}
	return 600.0
}

func workerStats(wl *workload.Info) (int32, int64, int64) {
	workerCount, totalGPU := workload.WorkerGPUStats(wl, gpuResourceName)
	var perWorkerGPU int64
	if wl == nil {
		return workerCount, totalGPU, perWorkerGPU
	}
	for _, podSet := range wl.TotalRequests {
		perPodGPU := podSet.SinglePodRequests()[gpuResourceName]
		if perPodGPU > perWorkerGPU {
			perWorkerGPU = perPodGPU
		}
	}
	return workerCount, totalGPU, perWorkerGPU
}

func topologyAware(wl *workload.Info) bool {
	if wl == nil || wl.Obj == nil {
		return false
	}
	for _, podSet := range wl.Obj.Spec.PodSets {
		if podSet.TopologyRequest != nil {
			return true
		}
	}
	return false
}

func queueClass(workerCount int32) string {
	if workerCount >= 2 {
		return "gang"
	}
	return "small"
}

func workloadAnnotation(wl *workload.Info, key string) string {
	if wl == nil || wl.Obj == nil {
		return ""
	}
	if value := strings.TrimSpace(wl.Obj.Annotations[key]); value != "" {
		return value
	}
	for _, podSet := range wl.Obj.Spec.PodSets {
		if value := strings.TrimSpace(podSet.Template.Annotations[key]); value != "" {
			return value
		}
	}
	return ""
}

func workloadProfileName(wl *workload.Info) string {
	if value := workloadAnnotation(wl, "admirl.ai/workload-name"); value != "" {
		return value
	}
	if wl != nil && wl.Obj != nil {
		return wl.Obj.Name
	}
	return ""
}

func workloadElasticBounds(wl *workload.Info, currentWorkerCount int32) (bool, int32, int32, int32) {
	elasticEnabled := strings.EqualFold(workloadAnnotation(wl, "admirl.ai/elastic-enabled"), "true")
	parseCount := func(key string, fallback int32) int32 {
		raw := workloadAnnotation(wl, key)
		if raw == "" {
			return fallback
		}
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || parsed <= 0 {
			return fallback
		}
		return int32(parsed)
	}
	minWorkers := parseCount("admirl.ai/min-worker-count", parseCount("admirl.ai/initial-worker-count", currentWorkerCount))
	preferredWorkers := parseCount("admirl.ai/preferred-worker-count", currentWorkerCount)
	maxWorkers := parseCount("admirl.ai/max-worker-count", parseCount("admirl.ai/final-worker-count", currentWorkerCount))
	if maxWorkers < preferredWorkers {
		maxWorkers = preferredWorkers
	}
	if preferredWorkers < minWorkers {
		preferredWorkers = minWorkers
	}
	if !elasticEnabled || maxWorkers <= minWorkers {
		return false, currentWorkerCount, currentWorkerCount, currentWorkerCount
	}
	return true, minWorkers, preferredWorkers, maxWorkers
}

func elasticScaleTag(currentWorkers, minWorkers, preferredWorkers, maxWorkers int32, elasticEnabled bool) string {
	if !elasticEnabled {
		return "fixed"
	}
	switch {
	case currentWorkers <= minWorkers:
		return "min"
	case currentWorkers >= maxWorkers:
		return "max"
	case currentWorkers >= preferredWorkers:
		return "preferred"
	default:
		return "partial"
	}
}

func flavorDomain(name kueue.ResourceFlavorReference) string {
	parts := strings.Split(string(name), "-")
	if len(parts) == 0 {
		return ""
	}
	return strings.ToUpper(parts[len(parts)-1])
}

func flavorGPUSize(name kueue.ResourceFlavorReference) int64 {
	parts := strings.Split(string(name), "-")
	if len(parts) < 2 {
		return 0
	}
	token := strings.TrimSuffix(parts[1], "gpu")
	var size int64
	_, _ = fmt.Sscanf(token, "%d", &size)
	if size < 0 {
		return 0
	}
	return size
}

type workloadSummary struct {
	key                workload.Reference
	profileName        string
	queueName          string
	clusterQueue       kueue.ClusterQueueReference
	priority           int32
	waitSeconds        float64
	runtime            float64
	workerCount        int32
	minWorkerCount     int32
	preferredWorkers   int32
	maxWorkerCount     int32
	elasticEnabled     bool
	totalGPU           int64
	perWorkerGPU       int64
	topologyAware      bool
	topologyPreference string
	queueClass         string
	flavors            []kueue.ResourceFlavorReference
	runtimeByFlavor    map[kueue.ResourceFlavorReference]float64
}

func competingOlderPressure(current workloadSummary, flavorName kueue.ResourceFlavorReference, all []workloadSummary) float64 {
	pressure := 0.0
	flavorSize := flavorGPUSize(flavorName)
	targetDomain := flavorDomain(flavorName)
	for _, other := range all {
		if other.key == current.key {
			continue
		}
		if other.waitSeconds < current.waitSeconds {
			sameBurstTopologyGang := other.topologyAware &&
				strings.EqualFold(other.topologyPreference, targetDomain) &&
				(current.waitSeconds-other.waitSeconds) <= 5.0
			if !sameBurstTopologyGang {
				continue
			}
		}
		if other.queueClass != "gang" {
			continue
		}
		matchesFlavor := false
		for _, candidate := range other.flavors {
			if candidate == flavorName {
				matchesFlavor = true
				break
			}
		}
		if !matchesFlavor {
			continue
		}
		if other.perWorkerGPU >= flavorSize || other.totalGPU > current.totalGPU || other.workerCount > current.workerCount {
			pressure += 1.0 + min(float64(other.workerCount)/4.0, 2.0)
		}
	}
	return pressure
}

func candidateTotalGPUCapacity(cq *schdcache.ClusterQueueSnapshot, fr resources.FlavorResource) int64 {
	if cq == nil {
		return 0
	}
	totalCapacity := cq.QuotaFor(fr).Nominal
	if cq.HasParent() {
		totalCapacity = max(totalCapacity, cq.Parent().Root().ResourceNode.SubtreeQuota[fr])
	}
	return max(totalCapacity, cq.PotentialAvailable(fr))
}

func buildCandidateActions(workloads []workload.Info, snapshot *schdcache.Snapshot) []candidateAction {
	now := float64(time.Now().Unix())
	summaries := make([]workloadSummary, 0, len(workloads))
	for _, info := range workloads {
		cq := snapshot.ClusterQueue(info.ClusterQueue)
		if cq == nil {
			continue
		}
		workerCount, totalGPU, perWorkerGPU := workerStats(&info)
		if totalGPU <= 0 {
			continue
		}
		profileName := workloadProfileName(&info)
		elasticEnabled, minWorkers, preferredWorkers, maxWorkers := workloadElasticBounds(&info, workerCount)
		queueName := string(info.Obj.Spec.QueueName)
		waitSeconds := 0.0
		if !info.Obj.CreationTimestamp.IsZero() {
			waitSeconds = now - float64(info.Obj.CreationTimestamp.Time.Unix())
			if waitSeconds < 0 {
				waitSeconds = 0
			}
		}
		resourceGroup := cq.RGByResource(gpuResourceName)
		if resourceGroup == nil {
			continue
		}
		flavors := append([]kueue.ResourceFlavorReference(nil), resourceGroup.Flavors...)
		runtimeByFlavor := make(map[kueue.ResourceFlavorReference]float64, len(flavors))
		for _, flavorName := range flavors {
			runtimeByFlavor[flavorName] = runtimeSecondsForFlavor(&info, flavorName)
		}
		summaries = append(summaries, workloadSummary{
			key:                workload.Key(info.Obj),
			profileName:        profileName,
			queueName:          queueName,
			clusterQueue:       info.ClusterQueue,
			priority:           utilpriority.Priority(info.Obj),
			waitSeconds:        waitSeconds,
			runtime:            runtimeSecondsForWorkload(&info),
			workerCount:        workerCount,
			minWorkerCount:     minWorkers,
			preferredWorkers:   preferredWorkers,
			maxWorkerCount:     maxWorkers,
			elasticEnabled:     elasticEnabled,
			totalGPU:           totalGPU,
			perWorkerGPU:       perWorkerGPU,
			topologyAware:      topologyAware(&info),
			topologyPreference: strings.TrimSpace(info.Obj.Annotations["admirl.ai/topology-domain"]),
			queueClass:         queueClass(max(workerCount, maxWorkers)),
			flavors:            flavors,
			runtimeByFlavor:    runtimeByFlavor,
		})
	}

	candidates := make([]candidateAction, 0, len(summaries)*4)
	for _, summary := range summaries {
		cq := snapshot.ClusterQueue(summary.clusterQueue)
		if cq == nil {
			continue
		}
		resourceGroup := cq.RGByResource(gpuResourceName)
		if resourceGroup == nil {
			continue
		}
		for _, flavorName := range resourceGroup.Flavors {
			fr := resources.FlavorResource{Flavor: flavorName, Resource: gpuResourceName}
			available := cq.Available(fr)
			totalCapacity := candidateTotalGPUCapacity(cq, fr)
			flavorSize := flavorGPUSize(flavorName)
			scaleTag := elasticScaleTag(summary.workerCount, summary.minWorkerCount, summary.preferredWorkers, summary.maxWorkerCount, summary.elasticEnabled)
			scaleFraction := 1.0
			if summary.maxWorkerCount > 0 {
				scaleFraction = float64(summary.workerCount) / float64(summary.maxWorkerCount)
			}
			candidates = append(candidates, candidateAction{
				ActionID:               string(summary.key) + "@" + string(flavorName),
				WorkloadID:             string(summary.key),
				WorkloadProfile:        summary.profileName,
				FlavorName:             string(flavorName),
				QueueName:              summary.queueName,
				ClusterQueue:           string(summary.clusterQueue),
				FairshareGroup:         summary.queueName,
				Priority:               summary.priority,
				WaitSeconds:            summary.waitSeconds,
				RuntimeSeconds:         summary.runtimeByFlavor[flavorName],
				WorkerCount:            summary.workerCount,
				TotalGPU:               summary.totalGPU,
				PerWorkerGPU:           summary.perWorkerGPU,
				TopologyAware:          summary.topologyAware,
				TopologyPreference:     summary.topologyPreference,
				FlavorDomain:           flavorDomain(flavorName),
				ImmediateFit:           available >= summary.totalGPU,
				Provisionable:          totalCapacity >= summary.totalGPU,
				AvailableGPU:           available,
				TotalGPUCapacity:       totalCapacity,
				FairshareDebt:          0.0,
				RequeueCount:           0,
				QueueClass:             summary.queueClass,
				FlavorGPUSize:          flavorSize,
				OversizeGPU:            max(0, flavorSize-summary.perWorkerGPU),
				CompetingOlderPressure: competingOlderPressure(summary, flavorName, summaries),
				ElasticEnabled:         summary.elasticEnabled,
				MinWorkerCount:         summary.minWorkerCount,
				PreferredWorkerCount:   summary.preferredWorkers,
				MaxWorkerCount:         summary.maxWorkerCount,
				ScaleTag:               scaleTag,
				ScaleFraction:          scaleFraction,
			})
		}
	}
	return candidates
}

func adviceFromResponse(parsed admissionResponse) Advice {
	workloadScores := make(map[workload.Reference]float64, len(parsed.WorkloadScores))
	for key, value := range parsed.WorkloadScores {
		workloadScores[workload.Reference(key)] = value
	}
	flavorRankings := make(map[workload.Reference][]kueue.ResourceFlavorReference, len(parsed.FlavorRankings))
	for key, order := range parsed.FlavorRankings {
		ranked := make([]kueue.ResourceFlavorReference, 0, len(order))
		for _, flavor := range order {
			ranked = append(ranked, kueue.ResourceFlavorReference(flavor))
		}
		flavorRankings[workload.Reference(key)] = ranked
	}
	guardPlan := ProtectedWorkloadPlan{
		ProtectedWorkload:    workload.Reference(parsed.ProtectedWorkload),
		ProtectedPriority:    parsed.ProtectedPriority,
		ProtectedWorkerCount: parsed.ProtectedWorkerCount,
		ProtectedTotalGPU:    parsed.ProtectedTotalGPU,
	}
	if len(parsed.ProtectedFlavors) > 0 {
		guardPlan.ProtectedFlavors = make([]kueue.ResourceFlavorReference, 0, len(parsed.ProtectedFlavors))
		for _, flavor := range parsed.ProtectedFlavors {
			guardPlan.ProtectedFlavors = append(guardPlan.ProtectedFlavors, kueue.ResourceFlavorReference(flavor))
		}
	}
	return Advice{
		WorkloadScores: workloadScores,
		FlavorRankings: flavorRankings,
		GuardPlan:      guardPlan,
		Source:         parsed.Source,
	}
}

func RequestAdvice(ctx context.Context, workloads []workload.Info, snapshot *schdcache.Snapshot) (Advice, error) {
	if !EnabledFromEnv() || len(workloads) == 0 || snapshot == nil {
		return Advice{}, nil
	}
	req := admissionRequest{
		RequestMode: "kueue-admission",
		Time:        float64(time.Now().Unix()),
		Candidates:  buildCandidateActions(workloads, snapshot),
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Advice{}, fmt.Errorf("marshal kueue admission advice request: %w", err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(advisorURLFromEnv()+"/api/kueue/admission-advice", "application/json", bytes.NewReader(body))
	if err != nil {
		return Advice{}, fmt.Errorf("post kueue admission advice request: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return Advice{}, fmt.Errorf("read kueue admission advice response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return Advice{}, fmt.Errorf("kueue admission advice response status %d: %s", resp.StatusCode, string(payload))
	}
	var parsed admissionResponse
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return Advice{}, fmt.Errorf("decode kueue admission advice response: %w", err)
	}
	return adviceFromResponse(parsed), nil
}

func (httpClient) AdviseAdmission(ctx context.Context, workloads []workload.Info, snapshot *schdcache.Snapshot) (Advice, error) {
	return RequestAdvice(ctx, workloads, snapshot)
}
