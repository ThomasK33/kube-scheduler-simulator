package networkbandwidth

import (
	"context"
	"fmt"
	"math"

	"golang.org/x/xerrors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

const (
	Name = "NetworkBandwidth"
)

var (
	_ framework.FilterPlugin      = &NetworkBandwidth{}
	_ framework.ScorePlugin       = &NetworkBandwidth{}
	_ framework.ScoreExtensions   = &NetworkBandwidth{}
	_ framework.EnqueueExtensions = &NetworkBandwidth{}
)

type NetworkBandwidth struct {
	args         NetworkBandwidthArgs
	sharedLister framework.SharedLister
}

func (n *NetworkBandwidth) Name() string {
	return Name
}

// Filter is called by the scheduling framework.
// All FilterPlugins should return "Success" to declare that
// the given node fits the pod. If Filter doesn't return "Success",
// it will return "Unschedulable", "UnschedulableAndUnresolvable" or "Error".
// For the node being evaluated, Filter plugins should look at the passed
// nodeInfo reference for this particular node's information (e.g., pods
// considered to be running on the node) instead of looking it up in the
// NodeInfoSnapshot because we don't guarantee that they will be the same.
// For example, during preemption, we may pass a copy of the original
// nodeInfo object that has some pods removed from it to evaluate the
// possibility of preempting them to schedule the target pod.
func (n *NetworkBandwidth) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	// Get node's network bandwidth limit
	limitAnnotation, ok := nodeInfo.Node().Annotations[n.args.NodeLimitAnnotation]
	if !ok {
		return framework.NewStatus(framework.Skip, fmt.Sprintf("Node %v does not have %v annotation present", nodeInfo.Node().Name, n.args.NodeLimitAnnotation))
	}
	limit, err := resource.ParseQuantity(limitAnnotation)
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("Node %v has an incorrect quantity in %v annotation present", nodeInfo.Node().Name, n.args.NodeLimitAnnotation))
	}

	allocatedAmount := getNodeAllocatedAmount(nodeInfo, n.args.IngressRequestAnnotation, n.args.EgressRequestAnnotation)

	// Obtain the pod's network requests
	podRequests := resource.NewQuantity(0, resource.DecimalSI)
	if ingressAnnotation, ok := pod.Annotations[n.args.IngressRequestAnnotation]; ok {
		ingressRequest, err := resource.ParseQuantity(ingressAnnotation)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("Could not parse quantity from pod %v %v annotations", pod.Name, n.args.IngressRequestAnnotation))
		}
		podRequests.Add(ingressRequest)
	}
	if requestAnnotation, ok := pod.Annotations[n.args.EgressRequestAnnotation]; ok {
		egressRequest, err := resource.ParseQuantity(requestAnnotation)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("Could not parse quantity from pod %v %v annotations", pod.Name, n.args.EgressRequestAnnotation))
		}
		podRequests.Add(egressRequest)
	}

	if podRequests.CmpInt64(0) == 0 {
		return framework.NewStatus(framework.Skip, fmt.Sprintf("Pod %v does not have network bandwidth request annotations set. (Missing %v or %v)", pod.Name, n.args.IngressRequestAnnotation, n.args.EgressRequestAnnotation))
	}

	// Check if allocatedAmount + podRequests <= limit
	if allocatedAmount.Add(*podRequests); allocatedAmount.Cmp(limit) > 0 {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node %v does not have enough network bandwidth capacity to schedule pod", nodeInfo.Node().Name))
	}

	return framework.NewStatus(framework.Success)
}

func getNodeAllocatedAmount(nodeInfo *framework.NodeInfo, ingressRequestAnnotation, egressRequestAnnotation string) *resource.Quantity {
	// Sum the already allocated amount
	allocatedAmount := resource.NewQuantity(0, resource.DecimalSI)
	for _, pod := range nodeInfo.Pods {
		if ingressAnnotation, ok := pod.Pod.Annotations[ingressRequestAnnotation]; ok {
			if ingressRequest, err := resource.ParseQuantity(ingressAnnotation); err == nil {
				allocatedAmount.Add(ingressRequest)
			}
		}
	}
	for _, pod := range nodeInfo.Pods {
		if requestAnnotation, ok := pod.Pod.Annotations[egressRequestAnnotation]; ok {
			if egressRequest, err := resource.ParseQuantity(requestAnnotation); err == nil {
				allocatedAmount.Add(egressRequest)
			}
		}
	}

	return allocatedAmount
}

// Score is called on each filtered node. It must return success and an integer
// indicating the rank of the node. All scoring plugins must return success or
// the pod will be rejected.
func (n *NetworkBandwidth) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := n.sharedLister.NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("failed to get node %q from Snapshot %w", nodeName, err))
	}

	// Get node's network bandwidth limit
	limitAnnotation, ok := nodeInfo.Node().Annotations[n.args.NodeLimitAnnotation]
	if !ok {
		return 0, framework.NewStatus(framework.Skip, fmt.Sprintf("Node %v does not have %v annotation present", nodeInfo.Node().Name, n.args.NodeLimitAnnotation))
	}
	limit, err := resource.ParseQuantity(limitAnnotation)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Node %v has an incorrect quantity in %v annotation present", nodeInfo.Node().Name, n.args.NodeLimitAnnotation))
	}

	allocatedAmount := getNodeAllocatedAmount(nodeInfo, n.args.IngressRequestAnnotation, n.args.EgressRequestAnnotation)

	limit.Sub(*allocatedAmount)

	return limit.Value(), nil
}

// ScoreExtensions returns a ScoreExtensions interface if it implements one, or nil if does not.
func (n *NetworkBandwidth) ScoreExtensions() framework.ScoreExtensions {
	return n
}

// NormalizeScore is called for all node scores produced by the same plugin's "Score"
// method. A successful run of NormalizeScore will update the scores list and return
// a success status.
func (n *NetworkBandwidth) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var minScore int64 = math.MaxInt64
	var maxScore int64 = math.MinInt64

	for _, nodeScore := range scores {
		score := nodeScore.Score

		if score > maxScore {
			maxScore = score
		}
		if score < minScore {
			minScore = score
		}
	}

	delta := maxScore - minScore

	for i := range scores {
		normalizedScore := float64(0)
		if delta > 0 {
			normalizedScore = float64(framework.MaxNodeScore) * (float64(scores[i].Score-minScore) / float64(delta))
		}

		scores[i].Score = int64(normalizedScore)
	}

	return nil
}

func (n *NetworkBandwidth) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Node, ActionType: framework.Add},
	}
}

// New initializes a new plugin and returns it.
func New(arg runtime.Object, h framework.Handle) (framework.Plugin, error) {
	if h.SnapshotSharedLister() == nil {
		return nil, xerrors.Errorf("SnapshotSharedLister is nil")
	}

	typedArg := NetworkBandwidthArgs{
		NodeLimitAnnotation:      "node.kubernetes.io/network-limit",
		EgressRequestAnnotation:  "kubernetes.io/egress-request",
		IngressRequestAnnotation: "kubernetes.io/ingress-request",
	}
	if arg != nil {
		err := frameworkruntime.DecodeInto(arg, &typedArg)
		if err != nil {
			return nil, xerrors.Errorf("decode arg into NetworkBandwidthArgs: %w", err)
		}
		klog.Info("NetworkBandwidthArgs is successfully applied")
	}

	return &NetworkBandwidth{
		args:         typedArg,
		sharedLister: h.SnapshotSharedLister(),
	}, nil
}

// NetworkBandwidthArgs is arguments for node number plugin.
//
//nolint:revive
type NetworkBandwidthArgs struct {
	metav1.TypeMeta

	NodeLimitAnnotation      string `json:"nodeLimitAnnotation,omitempty"`
	EgressRequestAnnotation  string `json:"egressRequestAnnotation,omitempty"`
	IngressRequestAnnotation string `json:"ingressRequestAnnotation,omitempty"`
}
