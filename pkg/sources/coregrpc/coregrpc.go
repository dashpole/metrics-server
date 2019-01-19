// Copyright 2018 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coregrpc

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/metrics-server/pkg/sources"
	"github.com/kubernetes-incubator/metrics-server/pkg/sources/summary"
	corestatsapi "k8s.io/kubernetes/pkg/kubelet/apis/corestats/v1alpha1"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	v1listers "k8s.io/client-go/listers/core/v1"
	corestats "k8s.io/kubernetes/pkg/kubelet/apis/corestats/v1alpha1"
)

// NodeInfo contains the information needed to identify and connect to a particular node
// (node name and preferred address).
type NodeInfo struct {
	Name           string
	ConnectAddress string
}

// Kubelet-provided metrics for pod and system container.
type grpcMetricsSource struct {
	node          NodeInfo
	kubeletClient KubeletInterface
}

func NewCoreMetricsSource(node NodeInfo, client KubeletInterface) sources.MetricSource {
	return &grpcMetricsSource{
		node:          node,
		kubeletClient: client,
	}
}

func (src *grpcMetricsSource) Name() string {
	return src.String()
}

func (src *grpcMetricsSource) String() string {
	return fmt.Sprintf("kubelet_core_grpc:%s", src.node.Name)
}

func (src *grpcMetricsSource) Collect(ctx context.Context) (*sources.MetricsBatch, error) {
	addr := src.kubeletClient.GetCoreGRPCAddr(src.node.ConnectAddress)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Error dialing addres %s: %v", addr, err)
	}
	defer conn.Close()
	client := corestatsapi.NewCoreStatsClient(conn)

	resp, err := client.Get(ctx, &corestats.StatsRequest{})
	if err != nil {
		return nil, fmt.Errorf("%+v.Get(_) = _, %v", client, err)
	}

	return src.convert(resp)
}

func (src *grpcMetricsSource) convert(stats *corestats.StatsResponse) (*sources.MetricsBatch, error) {
	res := &sources.MetricsBatch{
		Nodes: make([]sources.NodeMetricsPoint, 1),
		Pods:  make([]sources.PodMetricsPoint, len(stats.Pods)),
	}

	var errs []error
	errs = append(errs, src.decodeNodeStats(stats.Node, &res.Nodes[0]))
	if len(errs) != 0 {
		// if we had errors providing node metrics, discard the data point
		// so that we don't incorrectly report metric values as zero.
		res.Nodes = res.Nodes[:1]
	}

	num := 0
	for _, pod := range stats.Pods {
		podErr := src.decodePodStats(pod, &res.Pods[num])
		errs = append(errs, podErr)
		if podErr != nil {
			// NB: we explicitly want to discard pods with partial results, since
			// the horizontal pod autoscaler takes special action when a pod is missing
			// metrics (and zero CPU or memory does not count as "missing metrics")

			// we don't care if we reuse slots in the result array,
			// because they get completely overwritten in decodePodStats
			continue
		}
		num++
	}
	res.Pods = res.Pods[:num]

	return res, utilerrors.NewAggregate(errs)
}

func (src *grpcMetricsSource) decodeNodeStats(stats *corestats.Usage, target *sources.NodeMetricsPoint) error {
	if stats == nil {
		return fmt.Errorf("unable to get Usage for node %q, discarding data: missing stats", src.node.ConnectAddress)
	}
	*target = sources.NodeMetricsPoint{
		Name: src.node.Name,
		MetricsPoint: sources.MetricsPoint{
			Timestamp:   time.Unix(0, stats.Time),
			CpuUsage:    *uint64Quantity(stats.CpuUsageNanoCores, -9),
			MemoryUsage: *uint64Quantity(stats.MemoryWorkingSetBytes, 0),
		},
	}
	return nil
}

func (src *grpcMetricsSource) decodePodStats(stats *corestats.PodUsage, target *sources.PodMetricsPoint) error {
	if stats == nil {
		return fmt.Errorf("nil stats for pod %s/%s on node %q, discarding data", stats.Namespace, stats.Name, src.node.ConnectAddress)
	}
	// completely overwrite data in the target
	*target = sources.PodMetricsPoint{
		Name:       stats.Name,
		Namespace:  stats.Namespace,
		Containers: make([]sources.ContainerMetricsPoint, len(stats.Containers)),
	}

	for i, container := range stats.Containers {
		target.Containers[i] = sources.ContainerMetricsPoint{
			Name: container.Name,
			MetricsPoint: sources.MetricsPoint{
				Timestamp:   time.Unix(0, container.Usage.Time),
				CpuUsage:    *uint64Quantity(container.Usage.CpuUsageNanoCores, -9),
				MemoryUsage: *uint64Quantity(container.Usage.MemoryWorkingSetBytes, 0),
			},
		}
	}
	return nil
}

// uint64Quantity converts a uint64 into a Quantity, which only has constructors
// that work with int64 (except for parse, which requires costly round-trips to string).
// We lose precision until we fit in an int64 if greater than the max int64 value.
func uint64Quantity(val uint64, scale resource.Scale) *resource.Quantity {
	// easy path -- we can safely fit val into an int64
	if val <= math.MaxInt64 {
		return resource.NewScaledQuantity(int64(val), scale)
	}

	glog.V(1).Infof("unexpectedly large resource value %v, loosing precision to fit in scaled resource.Quantity", val)

	// otherwise, lose an decimal order-of-magnitude precision,
	// so we can fit into a scaled quantity
	return resource.NewScaledQuantity(int64(val/10), resource.Scale(1)+scale)
}

type coreGRPCProvider struct {
	nodeLister    v1listers.NodeLister
	kubeletClient KubeletInterface
	addrResolver  summary.NodeAddressResolver
}

func (p *coreGRPCProvider) GetMetricSources() ([]sources.MetricSource, error) {
	sources := []sources.MetricSource{}
	nodes, err := p.nodeLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("unable to list nodes: %v", err)
	}

	var errs []error
	for _, node := range nodes {
		info, err := p.getNodeInfo(node)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to extract connection information for node %q: %v", node.Name, err))
			continue
		}
		sources = append(sources, NewCoreMetricsSource(info, p.kubeletClient))
	}
	return sources, utilerrors.NewAggregate(errs)
}

func (p *coreGRPCProvider) getNodeInfo(node *corev1.Node) (NodeInfo, error) {
	// TODO(directxman12): why do we skip unready nodes?
	nodeReady := false
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			nodeReady = c.Status == corev1.ConditionTrue
			break
		}
	}
	if !nodeReady {
		return NodeInfo{}, fmt.Errorf("node %v is not ready", node.Name)
	}

	addr, err := p.addrResolver.NodeAddress(node)
	if err != nil {
		return NodeInfo{}, err
	}
	info := NodeInfo{
		Name:           node.Name,
		ConnectAddress: addr,
	}

	return info, nil
}

func NewProvider(nodeLister v1listers.NodeLister, kubeletClient KubeletInterface, addrResolver summary.NodeAddressResolver) sources.MetricSourceProvider {
	return &coreGRPCProvider{
		nodeLister:    nodeLister,
		kubeletClient: kubeletClient,
		addrResolver:  addrResolver,
	}
}
