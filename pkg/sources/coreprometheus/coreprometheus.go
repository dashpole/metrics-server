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

package coreprometheus

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/metrics-server/pkg/sources"
	"github.com/kubernetes-incubator/metrics-server/pkg/sources/summary"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	v1listers "k8s.io/client-go/listers/core/v1"
)

// NodeInfo contains the information needed to identify and connect to a particular node
// (node name and preferred address).
type NodeInfo struct {
	Name           string
	ConnectAddress string
}

// Kubelet-provided metrics for pod and system container.
type prometheusMetricsSource struct {
	node          NodeInfo
	kubeletClient KubeletInterface
}

func NewCoreMetricsSource(node NodeInfo, client KubeletInterface) sources.MetricSource {
	return &prometheusMetricsSource{
		node:          node,
		kubeletClient: client,
	}
}

func (src *prometheusMetricsSource) Name() string {
	return src.String()
}

func (src *prometheusMetricsSource) String() string {
	return fmt.Sprintf("kubelet_core_prometheus:%s", src.node.Name)
}

func (src *prometheusMetricsSource) Collect(ctx context.Context) (*sources.MetricsBatch, error) {
	stats, err := src.kubeletClient.GetCorePrometheus(ctx, src.node.ConnectAddress, src.node.Name)

	if err != nil {
		return nil, fmt.Errorf("unable to fetch metrics from Kubelet %s (%s): %v", src.node.Name, src.node.ConnectAddress, err)
	}

	res := &sources.MetricsBatch{
		Nodes: []sources.NodeMetricsPoint{
			{
				Name: src.node.Name,
			},
		},
		Pods: []sources.PodMetricsPoint{},
	}

	for _, family := range stats {
		name := family.GetName()
		if family.GetType() != dto.MetricType_GAUGE {
			// skip non-gauge metrics
			continue
		}
		for _, metric := range family.GetMetric() {
			info, err := getLabelInfo(metric.GetLabel())
			if err != nil {
				glog.Infof("error getting labels for metric %s: %v", name, err)
				continue
			}
			timestamp := metric.GetTimestampMs()
			value := int64(metric.GetGauge().GetValue())
			if info.container == "machine" {
				decodeMetric(name, value, timestamp, &res.Nodes[0].MetricsPoint)
			} else if info.container != "pod_sandbox" {
				src.decodePodStats(name, value, timestamp, info, &res.Pods)
			}
		}
	}
	return res, nil
}

func getLabelInfo(labels []*dto.LabelPair) (*labelInfo, error) {
	info := &labelInfo{}
	cFound, pFound, nFound := false, false, false
	for _, label := range labels {
		if label.GetName() == "container_name" {
			cFound = true
			info.container = label.GetValue()
		} else if label.GetName() == "pod_name" {
			pFound = true
			info.pod = label.GetValue()
		} else if label.GetName() == "pod_namespace" {
			nFound = true
			info.namespace = label.GetValue()
		}
	}
	if !cFound || !pFound || !nFound {
		return nil, fmt.Errorf("did not find labels %q, %q, and %q in labels %v", "container_name", "pod_name", "pod_namespace", labels)
	}
	return info, nil
}

type labelInfo struct {
	container string
	pod       string
	namespace string
}

func (src *prometheusMetricsSource) decodePodStats(name string, value int64, timestampMs int64, info *labelInfo, targets *[]sources.PodMetricsPoint) {
	for i, pod := range *targets {
		if pod.Name != info.pod || pod.Namespace != info.namespace {
			continue
		}
		if pod.Containers == nil {
			pod.Containers = []sources.ContainerMetricsPoint{}
		}
		for j, container := range pod.Containers {
			if container.Name != info.container {
				continue
			}
			decodeMetric(name, value, timestampMs, &(*targets)[i].Containers[j].MetricsPoint)
			return
		}
		point := sources.ContainerMetricsPoint{
			Name: info.container,
		}
		decodeMetric(name, value, timestampMs, &point.MetricsPoint)
		(*targets)[i].Containers = append(pod.Containers, point)
		return
	}
	point := sources.PodMetricsPoint{
		Name:      info.pod,
		Namespace: info.namespace,
		Containers: []sources.ContainerMetricsPoint{
			{
				Name: info.container,
			},
		},
	}
	decodeMetric(name, value, timestampMs, &point.Containers[0].MetricsPoint)
	*targets = append(*targets, point)
}

func decodeMetric(name string, value int64, timestampMs int64, target *sources.MetricsPoint) {
	if target == nil {
		*target = sources.MetricsPoint{
			Timestamp: time.Unix(0, timestampMs),
		}
	}
	switch name {
	case "cpu_usage_millicore_seconds":
		usage := resource.NewMilliQuantity(value, resource.DecimalSI)
		target.CpuUsage = *usage
	case "memory_working_set_bytes":
		usage := resource.NewQuantity(value, resource.DecimalSI)
		target.MemoryUsage = *usage
	case "ephemeral_storage_usage_bytes":
		usage := resource.NewQuantity(value, resource.DecimalSI)
		target.EphemeralStorageUsage = *usage
	}
}

type corePrometheusProvider struct {
	nodeLister    v1listers.NodeLister
	kubeletClient KubeletInterface
	addrResolver  summary.NodeAddressResolver
}

func (p *corePrometheusProvider) GetMetricSources() ([]sources.MetricSource, error) {
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

func (p *corePrometheusProvider) getNodeInfo(node *corev1.Node) (NodeInfo, error) {
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

func NewCorePrometheusProvider(nodeLister v1listers.NodeLister, kubeletClient KubeletInterface, addrResolver summary.NodeAddressResolver) sources.MetricSourceProvider {
	return &corePrometheusProvider{
		nodeLister:    nodeLister,
		kubeletClient: kubeletClient,
		addrResolver:  addrResolver,
	}
}
