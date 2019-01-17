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
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/kubernetes-incubator/metrics-server/pkg/sources"
	"github.com/kubernetes-incubator/metrics-server/pkg/sources/summary"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/pool"
	"github.com/prometheus/prometheus/pkg/textparse"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/rest"
)

// KubeletInterface knows how to fetch metrics from the Kubelet
type KubeletInterface interface {
	// GetCorePrometheus fetches core metrics from the given Kubelet
	GetCorePrometheus(ctx context.Context, host, hostName string) (*sources.MetricsBatch, error)
}

type kubeletClient struct {
	port            int
	deprecatedNoTLS bool
	client          *http.Client
	buffers         *pool.Pool
	lastScrapeSize  int
	typeMu          sync.RWMutex
	typeCache       map[string]textparse.MetricType
	metaMu          sync.RWMutex
	metaCache       map[string]*MetricMeta
}

type ErrNotFound struct {
	endpoint string
}

func (err *ErrNotFound) Error() string {
	return fmt.Sprintf("%q not found", err.endpoint)
}

func IsNotFoundError(err error) bool {
	_, isNotFound := err.(*ErrNotFound)
	return isNotFound
}

func (kc *kubeletClient) GetCorePrometheus(ctx context.Context, host, hostName string) (*sources.MetricsBatch, error) {
	scheme := "https"
	if kc.deprecatedNoTLS {
		scheme = "http"
	}
	url := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, strconv.Itoa(kc.port)),
		Path:   "/metrics/core",
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	client := kc.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &ErrNotFound{req.URL.String()}
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed - %q", resp.Status)
	}

	b := kc.buffers.Get(kc.lastScrapeSize).([]byte)
	buf := bytes.NewBuffer(b)
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body - %v", err)
	}
	b = buf.Bytes()
	if len(b) > 0 {
		kc.lastScrapeSize = len(b)
	}
	metrics := kc.parseMetrics(b, resp.Header.Get("Content-Type"))
	metrics.Nodes[0].Name = hostName

	kc.buffers.Put(b)

	return metrics, nil
}

func (kc *kubeletClient) parseMetrics(bytes []byte, contentType string) *sources.MetricsBatch {
	parser := textparse.New(bytes, contentType)
	res := &sources.MetricsBatch{
		Nodes: []sources.NodeMetricsPoint{{}},
		Pods:  []sources.PodMetricsPoint{},
	}
	scrapeTime := time.Now().Unix()

	for {
		et, err := parser.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		switch et {
		case textparse.EntryType:
			metric, t := parser.Type()
			kc.typeMu.Lock()
			if _, found := kc.typeCache[castString(metric)]; !found {
				kc.typeCache[string(metric)] = t
			}
			kc.typeMu.Unlock()
			continue
		case textparse.EntryHelp:
			// ignore help
			continue
		case textparse.EntryUnit:
			// ignore units
			continue
		case textparse.EntryComment:
			continue
		default:
		}

		ts, timestamp, value := parser.Series()
		if timestamp == nil {
			timestamp = &scrapeTime
		}

		kc.metaMu.Lock()
		meta, found := kc.metaCache[castString(ts)]
		if !found {
			var parsedLabels labels.Labels
			parser.Metric(&parsedLabels)
			meta = &MetricMeta{}
			for _, label := range parsedLabels {
				switch label.Name {
				case "__name__":
					// Prometheus stores metric name as special label
					meta.name = label.Value
				case "container_name":
					meta.container = label.Value
				case "pod_name":
					meta.pod = label.Value
				case "pod_namespace":
					meta.namespace = label.Value
				}
			}
			kc.metaCache[string(ts)] = meta
		}
		kc.metaMu.Unlock()

		if meta.name == "" || meta.container == "" {
			// make sure we have common labels
			continue
		}

		typ, found := kc.typeCache[meta.name]
		if !found || typ != textparse.MetricTypeGauge {
			continue
		}

		if meta.container == "machine" {
			decodeMetric(meta.name, value, *timestamp, &res.Nodes[0].MetricsPoint)
		} else if meta.pod == "" || meta.namespace == "" {
			continue
		} else if meta.container != "pod_sandbox" {
			decodePodStats(meta, value, *timestamp, &res.Pods)
		}
	}
	return res
}

func decodePodStats(meta *MetricMeta, value float64, timestampMs int64, targets *[]sources.PodMetricsPoint) {
	for i, pod := range *targets {
		if pod.Name != meta.pod || pod.Namespace != meta.namespace {
			continue
		}
		if pod.Containers == nil {
			pod.Containers = []sources.ContainerMetricsPoint{}
		}
		for j, container := range pod.Containers {
			if container.Name != meta.container {
				continue
			}
			decodeMetric(meta.name, value, timestampMs, &(*targets)[i].Containers[j].MetricsPoint)
			return
		}
		point := sources.ContainerMetricsPoint{
			Name: meta.container,
		}
		decodeMetric(meta.name, value, timestampMs, &point.MetricsPoint)
		(*targets)[i].Containers = append(pod.Containers, point)
		return
	}
	point := sources.PodMetricsPoint{
		Name:      meta.pod,
		Namespace: meta.namespace,
		Containers: []sources.ContainerMetricsPoint{
			{
				Name: meta.container,
			},
		},
	}
	decodeMetric(meta.name, value, timestampMs, &point.Containers[0].MetricsPoint)
	*targets = append(*targets, point)
}

func decodeMetric(name string, value float64, timestampMs int64, target *sources.MetricsPoint) {
	if target == nil {
		*target = sources.MetricsPoint{
			Timestamp: time.Unix(0, timestampMs),
		}
	}
	switch name {
	case "cpu_usage_millicore_seconds":
		usage := resource.NewMilliQuantity(int64(value), resource.DecimalSI)
		target.CpuUsage = *usage
	case "memory_working_set_bytes":
		usage := resource.NewQuantity(int64(value), resource.DecimalSI)
		target.MemoryUsage = *usage
	case "ephemeral_storage_usage_bytes":
		usage := resource.NewQuantity(int64(value), resource.DecimalSI)
		target.EphemeralStorageUsage = *usage
	}
}

type MetricMeta struct {
	name      string
	container string
	pod       string
	namespace string
}

func timeseriesMatchesMetric(timeseries, metricName string) bool {
	return strings.HasPrefix(timeseries, metricName) &&
		(len(metricName) == len(timeseries) || timeseries[len(metricName)] == '{')
}

const (
	scalingFactor        = 3
	bufferMinSizeInBytes = 1e3
	bufferMaxSizeInBytes = 1e6
)

func NewKubeletClient(transport http.RoundTripper, port int, deprecatedNoTLS bool) (KubeletInterface, error) {
	c := &http.Client{
		Transport: transport,
	}
	return &kubeletClient{
		port:            port,
		client:          c,
		deprecatedNoTLS: deprecatedNoTLS,
		buffers:         pool.New(bufferMinSizeInBytes, bufferMaxSizeInBytes, scalingFactor, func(size int) interface{} { return make([]byte, 0, size) }),
		typeCache:       make(map[string]textparse.MetricType),
		metaCache:       make(map[string]*MetricMeta),
	}, nil
}

// KubeletClientFor constructs a new KubeletInterface for the given configuration.
func KubeletClientFor(config *summary.KubeletClientConfig) (KubeletInterface, error) {
	transport, err := rest.TransportFor(config.RESTConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to construct transport: %v", err)
	}

	return NewKubeletClient(transport, config.Port, config.DeprecatedCompletelyInsecure)
}

func castString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
