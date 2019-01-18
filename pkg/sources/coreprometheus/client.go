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
	"net"
	"net/http"
	"net/url"
	"strconv"
	"unsafe"

	"github.com/kubernetes-incubator/metrics-server/pkg/sources/summary"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"k8s.io/client-go/rest"
)

// KubeletInterface knows how to fetch metrics from the Kubelet
type KubeletInterface interface {
	// GetCorePrometheus fetches core metrics from the given Kubelet
	GetCorePrometheus(ctx context.Context, host, hostName string) (map[string]*dto.MetricFamily, error)
}

type kubeletClient struct {
	port            int
	deprecatedNoTLS bool
	client          *http.Client
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

func (kc *kubeletClient) GetCorePrometheus(ctx context.Context, host, hostName string) (map[string]*dto.MetricFamily, error) {
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

	parser := &expfmt.TextParser{}
	metrics, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}

	return metrics, nil
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
