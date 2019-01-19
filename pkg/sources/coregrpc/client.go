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
	"fmt"

	"github.com/kubernetes-incubator/metrics-server/pkg/sources/summary"
)

// KubeletInterface knows how to fetch metrics from the Kubelet
type KubeletInterface interface {
	// GetCoreGRPCAddr fetches core metrics from the given Kubelet
	GetCoreGRPCAddr(host string) string
}

type kubeletClient struct {
	port int
}

func (kc *kubeletClient) GetCoreGRPCAddr(host string) string {
	return fmt.Sprintf("%s:%d", host, kc.port)
}

func KubeletClientFor(config *summary.KubeletClientConfig) (KubeletInterface, error) {
	return &kubeletClient{
		port: config.Port,
	}, nil
}
