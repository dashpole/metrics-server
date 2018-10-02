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

	"k8s.io/kubernetes/pkg/kubelet/apis/corestats"
	corestatsapi "k8s.io/kubernetes/pkg/kubelet/apis/corestats/v1alpha1"
)

// KubeletCoreInterface knows how to fetch metrics from the Kubelet
type KubeletCoreInterface interface {
	// GetCoreGRPC fetches core metrics from the given Kubelet
	GetCoreGRPC(ctx context.Context, host string) (*corestatsapi.StatsResponse, error)
}

type kubeletClient struct {
	port int
}

func (kc *kubeletClient) GetCoreGRPC(ctx context.Context, host string) (*corestatsapi.StatsResponse, error) {
	addr := fmt.Sprintf("%s:%d", host, kc.port)
	client, conn, err := corestats.GetClient(addr)
	if err != nil {
		return nil, fmt.Errorf("Error getting corestats client at %s: %v", addr, err)
	}
	defer conn.Close()
	resp, err := client.Get(ctx, &corestatsapi.StatsRequest{})
	if err != nil {
		return nil, fmt.Errorf("%+v.Get(_) = _, %v", client, err)
	}
	return resp, nil
}

func NewKubeletCoreClient(port int) (KubeletCoreInterface, error) {
	return &kubeletClient{
		port: port,
	}, nil
}
