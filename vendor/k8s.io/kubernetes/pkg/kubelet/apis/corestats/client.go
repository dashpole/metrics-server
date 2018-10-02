/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package corestats

import (
	"fmt"

	"google.golang.org/grpc"

	api "k8s.io/kubernetes/pkg/kubelet/apis/corestats/v1alpha1"
)

func GetClient(addr string) (api.CoreStatsClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("Error dialing addres %s: %v", addr, err)
	}
	return api.NewCoreStatsClient(conn), conn, nil
}
