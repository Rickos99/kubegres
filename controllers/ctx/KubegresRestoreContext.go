/*
Copyright 2022.

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

package ctx

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubegresRestoreContext struct {
	KubegresRestore *v1.KubegresRestore
	Status          *status.RestoreStatusWrapper
	Ctx             context.Context
	Log             log.LogWrapper[*v1.KubegresRestore]
	Client          client.Client
}

const (
	RestoreJobSuffix = "-job"
)

func (r *KubegresRestoreContext) GetRestoreJobName() string {
	return r.KubegresRestore.Name + RestoreJobSuffix
}

func (r *KubegresRestoreContext) GetNamespacesresourceName(name string) types.NamespacedName {
	return types.NamespacedName{
		Namespace: r.KubegresRestore.Namespace,
		Name:      name,
	}
}
