/*
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

package resources_count_spec

import (
	kubegresv1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/states"
)

type KubegresCountSpecEnforcer struct {
	kubegresRestoreContext ctx.KubegresRestoreContext
	restoreStates          states.RestoreJobStates
	kubegresSpec           kubegresv1.KubegresSpec
}

func CreateKubegresCountSpecEnforcer(kubegresRestoreContext ctx.KubegresRestoreContext,
	restoreStates states.RestoreJobStates,
	kubegresSpec kubegresv1.KubegresSpec) KubegresCountSpecEnforcer {
	return KubegresCountSpecEnforcer{
		kubegresRestoreContext: kubegresRestoreContext,
		restoreStates:          restoreStates,
		kubegresSpec:           kubegresSpec,
	}
}

func (r *KubegresCountSpecEnforcer) EnforceSpec() error {
	//TODO: If job is completed, set replicas and update the cluster spec.
	if !r.isClusterDeployed() {
		return r.deployKubegres()
	}
	return nil
}

func (r *KubegresCountSpecEnforcer) deployKubegres() error {
	var replicas int32 = 1
	kubegres := &kubegresv1.Kubegres{}
	kubegres.Spec = r.kubegresSpec
	kubegres.ObjectMeta.Name = r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName
	kubegres.ObjectMeta.Namespace = r.kubegresRestoreContext.KubegresRestore.Namespace
	kubegres.Spec.Replicas = &replicas
	kubegres.Spec.Resources = r.kubegresRestoreContext.KubegresRestore.Spec.Resources

	err := r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, kubegres)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("KubegresDeploymentErr", err, "Unable to deploy kubegres resource.")
		return err
	}

	r.kubegresRestoreContext.Log.InfoEvent("KubegresDeployed", "Deployed kubegres resource", "Kubegres name", kubegres.Name)

	return nil
}

func (r *KubegresCountSpecEnforcer) isClusterDeployed() bool {
	return r.restoreStates.IsClusterDeployed
}
