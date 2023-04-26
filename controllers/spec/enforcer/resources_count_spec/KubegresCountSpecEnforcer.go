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
	restoreStates          states.RestoreResourceStates
	targetKubegresSpec     kubegresv1.KubegresSpec
}

func CreateKubegresCountSpecEnforcer(kubegresRestoreContext ctx.KubegresRestoreContext,
	restoreStates states.RestoreResourceStates,
	targetKubegresSpec kubegresv1.KubegresSpec) KubegresCountSpecEnforcer {
	return KubegresCountSpecEnforcer{
		kubegresRestoreContext: kubegresRestoreContext,
		restoreStates:          restoreStates,
		targetKubegresSpec:     targetKubegresSpec,
	}
}

func (r *KubegresCountSpecEnforcer) EnforceSpec() error {
	if !r.isClusterDeployed() {
		return r.deployKubegres()
	}

	if r.isJobCompleted() {
		// TODO: reorder these 2 operations. First modify resource limits, then add replicas. Should be faster that way.
		if r.kubegresHasReplicas() {
			if err := r.addReplicasToKubegres(); err != nil {
				return err
			}
		}

		if r.areCustomResourceLimitsDefined() {
			return r.modifyResourceLimitsOfKubegres()
		}
	}

	return nil
}

func (r *KubegresCountSpecEnforcer) deployKubegres() error {
	var replicas int32 = 1
	kubegres := &kubegresv1.Kubegres{}
	kubegres.Spec = r.targetKubegresSpec
	kubegres.ObjectMeta.Name = r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName
	kubegres.ObjectMeta.Namespace = r.kubegresRestoreContext.KubegresRestore.Namespace
	kubegres.Spec.Replicas = &replicas

	kubegres.Labels = map[string]string{}
	kubegres.Labels[ctx.ManagedByKubegresRestoreLabel] = "true"

	if r.areCustomResourceLimitsDefined() {
		kubegres.Spec.Resources = r.kubegresRestoreContext.KubegresRestore.Spec.Resources
	} else {
		kubegres.Spec.Resources = r.kubegresRestoreContext.SourceKubegresClusterSpec.Resources
	}

	err := r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, kubegres)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("KubegresDeploymentErr", err, "Unable to deploy kubegres resource.")
		return err
	}

	r.kubegresRestoreContext.Log.InfoEvent("KubegresDeployed", "Deployed kubegres resource", "Kubegres name", kubegres.Name)

	return nil
}

func (r *KubegresCountSpecEnforcer) addReplicasToKubegres() error {
	kubegres := r.restoreStates.Cluster.Kubegres
	kubegres.Spec.Replicas = r.kubegresRestoreContext.SourceKubegresClusterSpec.Replicas

	err := r.kubegresRestoreContext.Client.Update(r.kubegresRestoreContext.Ctx, kubegres)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("KubegresScaleErr", err, "Unable to scale kubegres resource.")
		return err
	}

	r.kubegresRestoreContext.Log.InfoEvent("KubegresReplicasAdded", "Added replicas to kubegres resource", "Kubegres name", kubegres.Name)
	return nil
}

func (r *KubegresCountSpecEnforcer) areCustomResourceLimitsDefined() bool {
	restoreSpec := r.kubegresRestoreContext.KubegresRestore.Spec
	return restoreSpec.Resources.Requests != nil || restoreSpec.Resources.Limits != nil
}

func (r *KubegresCountSpecEnforcer) modifyResourceLimitsOfKubegres() error {
	kubegres := r.restoreStates.Cluster.Kubegres
	kubegres.Spec.Resources = r.kubegresRestoreContext.SourceKubegresClusterSpec.Resources

	return r.kubegresRestoreContext.Client.Update(r.kubegresRestoreContext.Ctx, kubegres)
}

func (r *KubegresCountSpecEnforcer) isClusterDeployed() bool {
	return r.restoreStates.Cluster.IsDeployed
}

func (r *KubegresCountSpecEnforcer) isJobCompleted() bool {
	return r.restoreStates.Job.JobPhase == states.JobSucceded
}

func (r *KubegresCountSpecEnforcer) kubegresHasReplicas() bool {
	return r.kubegresRestoreContext.SourceKubegresClusterSpec.Replicas != nil
}
