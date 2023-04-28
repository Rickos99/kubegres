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
	"reactive-tech.io/kubegres/controllers/spec/template"
	"reactive-tech.io/kubegres/controllers/states"
)

type KubegresCountSpecEnforcer struct {
	kubegresRestoreContext ctx.KubegresRestoreContext
	resourcesCreator       template.RestoreJobResourcesCreatorTemplate
	restoreStates          states.RestoreResourceStates
	targetKubegresSpec     kubegresv1.KubegresSpec
}

func CreateKubegresCountSpecEnforcer(kubegresRestoreContext ctx.KubegresRestoreContext,
	restoreStates states.RestoreResourceStates,
	targetKubegresSpec kubegresv1.KubegresSpec) KubegresCountSpecEnforcer {

	resourcesCreator := template.CreateRestoreJobCreator(kubegresRestoreContext)
	return KubegresCountSpecEnforcer{
		kubegresRestoreContext: kubegresRestoreContext,
		resourcesCreator:       resourcesCreator,
		restoreStates:          restoreStates,
		targetKubegresSpec:     targetKubegresSpec,
	}
}

func (r *KubegresCountSpecEnforcer) EnforceSpec() error {
	if r.isSnapshotFoundInPVC() {
		return nil
	}

	if !r.isClusterDeployed() {
		return r.deployKubegres()
	}

	if r.isJobCompleted() {
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobIsCompleted)
		return r.finalizeKubegres()
	}

	return nil
}

func (r *KubegresCountSpecEnforcer) deployKubegres() error {
	kubegresTemplate := r.resourcesCreator.CreateKubegresResource(r.targetKubegresSpec)
	err := r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, &kubegresTemplate)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("KubegresDeploymentErr", err, "Unable to deploy kubegres resource.")
		return err
	}

	r.kubegresRestoreContext.Log.InfoEvent("KubegresDeployed", "Deployed kubegres resource", "Kubegres name", kubegresTemplate.Name)

	return nil
}

func (r *KubegresCountSpecEnforcer) finalizeKubegres() error {
	kubegresIsChanged := false
	kubegres := r.restoreStates.Cluster.Kubegres
	if r.kubegresHasReplicas() {
		kubegresIsChanged = true
		kubegres.Spec.Replicas = r.kubegresRestoreContext.SourceKubegresClusterSpec.Replicas
	}

	if r.kubegresRestoreContext.AreResourcesSpecifiedForRestoreJob() {
		kubegresIsChanged = true
		kubegres.Spec.Resources = r.kubegresRestoreContext.SourceKubegresClusterSpec.Resources
	}

	if r.restoreStates.Cluster.IsManagedByKubegresRestore {
		kubegresIsChanged = true
		delete(kubegres.Labels, ctx.ManagedByKubegresRestoreLabel)
		r.kubegresRestoreContext.Log.InfoEvent("ReleasedKubegresResource", "Restore label of kubegres resource will be removed. No further changes will be applied", "Kubegres name", kubegres.Name)
	}

	if kubegresIsChanged {
		if err := r.kubegresRestoreContext.Client.Update(r.kubegresRestoreContext.Ctx, kubegres); err != nil {
			return err
		}
	}
	return nil
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

func (r *KubegresCountSpecEnforcer) isSnapshotFoundInPVC() bool {
	return r.restoreStates.FileChecker.ExitStatus != states.OkExitStatus
}
