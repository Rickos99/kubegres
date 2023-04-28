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

type JobCountSpecEnforcer struct {
	kubegresRestoreContext ctx.KubegresRestoreContext
	restoreStates          states.RestoreResourceStates
	resourcesCreator       template.RestoreJobResourcesCreatorTemplate
	kubegresSpec           kubegresv1.KubegresSpec
}

func CreateJobCountSpecEnforcer(kubegresRestoreContext ctx.KubegresRestoreContext,
	restoreStates states.RestoreResourceStates,
	kubegresSpec kubegresv1.KubegresSpec) JobCountSpecEnforcer {

	resourcesCreator := template.CreateRestoreJobCreator(kubegresRestoreContext)
	return JobCountSpecEnforcer{
		kubegresRestoreContext: kubegresRestoreContext,
		restoreStates:          restoreStates,
		resourcesCreator:       resourcesCreator,
		kubegresSpec:           kubegresSpec,
	}
}

func (r *JobCountSpecEnforcer) EnforceSpec() error {
	if r.isJobCompleted() || r.isJobDeployed() || r.isSnapshotFoundInPVC() {
		return nil
	}

	if r.isClusterReady() {
		return r.deployRestoreJob()
	}
	return nil
}

func (r *JobCountSpecEnforcer) deployRestoreJob() error {
	restoreJobTemplate, err := r.resourcesCreator.CreateRestoreJob(r.kubegresSpec)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("JobTemplateErr", err, "Unable to create restore job object from template.")
		return err
	}

	err = r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, &restoreJobTemplate)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("JobDeploymentErr", err, "Unable to deploy restore job.")
		return err
	}

	r.kubegresRestoreContext.Log.InfoEvent("JobDeployment", "Deployed restore job.", "Job name", restoreJobTemplate.Name)
	return nil
}

func (r *JobCountSpecEnforcer) isClusterReady() bool {
	return r.restoreStates.Cluster.IsReady
}

func (r *JobCountSpecEnforcer) isJobCompleted() bool {
	return r.restoreStates.Job.JobPhase == states.JobSucceded
}

func (r *JobCountSpecEnforcer) isJobDeployed() bool {
	return r.restoreStates.Job.IsJobDeployed
}

func (r *JobCountSpecEnforcer) isSnapshotFoundInPVC() bool {
	return r.restoreStates.FileChecker.ExitStatus != states.OkExitStatus
}
