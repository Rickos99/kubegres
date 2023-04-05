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

package states

import (
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"reactive-tech.io/kubegres/controllers/ctx"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type JobPhase string

const (
	JobPending  JobPhase = "Pending"
	JobRunning  JobPhase = "Running"
	JobSucceded JobPhase = "Succeded"
	JobFailed   JobPhase = "Failed"
)

type RestoreJobStates struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	IsJobDeployed bool
	IsPvcDeployed bool
	JobPhase      JobPhase

	Job *batchv1.Job
}

func loadRestoreJobStates(kubegresRestoreContext ctx.KubegresRestoreContext) (RestoreJobStates, error) {
	restoreJobStates := RestoreJobStates{kubegresRestoreContext: kubegresRestoreContext}

	if err := restoreJobStates.loadStates(); err != nil {
		return restoreJobStates, err
	}

	return restoreJobStates, nil
}

func (r *RestoreJobStates) loadStates() (err error) {
	r.Job, err = r.getRestoreJobResource()
	if err != nil {
		return err
	}

	pvc, err := r.getPvcResource()
	if err != nil {
		return err
	}

	r.IsPvcDeployed = pvc.Name != ""

	if r.Job.Name == "" {
		r.IsJobDeployed = false
		r.JobPhase = JobPending

		return nil
	}

	r.IsJobDeployed = true

	jobIsRunnning := r.Job.Status.Active != 0
	jobHasSucceded := r.Job.Status.Succeeded != 0
	jobHasFailed := r.Job.Status.Failed != 0

	if jobIsRunnning {
		r.JobPhase = JobRunning
		r.kubegresRestoreContext.Status.SetIsCompleted(false)
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobIsRunning)
	} else if jobHasSucceded {
		r.JobPhase = JobSucceded

		if !r.kubegresRestoreContext.Status.GetIsCompleted() {
			r.kubegresRestoreContext.Log.InfoEvent("RestoreJobCompleted", "Restorejob has completed succesfully.")
		}

		r.kubegresRestoreContext.Status.SetIsCompleted(true)
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobIsCompleted)
	} else if jobHasFailed {
		r.JobPhase = JobFailed
		r.kubegresRestoreContext.Status.SetIsCompleted(false)
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobFailed)

		jobPod, err := r.getRestoreJobPod()
		if err != nil {
			r.kubegresRestoreContext.Log.ErrorEvent("FailedToGetExitCodeOfFailedPod", err, "Unable to get exit code of failed restore job.", "Name of job", r.Job.Name)
			return err
		}

		jobExitCode := r.getExitCodeFromFailedJob(jobPod)
		r.kubegresRestoreContext.Log.InfoEvent("RestoreJobFailed", "Unable to complete restore job, exit code "+strconv.Itoa(int(jobExitCode))+". See 'pod/"+jobPod.Name+"' for more details.", "Name of job", r.Job.Name)
	}

	return nil
}

func (r *RestoreJobStates) getRestoreJobResource() (*batchv1.Job, error) {
	restoreJob := &batchv1.Job{}
	resourceName := r.kubegresRestoreContext.GetRestoreJobName()
	resourceNamespace := r.kubegresRestoreContext.KubegresRestore.Namespace
	restoreJobKey := types.NamespacedName{
		Namespace: resourceNamespace,
		Name:      resourceName,
	}

	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, restoreJobKey, restoreJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("RestoreJobLoadingErr", err, "Unable to load deployed restore job.", "RestoreJob Name", restoreJobKey)
		}
	}

	return restoreJob, err
}

func (r *RestoreJobStates) getRestoreJobPod() (*core.Pod, error) {
	restorePodList := &core.PodList{}
	jobName := r.kubegresRestoreContext.GetRestoreJobName()
	opts := []client.ListOption{
		client.InNamespace(r.kubegresRestoreContext.KubegresRestore.Namespace),
		client.MatchingLabels{"job-name": jobName},
	}
	err := r.kubegresRestoreContext.Client.List(r.kubegresRestoreContext.Ctx, restorePodList, opts...)

	if err != nil {
		if apierrors.IsNotFound(err) {
			r.kubegresRestoreContext.Log.Info("Restore job has not deployed any pods yet.", "Job name", jobName)
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("RestoreJobPodLoadingErr", err, "Unable to load any pods owned by restore job.", "Job name", jobName)
		}
	}

	if len(restorePodList.Items) == 0 {
		return &core.Pod{}, err
	}
	return &restorePodList.Items[0], err
}

func (r *RestoreJobStates) getPvcResource() (*core.PersistentVolumeClaim, error) {
	pvc := &core.PersistentVolumeClaim{}
	resourceName := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.File.PvcName
	resourceNamespace := r.kubegresRestoreContext.KubegresRestore.Namespace
	pvcKey := types.NamespacedName{
		Namespace: resourceNamespace,
		Name:      resourceName,
	}

	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, pvcKey, pvc)

	if err != nil {
		if apierrors.IsNotFound(err) {
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("RestoreJobPvcLoadingErr", err, "Unable to load deployed restore job PVC.", "PVC Name", pvcKey)
		}
	}

	return pvc, err
}

func (r *RestoreJobStates) getExitCodeFromFailedJob(jobPod *core.Pod) int32 {
	return jobPod.Status.ContainerStatuses[0].State.Terminated.ExitCode
}
