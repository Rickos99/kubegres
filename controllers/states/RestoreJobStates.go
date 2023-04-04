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
	"errors"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"reactive-tech.io/kubegres/controllers/states/statefulset"
)

// TODO: Refactor
type RestoreJobStates struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	IsClusterDeployed bool
	IsClusterReady    bool
	Cluster           *v1.Kubegres

	IsJobDeployed  bool
	IsJobRunning   bool
	IsJobCompleted bool

	// Stage string
}

func LoadRestoreJobStates(kubegresRestoreContext ctx.KubegresRestoreContext) (RestoreJobStates, error) {
	restoreJobStates := RestoreJobStates{kubegresRestoreContext: kubegresRestoreContext}

	if err := restoreJobStates.loadClusterStates(); err != nil {
		return restoreJobStates, err
	}

	if err := restoreJobStates.loadJobStates(); err != nil {
		return restoreJobStates, err
	}

	return restoreJobStates, nil
}

func (r *RestoreJobStates) loadJobStates() error {
	restoreJob := &batchv1.Job{}
	jobKey := types.NamespacedName{
		Namespace: r.kubegresRestoreContext.KubegresRestore.Namespace,
		Name:      r.kubegresRestoreContext.GetRestoreJobName(),
	}
	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, jobKey, restoreJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			r.IsJobDeployed = false
			return nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("RestoreJobLoadingErr", err, "Unable to load deployed restore job", "KubegresRestore Name", jobKey)
			return err
		}
	}

	r.kubegresRestoreContext.Log.WithValues("JobStatus", restoreJob.Status)

	jobIsRunnning := restoreJob.Status.Active != 0
	jobHasSucceded := restoreJob.Status.Succeeded != 0

	r.IsJobDeployed = true
	r.IsJobRunning = jobIsRunnning
	r.IsJobCompleted = !jobIsRunnning && jobHasSucceded

	r.kubegresRestoreContext.Status.SetIsCompleted(r.IsJobCompleted)

	if r.IsJobRunning {
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobIsRunning)
	} else if r.IsJobCompleted {
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobIsCompleted)
	} else if r.IsJobDeployed && !r.IsJobRunning && !r.IsJobCompleted {
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageRestoreJobFailed)
		r.kubegresRestoreContext.Log.ErrorEvent("RestoreJobFailed", errors.New("unable to complete restore job"), "Unable to complete restore job", "Name of job", restoreJob.Name)
	}

	return nil
}

func (r *RestoreJobStates) loadClusterStates() error {
	r.Cluster = &v1.Kubegres{}
	clusterKey := types.NamespacedName{
		Namespace: r.kubegresRestoreContext.KubegresRestore.Namespace,
		Name:      r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName,
	}
	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, clusterKey, r.Cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.IsClusterDeployed = false
			r.IsClusterReady = false

			r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageDeployingCluster)
			return nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("KubegresClusterLoadingErr", err, "Unable to load deployed kubegres cluster", "KubegresCluster", clusterKey)
			return err
		}
	}

	kubegresLogwrapper := log.LogWrapper[*v1.Kubegres]{Resource: r.Cluster, Logger: r.kubegresRestoreContext.Log.Logger, Recorder: r.kubegresRestoreContext.Log.Recorder}
	kubegresStatusWrapper := &status.KubegresStatusWrapper{
		Kubegres: r.Cluster,
		Ctx:      r.kubegresRestoreContext.Ctx,
		Log:      kubegresLogwrapper,
		Client:   r.kubegresRestoreContext.Client,
	}
	kubegresContext := ctx.KubegresContext{
		Kubegres: r.Cluster,
		Status:   kubegresStatusWrapper,
		Ctx:      r.kubegresRestoreContext.Ctx,
		Log:      kubegresLogwrapper,
		Client:   r.kubegresRestoreContext.Client,
	}

	statefulSetStates, err := statefulset.LoadStatefulSetsStates(kubegresContext)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("StatefulSetInKubegresLoadingErr", err, "Unable to load state of deployed StatefulSets in Kubegres cluster", "Kubegres name", r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName)
		return err
	}

	serviceStates, err := loadServicesStates(kubegresContext)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("ServiceInKubegresLoadingErr", err, "Unable to load state of deployed service in Kubegres cluster", "Kubegres name", r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName)
		return err
	}

	r.IsClusterDeployed = true
	r.IsClusterReady = statefulSetStates.Primary.IsReady && serviceStates.Primary.IsDeployed

	if !r.IsClusterReady {
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageWaitingForCluster)
	}

	return nil
}
