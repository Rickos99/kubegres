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
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
)

type RestoreJobStates struct {
	KubegresRestoreContext ctx.KubegresRestoreContext

	IsClusterDeployed bool
	IsClusterReady    bool
	Cluster           *v1.Kubegres

	IsJobDeployed  bool
	IsJobRunning   bool
	IsJobCompleted bool
	Job            batchv1.Job

	Stage string
}

func LoadRestoreJobStates(kubegresRestoreContext ctx.KubegresRestoreContext) (RestoreJobStates, error) {
	restoreJobStates := RestoreJobStates{KubegresRestoreContext: kubegresRestoreContext}

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
		Namespace: r.KubegresRestoreContext.KubegresRestore.Namespace,
		Name:      r.KubegresRestoreContext.GetRestoreJobName(),
	}
	err := r.KubegresRestoreContext.Client.Get(r.KubegresRestoreContext.Ctx, jobKey, restoreJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			r.IsJobDeployed = false
			r.Stage = "" //TODO: Replace with correct stage (constant)
			return nil
		} else {
			r.KubegresRestoreContext.Log.ErrorEvent("RestoreJobLoadingErr", err, "Unable to load deployed restore job", "KubegresRestore Name", jobKey)
			return err
		}
	}

	r.KubegresRestoreContext.Log.WithValues("JobStatus", restoreJob.Status)

	jobIsRunnning := restoreJob.Status.Active != 0     // TODO: Verify that only one Pod is created
	jobHasSucceded := restoreJob.Status.Succeeded != 0 // TODO: Verify that only one Pod is created

	r.IsJobDeployed = true
	r.IsJobRunning = jobIsRunnning
	r.IsJobCompleted = !jobIsRunnning && jobHasSucceded
	// r.Stage = "" //TODO: Replace with correct stage (constant)

	return nil
}

func (r *RestoreJobStates) loadClusterStates() error {
	cluster := &v1.Kubegres{}
	clusterKey := types.NamespacedName{
		Namespace: r.KubegresRestoreContext.KubegresRestore.Namespace,
		Name:      r.KubegresRestoreContext.GetRestoreJobName(),
	}
	err := r.KubegresRestoreContext.Client.Get(r.KubegresRestoreContext.Ctx, clusterKey, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.IsClusterDeployed = false
			r.IsClusterReady = false
			// r.Stage = "Cluster not deployed yet" //TODO: Replace with correct stage (constant)
			return nil
		} else {
			r.KubegresRestoreContext.Log.ErrorEvent("KubegresClusterLoadingErr", err, "Unable to load deployed kubegres cluster", "KubegresCluster", clusterKey)
			return err
		}
	}

	r.IsClusterDeployed = true
	r.IsClusterReady = cluster.Status.BlockingOperation.OperationId == "" //TODO: Replace with a better metric of make verify that blocking operation is never empty during deployment

	// if !r.IsClusterReady {
	// 	r.Stage = "Cluster is deploying" //TODO: Replace with correct stage (constant)
	// }

	return nil
}
