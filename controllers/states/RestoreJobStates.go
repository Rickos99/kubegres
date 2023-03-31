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
	apps "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TODO: Refactor
type RestoreJobStates struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	IsClusterDeployed bool
	IsClusterReady    bool
	// Cluster           *v1.Kubegres

	IsJobDeployed  bool
	IsJobRunning   bool
	IsJobCompleted bool
	// Job            batchv1.Job

	Stage string
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
			r.Stage = "" //TODO: Replace with correct stage (constant)
			return nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("RestoreJobLoadingErr", err, "Unable to load deployed restore job", "KubegresRestore Name", jobKey)
			return err
		}
	}

	r.kubegresRestoreContext.Log.WithValues("JobStatus", restoreJob.Status)

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
		Namespace: r.kubegresRestoreContext.KubegresRestore.Namespace,
		Name:      r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName,
	}
	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, clusterKey, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.IsClusterDeployed = false
			r.IsClusterReady = false
			// r.Stage = "Cluster not deployed yet" //TODO: Replace with correct stage (constant)
			return nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("KubegresClusterLoadingErr", err, "Unable to load deployed kubegres cluster", "KubegresCluster", clusterKey)
			return err
		}
	}

	isStatefulSetReady, err := r.isStatefulSetReady()
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("StatefulSetInKubegresLoadingErr", err, "Unable to load any deployed StatefulSets in Kubegres cluster", "Kubegres name", r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName)
		return err
	}

	isPrimaryServiceReady, err := r.isPrimaryServiceReady()
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("PrimaryServiceInKubegresLoadingErr", err, "Unable to load any deployed primary service in Kubegres cluster", "Kubegres name", r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName)
		return err
	}

	r.IsClusterDeployed = true
	r.IsClusterReady = isStatefulSetReady && isPrimaryServiceReady

	if err != nil {
		return err
	}

	// if !r.IsClusterReady {
	// 	r.Stage = "Cluster is deploying" //TODO: Replace with correct stage (constant)
	// }

	return nil
}

func (r *RestoreJobStates) isStatefulSetReady() (bool, error) {
	statefulSets, err := r.getStatefulSetsOwnedByKubegres()
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	if len(statefulSets.Items) == 0 {
		return false, nil
	}

	isClusterReady := true
	for _, statefulSet := range statefulSets.Items {
		if statefulSet.Status.ReadyReplicas < 1 {
			isClusterReady = false
			break
		}
	}
	return isClusterReady, nil
}

func (r *RestoreJobStates) isPrimaryServiceReady() (bool, error) {
	_, err := r.getPrimaryServiceOwnedByKubegres()

	if err == nil {
		return true, nil
	}

	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

func (r *RestoreJobStates) getStatefulSetsOwnedByKubegres() (*apps.StatefulSetList, error) {
	clusterName := r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName
	list := &apps.StatefulSetList{}
	opts := []client.ListOption{
		client.InNamespace(r.kubegresRestoreContext.KubegresRestore.Namespace),
		client.MatchingFields{ctx.DeploymentOwnerKey: clusterName},
	}
	err := r.kubegresRestoreContext.Client.List(r.kubegresRestoreContext.Ctx, list, opts...)

	return list, err
}

func (r *RestoreJobStates) getPrimaryServiceOwnedByKubegres() (*core.Service, error) {
	serviceName := r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName
	service := &core.Service{}
	serviceKey := types.NamespacedName{
		Namespace: r.kubegresRestoreContext.KubegresRestore.Namespace,
		Name:      serviceName,
	}
	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, serviceKey, service)

	return service, err
}
