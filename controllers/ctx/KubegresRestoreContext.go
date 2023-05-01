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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
)

type KubegresRestoreContext struct {
	KubegresRestore           *v1.KubegresRestore
	SourceKubegresClusterSpec v1.KubegresSpec
	Status                    *status.RestoreStatusWrapper
	Ctx                       context.Context
	Log                       log.LogWrapper[*v1.KubegresRestore]
	Client                    client.Client
}

const (
	KindKubegresRestore           = "KubegresRestore"
	RestoreJobSuffix              = "-job"
	RestoreJobKubegresTargetField = ".spec.clusterName"
	ManagedByKubegresRestoreLabel = "managed-by-kubegres-restore"
	FileCheckerPodSuffix          = "-file-checker"
)

const (
	StageCheckingSnapshotFile  = "Checking snapshot file"
	StageDeployingCluster      = "Deploying Kubegres Cluster"
	StageWaitingForCluster     = "Waiting for Kubegres Cluster to be ready"
	StageRestoreJobIsDeploying = "Waiting for restore job to deploy"
	StageRestoreJobIsRunning   = "Restoring database from snaphot"
	StageRestoreJobIsCompleted = "Restorejob completed succesfully"
	StageRestoreJobFailed      = "Restorejob has stopped due to fatal error"
)

func CreateKubegresRestoreContext(kubegresRestore *v1.KubegresRestore,
	status *status.RestoreStatusWrapper,
	ctx context.Context,
	log log.LogWrapper[*v1.KubegresRestore],
	client client.Client) (KubegresRestoreContext, error) {
	kubegresRestoreContext := KubegresRestoreContext{
		KubegresRestore: kubegresRestore,
		Status:          status,
		Ctx:             ctx,
		Log:             log,
		Client:          client,
	}
	sourceKubegresClusterSpec, err := kubegresRestoreContext.assignSourceKubegresCluserSpec(kubegresRestore)
	if err != nil {
		return kubegresRestoreContext, err
	}
	kubegresRestoreContext.SourceKubegresClusterSpec = sourceKubegresClusterSpec

	return kubegresRestoreContext, err
}

func (r *KubegresRestoreContext) GetFileCheckerPodName() string {
	return r.KubegresRestore.Name + FileCheckerPodSuffix
}

func (r *KubegresRestoreContext) GetRestoreJobName() string {
	return r.KubegresRestore.Name + RestoreJobSuffix
}

func (r *KubegresRestoreContext) GetNamespacesresourceName(name string) types.NamespacedName {
	return types.NamespacedName{
		Namespace: r.KubegresRestore.Namespace,
		Name:      name,
	}
}

func (r *KubegresRestoreContext) ShouldRestoreFromExistingCluster() bool {
	return r.KubegresRestore.Spec.DataSource.Cluster.ClusterName != ""
}

func (r *KubegresRestoreContext) AreResourcesSpecifiedForRestoreJob() bool {
	restoreSpec := r.KubegresRestore.Spec
	return restoreSpec.Resources.Requests != nil || restoreSpec.Resources.Limits != nil
}

func (r *KubegresRestoreContext) assignSourceKubegresCluserSpec(kubegresRestore *v1.KubegresRestore) (v1.KubegresSpec, error) {
	if r.ShouldRestoreFromExistingCluster() {
		return r.getKubegresSpecFromExistingCluster()
	} else {
		return r.KubegresRestore.Spec.DataSource.Cluster.ClusterSpec, nil
	}
}

func (r *KubegresRestoreContext) getKubegresSpecFromExistingCluster() (v1.KubegresSpec, error) {
	cluster := &v1.Kubegres{}
	clusterKey := r.GetNamespacesresourceName(r.KubegresRestore.Spec.DataSource.Cluster.ClusterName)
	err := r.Client.Get(r.Ctx, clusterKey, cluster)
	if err != nil && apierrors.IsNotFound(err) {
		r.Log.ErrorEvent("KubegresSpecFromExistingClusterErr", err, "Unable to get Kubegres specification from non-existing source cluster", "ClusterName", clusterKey.Name)
		err = nil
	}
	return cluster.Spec, err
}
