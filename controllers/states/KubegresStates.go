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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"reactive-tech.io/kubegres/controllers/states/statefulset"
)

type KubegresStates struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	IsDeployed                 bool
	IsReady                    bool
	IsManagedByKubegresRestore bool

	Kubegres *v1.Kubegres
}

func loadKubegresStates(kubegresRestoreContext ctx.KubegresRestoreContext) (KubegresStates, error) {
	kubegresStates := KubegresStates{kubegresRestoreContext: kubegresRestoreContext}
	err := kubegresStates.loadStates()
	return kubegresStates, err
}

func (r *KubegresStates) loadStates() (err error) {
	r.Kubegres, err = r.getKubegresResource()
	if err != nil {
		return err
	}

	if r.Kubegres.Name == "" {
		r.IsDeployed = false
		r.IsReady = false
		r.IsManagedByKubegresRestore = false
		return nil
	}

	kubegresContext := r.createKubegresContext()

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

	r.IsDeployed = true
	r.IsReady = statefulSetStates.Primary.IsReady && serviceStates.Primary.IsDeployed
	r.IsManagedByKubegresRestore = r.isKubegresManagedByKubegresRestore()

	if !r.IsReady {
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageWaitingForCluster)
	}

	return nil
}

func (r *KubegresStates) getKubegresResource() (*v1.Kubegres, error) {
	kubegres := &v1.Kubegres{}
	resourceName := r.kubegresRestoreContext.KubegresRestore.Spec.ClusterName
	resourceNamespace := r.kubegresRestoreContext.KubegresRestore.Namespace
	clusterKey := types.NamespacedName{
		Namespace: resourceNamespace,
		Name:      resourceName,
	}

	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, clusterKey, kubegres)

	if err != nil {
		if apierrors.IsNotFound(err) {
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("KubegresClusterLoadingErr", err, "Unable to load deployed kubegres cluster", "KubegresCluster", clusterKey)
		}
	}

	return kubegres, err
}

func (r *KubegresStates) createKubegresContext() ctx.KubegresContext {
	kubegresLogwrapper := log.LogWrapper[*v1.Kubegres]{Resource: r.Kubegres, Logger: r.kubegresRestoreContext.Log.Logger, Recorder: r.kubegresRestoreContext.Log.Recorder}
	kubegresStatusWrapper := &status.KubegresStatusWrapper{
		Kubegres: r.Kubegres,
		Ctx:      r.kubegresRestoreContext.Ctx,
		Log:      kubegresLogwrapper,
		Client:   r.kubegresRestoreContext.Client,
	}
	kubegresContext := ctx.KubegresContext{
		Kubegres: r.Kubegres,
		Status:   kubegresStatusWrapper,
		Ctx:      r.kubegresRestoreContext.Ctx,
		Log:      kubegresLogwrapper,
		Client:   r.kubegresRestoreContext.Client,
	}
	return kubegresContext
}

func (r *KubegresStates) isKubegresManagedByKubegresRestore() bool {
	label, exists := r.Kubegres.Labels[ctx.ManagedByKubegresRestoreLabel]
	if exists {
		return label == r.kubegresRestoreContext.KubegresRestore.Name
	}
	return false
}
