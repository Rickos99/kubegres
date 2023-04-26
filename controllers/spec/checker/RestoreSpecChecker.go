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

package checker

import (
	"errors"
	"reflect"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	kubegresv1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/states"
)

type RestoreSpecChecker struct {
	kubegresRestoreContext ctx.KubegresRestoreContext
	restoreResourceStates  states.RestoreResourceStates
}

func CreateRestoreSpecChecker(kubegresRestoreContext ctx.KubegresRestoreContext, restoreResourceStates states.RestoreResourceStates) RestoreSpecChecker {
	return RestoreSpecChecker{
		kubegresRestoreContext: kubegresRestoreContext,
		restoreResourceStates:  restoreResourceStates,
	}
}

func (r *RestoreSpecChecker) CheckSpec() (SpecCheckResult, error) {
	specCheckResult := SpecCheckResult{}

	spec := &r.kubegresRestoreContext.KubegresRestore.Spec

	if !r.isRestoreJobPvcDeployed() {
		specCheckResult.HasSpecFatalError = true
		specCheckResult.FatalErrorMessage = r.logSpecErrMsg("In the Resources Spec the value of " +
			"'spec.DataSource.File.PvcName' has a PersistentVolumeClaim name which is not deployed. Please deploy this " +
			"PersistentVolumeClaim, otherwise this operator cannot work correctly.")
	}

	if spec.DataSource.File.Mountpath == "" {
		specCheckResult.HasSpecFatalError = true
		specCheckResult.FatalErrorMessage = r.createErrMsgSpecUndefined("spec.DataSource.File.Mountpath")
	}

	if spec.DataSource.File.Snapshot == "" {
		specCheckResult.HasSpecFatalError = true
		specCheckResult.FatalErrorMessage = r.createErrMsgSpecUndefined("spec.DataSource.File.Snapshot")
	}

	if r.restoreResourceStates.Cluster.IsDeployed && !r.isDeployedClusterMangedByKubegresRestore() {
		specCheckResult.HasSpecFatalError = true
		specCheckResult.FatalErrorMessage = r.logSpecErrMsg("In the Resources Spec the value of " +
			"'spec.ClusterName' must not refer to an existing Kubegres resource. Please change this value, " +
			"otherwise the restore process cannot proceed.")
	}

	if r.isClusterNameAndClusterSpecDefined() {
		specCheckResult.HasSpecFatalError = true
		specCheckResult.FatalErrorMessage = r.logSpecErrMsg("In the Resources Spec the fields " +
			"'spec.DataSource.Cluster.ClusterName' and 'spec.DataSource.Cluster.ClusterSpec'" +
			" cannot be used at the same time. Please unset one of them.")
	}

	if r.kubegresRestoreContext.ShouldRestoreFromExistingCluster() {
		isDataSourceKubegresClusterDeployed, err := r.isDataSourceKubegresClusterDeployed()
		if err != nil {
			return specCheckResult, err
		}

		if !isDataSourceKubegresClusterDeployed {
			specCheckResult.HasSpecFatalError = true
			specCheckResult.FatalErrorMessage = r.logSpecErrMsg("In the Resources Spec the value of " +
				"'spec.DataSource.Cluster.ClusterName' refers to a Kubegres resource which is not deployed. Please deploy this " +
				"Kubegres resource, otherwise this operator cannot work correctly.")
		}
	} else {
		// TODO Check 'spec.DataSource.Cluster.ClusterSpec'
	}

	if spec.CustomConfig != "" {
		isCustomConfigDeployed, err := r.isCustomConfigDeployed()
		if err != nil {
			return specCheckResult, err
		}

		if !isCustomConfigDeployed {
			specCheckResult.HasSpecFatalError = true
			specCheckResult.FatalErrorMessage = r.logSpecErrMsg("In the Resources Spec the value of " +
				"'spec.CustomConfig' refers to a ConfigMap which is not deployed. Please deploy this " +
				"ConfigMap, otherwise this operator cannot work correctly.")
		}
	}

	if spec.DatabaseName == "" {
		specCheckResult.HasSpecFatalError = true
		specCheckResult.FatalErrorMessage = r.createErrMsgSpecUndefined("spec.DatabaseName")
	}

	return specCheckResult, nil
}

func (r *RestoreSpecChecker) isRestoreJobPvcDeployed() bool {
	return r.restoreResourceStates.Job.IsPvcDeployed
}

func (r *RestoreSpecChecker) isDeployedClusterMangedByKubegresRestore() bool {
	return r.restoreResourceStates.Cluster.IsDeployed && r.restoreResourceStates.Cluster.IsManagedByKubegresRestore
}

func (r *RestoreSpecChecker) isClusterNameAndClusterSpecDefined() bool {
	clusterSpec := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.Cluster.ClusterSpec
	emptyClusterSpec := kubegresv1.KubegresSpec{}

	clusterSpecIsSpecified := !reflect.DeepEqual(clusterSpec, emptyClusterSpec)
	clusterNameIsSpecified := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.Cluster.ClusterName != ""

	return clusterSpecIsSpecified && clusterNameIsSpecified
}

func (r *RestoreSpecChecker) isDataSourceKubegresClusterDeployed() (bool, error) {
	kubegresCluster := &kubegresv1.Kubegres{}
	kubegresClusterName := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.Cluster.ClusterName
	kubegresClusterKey := r.kubegresRestoreContext.GetNamespacesresourceName(kubegresClusterName)

	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, kubegresClusterKey, kubegresCluster)

	if err != nil {
		if apierrors.IsNotFound(err) {
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("GetKubegresSpecError", err, "Unable to get kubegres spec.")
		}
	}

	kubegresClusterIsDeployed := kubegresCluster.Name != ""
	return kubegresClusterIsDeployed, nil
}

func (r *RestoreSpecChecker) isCustomConfigDeployed() (bool, error) {
	customConfig := &core.ConfigMap{}
	customConfigName := r.kubegresRestoreContext.KubegresRestore.Spec.CustomConfig
	customConfigKey := r.kubegresRestoreContext.GetNamespacesresourceName(customConfigName)

	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, customConfigKey, customConfig)

	if err != nil {
		if apierrors.IsNotFound(err) {
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("GetCustomConfigError", err, "Unable to get custom config.", "ConfigMap", customConfigKey)
		}
	}

	customConfigIsDeployed := customConfig.Name != ""
	return customConfigIsDeployed, nil
}

func (r *RestoreSpecChecker) logSpecErrMsg(errorMsg string) string {
	r.kubegresRestoreContext.Log.ErrorEvent("SpecCheckErr", errors.New(errorMsg), "")
	return errorMsg
}

func (r *RestoreSpecChecker) createErrMsgSpecUndefined(specName string) string {
	errorMsg := "In the Resources Spec the value of '" + specName + "' is undefined. Please set a value otherwise this operator cannot work correctly."
	return r.logSpecErrMsg(errorMsg)
}
