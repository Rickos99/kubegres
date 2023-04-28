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
	"errors"
	"path"
	"strings"

	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/spec/template"
	"reactive-tech.io/kubegres/controllers/states"
)

type FileCheckerPodCountSpecEnforcer struct {
	kubegresRestoreContext ctx.KubegresRestoreContext
	restoreStates          states.RestoreResourceStates
	resourcesCreator       template.RestoreJobResourcesCreatorTemplate
}

func CreateFileCheckerPodCountSpecEnforcer(kubegresRestoreContext ctx.KubegresRestoreContext,
	restoreStates states.RestoreResourceStates) FileCheckerPodCountSpecEnforcer {

	resourcesCreator := template.CreateRestoreJobCreator(kubegresRestoreContext)
	return FileCheckerPodCountSpecEnforcer{
		kubegresRestoreContext: kubegresRestoreContext,
		restoreStates:          restoreStates,
		resourcesCreator:       resourcesCreator,
	}
}

func (r *FileCheckerPodCountSpecEnforcer) EnforceSpec() error {
	if r.isFileCheckerPodDeployed() {
		if r.isThereSpecDifference() {
			// Delete current pod. Next reconcile will deploy pod with new spec.
			// This avoids triggering reconciliation multiple times
			return r.deleteFileCheckerPod()
		} else if r.isSnapshotFileNotFoundInPvc() {
			r.logSnapshotNotFoundErrorEvent()
		}
		return nil
	}

	return r.deployFileCheckerPod()
}

func (r *FileCheckerPodCountSpecEnforcer) deployFileCheckerPod() error {
	fileCheckerPodTemplate, err := r.resourcesCreator.CreateFileCheckerPod()
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("FileCheckerTemplateErr", err, "Unable to create file checker pod from template.")
		return err
	}

	err = r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, &fileCheckerPodTemplate)
	if err != nil {
		r.kubegresRestoreContext.Log.ErrorEvent("FileCheckerDeploymentErr", err, "Unable to deploy file checker.")
		return err
	}

	r.kubegresRestoreContext.Log.InfoEvent("FileCheckerDeployment", "Deployed file checker.")
	return nil
}

func (r *FileCheckerPodCountSpecEnforcer) deleteFileCheckerPod() error {
	return r.kubegresRestoreContext.Client.Delete(r.kubegresRestoreContext.Ctx, r.restoreStates.FileChecker.Pod)
}

func (r *FileCheckerPodCountSpecEnforcer) isThereSpecDifference() bool {
	return r.haveMountpathSpecChanged() || // Mountpath
		r.haveSnapshotSpecChanged() || // Snapshot file
		r.havePVCSpecChanged() // PVC name
}

func (r *FileCheckerPodCountSpecEnforcer) haveMountpathSpecChanged() bool {
	expected := r.restoreStates.FileChecker.Pod.Spec.Containers[0].VolumeMounts[0].MountPath
	actual := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.File.Mountpath
	return expected != actual
}

func (r *FileCheckerPodCountSpecEnforcer) haveSnapshotSpecChanged() bool {
	restoreSpec := r.kubegresRestoreContext.KubegresRestore.Spec
	expected := r.restoreStates.FileChecker.Pod.Spec.Containers[0].Env[0].Value
	actual := path.Join(restoreSpec.DataSource.File.Mountpath, restoreSpec.DataSource.File.Snapshot)
	return expected != actual
}

func (r *FileCheckerPodCountSpecEnforcer) havePVCSpecChanged() bool {
	expected := r.restoreStates.FileChecker.Pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
	actual := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.File.PvcName
	return expected != actual
}

func (r *FileCheckerPodCountSpecEnforcer) isFileCheckerPodDeployed() bool {
	return r.restoreStates.FileChecker.IsPodDeployed
}

func (r *FileCheckerPodCountSpecEnforcer) isSnapshotFileNotFoundInPvc() bool {
	return r.restoreStates.FileChecker.ExitStatus == states.FileNotFoundExitStatus
}

func (r *FileCheckerPodCountSpecEnforcer) logSnapshotNotFoundErrorEvent() {
	errorMsg := "In the Resources Spec the file specified by " +
		"'spec.DataSource.File.Snapshot' is not found. Please make sure the filename is correct."
	if len(r.restoreStates.FileChecker.MostRecentSnapshots) > 0 {
		errorMsg += " The most recent snapshots found are " +
			prettyStringFromFileArray(r.restoreStates.FileChecker.MostRecentSnapshots) +
			"."
	}
	r.kubegresRestoreContext.Log.ErrorEvent("SnapshotFileNotFoundErr", errors.New(errorMsg), "")
}

func prettyStringFromFileArray(filenames []string) string {
	quoted := make([]string, len(filenames))
	for i, elem := range filenames {
		quoted[i] = "'" + elem + "'"
	}
	return strings.Join(quoted, ", ")
}
