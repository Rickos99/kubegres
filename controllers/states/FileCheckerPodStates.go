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
	"strings"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"reactive-tech.io/kubegres/controllers/ctx"
)

type ExitStatus string

const (
	OkExitStatus           ExitStatus = "File exists"
	FileNotFoundExitStatus ExitStatus = "File not found"
	UndefinedExitStatus    ExitStatus = "Undefined"
)

type FileCheckerPodStates struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	IsPodDeployed       bool
	IsPodTerminated     bool
	ExitStatus          ExitStatus
	MostRecentSnapshots []string

	Pod *core.Pod
}

func loadFileCheckerPodStates(kubegresRestoreContext ctx.KubegresRestoreContext) (FileCheckerPodStates, error) {
	fileCheckerPodStates := FileCheckerPodStates{kubegresRestoreContext: kubegresRestoreContext}
	fileCheckerPodStates.loadStates()
	return fileCheckerPodStates, nil
}

func (r *FileCheckerPodStates) loadStates() (err error) {
	r.Pod, err = r.getFileCheckerPodResource()
	if err != nil {
		return err
	}

	if r.Pod.Name == "" {
		r.IsPodDeployed = false
		r.IsPodTerminated = false

		return nil
	}

	r.IsPodDeployed = true
	r.IsPodTerminated = r.Pod.Status.ContainerStatuses[0].State.Terminated != nil

	if r.IsPodDeployed && r.IsPodTerminated {
		// Get exit code
		exitCode := r.getExitCodeFromContainer()

		if exitCode == 0 {
			r.ExitStatus = OkExitStatus
			// r.kubegresRestoreContext.Status.SetSnapshotStatus(OkExitStatus)
		} else {
			r.ExitStatus = FileNotFoundExitStatus
			r.MostRecentSnapshots = r.getMostRecentSnapshots()
			// r.kubegresRestoreContext.Status.SetSnapshotStatus(FileNotFoundExitStatus)
			fileSource := r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.File
			r.kubegresRestoreContext.Log.Info("Unable to find snapshot '" + fileSource.Snapshot + "' in PVC '" + fileSource.PvcName + "'")
		}
	} else {
		r.ExitStatus = UndefinedExitStatus
		r.kubegresRestoreContext.Status.SetCurrentStage(ctx.StageCheckingSnapshotFile)
	}

	return nil
}

func (r *FileCheckerPodStates) getFileCheckerPodResource() (*core.Pod, error) {
	fileCheckerPod := &core.Pod{}
	fileCheckerPodKey := r.kubegresRestoreContext.GetNamespacesresourceName(r.kubegresRestoreContext.GetFileCheckerPodName())

	err := r.kubegresRestoreContext.Client.Get(r.kubegresRestoreContext.Ctx, fileCheckerPodKey, fileCheckerPod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			err = nil
		} else {
			r.kubegresRestoreContext.Log.ErrorEvent("FileCheckerLoadingErr", err, "Unable to load deployed pod to check if snapshot file exists.", "FileChecker Pod Name", fileCheckerPodKey.Name)
		}
	}

	return fileCheckerPod, err
}

func (r *FileCheckerPodStates) getExitCodeFromContainer() int32 {
	return r.Pod.Status.ContainerStatuses[0].State.Terminated.ExitCode
}

func (r *FileCheckerPodStates) getMostRecentSnapshots() []string {
	// The termination message contains information about most recent snapshots in the PVC.
	rawMessage := r.Pod.Status.ContainerStatuses[0].State.Terminated.Message
	if rawMessage == "" {
		return make([]string, 0)
	}

	files := strings.Split(rawMessage, "\n")
	if files[len(files)-1] == "" {
		return files[:len(files)-1]
	}
	return files
}
