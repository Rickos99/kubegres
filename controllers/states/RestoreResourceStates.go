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

import "reactive-tech.io/kubegres/controllers/ctx"

type RestoreResourceStates struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	Cluster     KubegresStates
	Job         RestoreJobStates
	FileChecker FileCheckerPodStates
}

func LoadRestoreResourceStates(kubegresRestoreContext ctx.KubegresRestoreContext) (RestoreResourceStates, error) {
	restoreResourceStates := RestoreResourceStates{kubegresRestoreContext: kubegresRestoreContext}
	err := restoreResourceStates.loadStates()
	return restoreResourceStates, err
}

func (r *RestoreResourceStates) loadStates() (err error) {
	err = r.loadClusterStates()
	if err != nil {
		return err
	}

	err = r.loadJobStates()
	if err != nil {
		return err
	}

	err = r.loadFileCheckerStates()
	if err != nil {
		return err
	}

	return nil
}

func (r *RestoreResourceStates) loadClusterStates() (err error) {
	r.Cluster, err = loadKubegresStates(r.kubegresRestoreContext)
	return err
}

func (r *RestoreResourceStates) loadJobStates() (err error) {
	r.Job, err = loadRestoreJobStates(r.kubegresRestoreContext)
	return err
}

func (r *RestoreResourceStates) loadFileCheckerStates() (err error) {
	r.FileChecker, err = loadFileCheckerPodStates(r.kubegresRestoreContext)
	return err
}
