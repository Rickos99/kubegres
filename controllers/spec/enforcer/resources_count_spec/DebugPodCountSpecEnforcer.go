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
	"strconv"

	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/spec/template"
	"reactive-tech.io/kubegres/controllers/states"
)

type DebugPodCountSpecEnforcer struct {
	kubegresContext  ctx.KubegresContext
	resourcesStates  states.ResourcesStates
	resourcesCreator template.ResourcesCreatorFromTemplate
}

func CreateDebugPodCountSpecEnforcer(kubegresContext ctx.KubegresContext,
	resourcesStates states.ResourcesStates,
	resourcesCreator template.ResourcesCreatorFromTemplate) DebugPodCountSpecEnforcer {

	return DebugPodCountSpecEnforcer{
		kubegresContext:  kubegresContext,
		resourcesStates:  resourcesStates,
		resourcesCreator: resourcesCreator,
	}
}

func (r *DebugPodCountSpecEnforcer) EnforceSpec() error {
	nbreDebugPodsToDeploy := r.kubegresContext.Kubegres.Spec.DebugPods - r.resourcesStates.DebugPods.NbreDeployed

	if nbreDebugPodsToDeploy > 0 {
		return r.deployDebugPods(nbreDebugPodsToDeploy)
	} else if nbreDebugPodsToDeploy < 0 {
		return r.undeployDebugPods(nbreDebugPodsToDeploy * (-1))
	} else if nbreDebugPodsToDeploy == 0 {
		return nil
	}

	return nil
}

func (r *DebugPodCountSpecEnforcer) deployDebugPods(nbreDebugPodsToDeploy int32) error {
	for i := int32(0); i < nbreDebugPodsToDeploy; i++ {
		debugPod, err := r.resourcesCreator.CreateDebugPod()
		if err != nil {
			r.kubegresContext.Log.ErrorEvent("DebugPodTemplateErr", err, "Error while creating a debugPod object from template.")
			return err
		}

		// r.kubegresContext.Log.Info(fmt.Sprintf("Deploying debugPod '%s' (%d of %d)", debugPod.Name, i, nbreDebugPodsToDeploy))
		r.kubegresContext.Log.Info("Deploying debugPod '" + debugPod.Name + "'")
		err = r.kubegresContext.Client.Create(r.kubegresContext.Ctx, &debugPod)
		if err != nil {
			r.kubegresContext.Log.ErrorEvent("DebugPodDeploymentErr", err, "Unable to deploy debugPod")
			return err
		}
	}

	r.kubegresContext.Status.SetEnforcedDebugPods((r.kubegresContext.Kubegres.Status.EnforcedDebugPods))
	r.kubegresContext.Log.InfoEvent("DebugPodDeployment", "Deployed "+strconv.Itoa(int(nbreDebugPodsToDeploy))+" pod(s)")
	return nil
}

func (r *DebugPodCountSpecEnforcer) undeployDebugPods(nbreDebugPodsToUndeploy int32) error {
	debugPodsToUndeploy := r.getDebugPodsToUndeploy(nbreDebugPodsToUndeploy)
	for _, debugPod := range debugPodsToUndeploy {
		r.kubegresContext.Log.Info("Deleting DebugPod", "name", debugPod.Pod.Name)
		err := r.kubegresContext.Client.Delete(r.kubegresContext.Ctx, &debugPod.Pod)

		if err != nil {
			r.kubegresContext.Log.ErrorEvent("DebugPodDeletionErr", err, "Unable to delete DebugPod.", "DebugPod name", debugPod.Pod.Name)
			return err
		}

		r.kubegresContext.Log.InfoEvent("DebugPodDeletion", "Deleted DebugPod.", "DebugPod name", debugPod.Pod.Name)
	}
	return nil
}

func (r *DebugPodCountSpecEnforcer) getDebugPodsToUndeploy(nbreDebugPodsToUndeploy int32) []states.DebugPodWrapper {
	if nbreDebugPodsToUndeploy > int32(len(r.resourcesStates.DebugPods.Pods)) {
		return r.resourcesStates.DebugPods.Pods[:len(r.resourcesStates.DebugPods.Pods)]
	}
	return r.resourcesStates.DebugPods.Pods[:nbreDebugPodsToUndeploy]
}
