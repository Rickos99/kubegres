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

package states

import (
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"reactive-tech.io/kubegres/controllers/ctx"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DebugPodsStates struct {
	NbreDeployed             int32
	SpecExpectedNbreToDeploy int32
	Pods                     []DebugPodWrapper
	kubegresContext          ctx.KubegresContext
}

type DebugPodWrapper struct {
	IsDeployed bool
	IsReady    bool
	IsStuck    bool
	Pod        core.Pod
}

func loadDebugPodsStates(kubegresContext ctx.KubegresContext) (DebugPodsStates, error) {
	DebugPodsStates := DebugPodsStates{kubegresContext: kubegresContext}
	err := DebugPodsStates.loadStates()
	return DebugPodsStates, err
}

func (r *DebugPodsStates) loadStates() (err error) {
	deployedDebugPods, err := r.getDeployedDebugPods()
	if err != nil {
		return err
	}

	r.NbreDeployed = int32(len(deployedDebugPods.Items))
	r.SpecExpectedNbreToDeploy = r.kubegresContext.Kubegres.Spec.DebugPods

	for _, debugPod := range deployedDebugPods.Items {
		isDebugPodReady := r.isDebugPodReady(debugPod)
		isDebugPodStuck := r.isDebugPodStuck(debugPod)

		debugPodWrapper := DebugPodWrapper{
			IsDeployed: true,
			IsReady:    isDebugPodReady && !isDebugPodStuck,
			IsStuck:    isDebugPodStuck,
			Pod:        debugPod,
		}

		r.Pods = append(r.Pods, debugPodWrapper)
	}

	return nil
}

func (r *DebugPodsStates) getDeployedDebugPods() (*core.PodList, error) {
	list := &core.PodList{}
	opts := []client.ListOption{
		client.InNamespace(r.kubegresContext.Kubegres.Namespace),
		client.MatchingLabels{
			"app":  r.kubegresContext.Kubegres.Name,
			"role": "debug",
		},
	}
	err := r.kubegresContext.Client.List(r.kubegresContext.Ctx, list, opts...)

	if err != nil {
		if apierrors.IsNotFound(err) {
			r.kubegresContext.Log.Info("There is not any deployed Pods yet", "Kubegres name", r.kubegresContext.Kubegres.Name)
			err = nil
		} else {
			r.kubegresContext.Log.ErrorEvent("PodLoadingErr", err, "Unable to load any deployed Pods.", "Kubegres name", r.kubegresContext.Kubegres.Name)
		}
	}

	return list, err
}

func (r *DebugPodsStates) isDebugPodReady(debugPod core.Pod) bool {
	if len(debugPod.Status.ContainerStatuses) == 0 {
		return false
	}
	return debugPod.Status.ContainerStatuses[0].Ready
}

func (r *DebugPodsStates) isDebugPodStuck(debugPod core.Pod) bool {
	if len(debugPod.Status.ContainerStatuses) == 0 ||
		debugPod.Status.ContainerStatuses[0].State.Waiting == nil {
		return false
	}

	waitingReason := debugPod.Status.ContainerStatuses[0].State.Waiting.Reason
	if waitingReason == "CrashLoopBackOff" || waitingReason == "Error" {
		r.kubegresContext.Log.Info("POD is waiting", "Reason", waitingReason)
		return true
	}

	return false
}
