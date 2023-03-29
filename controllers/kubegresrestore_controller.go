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

package controllers

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubegresv1 "reactive-tech.io/kubegres/api/v1"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx/resources"
)

// KubegresRestoreReconciler reconciles a KubegresRestore object
type KubegresRestoreReconciler struct {
	client.Client
	Logger   logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=kubegres.reactive-tech.io,resources=kubegresrestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kubegres.reactive-tech.io,resources=kubegresrestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kubegres.reactive-tech.io,resources=kubegresrestores/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *KubegresRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// _ = log.FromContext(ctx)

	r.Logger.Info("------------------------------------------")
	r.Logger.Info("------------------------------------------")

	// Allow Kubernetes to update its system.
	time.Sleep(1 * time.Second)

	// ### 1. Create restore context
	// Get KubegresRestore resource
	restoreJob, err := r.getDeployedKubegresRestoreResource(ctx, req)
	if err != nil {
		r.Logger.Info("KubegresRestore resource does not exist")
		return ctrl.Result{}, nil
	}

	r.Logger.Info("KubegresRestore", "Job", restoreJob, "Req", req)

	// // Get target Kubegres cluster based on the job spec
	// targetKubegresCluster, err := r.getDeployedKubegresResource(ctx, req, restoreJob.Spec.ClusterName)
	// if err != nil {
	// 	r.Logger.Info("Kubegres resource does not exist")
	// 	return ctrl.Result{}, nil
	// }

	// r.Logger.Info("KubegresRestore", "Job", restoreJob, "KubegresCluster", targetKubegresCluster)

	// Get RestoreContext
	restoreJobContext, err := resources.CreateRestoreJobContext(restoreJob, ctx, r.Logger, r.Client, r.Recorder)
	if err != nil {
		return ctrl.Result{}, err
	}

	// ### 2. Deploy cluster if it is not already
	if !restoreJobContext.RestoreJobStates.IsClusterDeployed {
		var kubegresSpec v1.KubegresSpec
		restoreFromZero := false // TODO: Get this value from KubegresRestore spec
		if restoreFromZero {
			// TODO: Implement createClusterSpec(...)
			//createClusterSpec(restoreJobContext.KubegresRestoreContext.KubegresRestore.Spec, kubegres)
		} else {
			if kubegresSpec, err = restoreJobContext.CreateKubegresSpecFromExistingCluster(); err != nil {
				restoreJobContext.LogWrapper.ErrorEvent("ErrorWhenCopyingClusterSpec", err, "Unable to copy spec of source Kubegres cluster")
				return r.returnn(ctrl.Result{}, err, restoreJobContext)
			}
		}

		restoreJobContext.LogWrapper.Logger.Info("Step 2", "KubegresRestore", restoreJob, "ClusterSpec", kubegresSpec)
		// if err := createClusterFromSpec(restoreJobContext, kubegresSpec); err != nil {
		// 	restoreJobContext.LogWrapper.ErrorEvent("ErrorWhenCreatingNewCluster", err, "Unable to create a new Kubegres cluster", "KubegresSpec", kubegresSpec)
		// 	return r.returnn(ctrl.Result{}, err, restoreJobContext)
		// }
	}

	// ### 3. Requeue if cluster is not ready
	if !restoreJobContext.RestoreJobStates.IsClusterReady {
		restoreJobContext.LogWrapper.Logger.Info("Kubegres cluster not ready yet", "RestoreJob", restoreJobContext.KubegresRestoreContext.KubegresRestore, "Status", restoreJobContext.RestoreStatusWrapper)
		return r.returnn(ctrl.Result{Requeue: true}, nil, restoreJobContext)
	} else {
		restoreJobContext.LogWrapper.Logger.Info("Kubegres cluster is ready", "RestoreJob", restoreJobContext.KubegresRestoreContext.KubegresRestore, "Status", restoreJobContext.RestoreStatusWrapper)
	}

	// ### 4. Update restore job status

	// ### 5. Deploy restore job if not already deployed

	// ### 6. If job is running, requeue and wait

	// ### 7. If job is completed, set replicas and update the cluster spec.

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KubegresRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubegresv1.KubegresRestore{}).
		Complete(r)
}

func (r *KubegresRestoreReconciler) returnn(result ctrl.Result,
	err error,
	resourcesContext *resources.RestoreJobContext) (ctrl.Result, error) {

	errStatusUpt := resourcesContext.KubegresRestoreContext.Status.UpdateStatusIfChanged()
	if errStatusUpt != nil && err == nil {
		return result, errStatusUpt
	}

	return result, err
}

func (r *KubegresRestoreReconciler) getDeployedKubegresRestoreResource(ctx context.Context, req ctrl.Request) (*kubegresv1.KubegresRestore, error) {
	restoreJob := &kubegresv1.KubegresRestore{}
	err := r.Client.Get(ctx, req.NamespacedName, restoreJob)
	if err != nil {
		return &kubegresv1.KubegresRestore{}, err
	}

	return restoreJob, nil
}

func (r *KubegresRestoreReconciler) getDeployedKubegresResource(ctx context.Context, req ctrl.Request, kubegresName string) (*kubegresv1.Kubegres, error) {
	kubegresNamespacedName := types.NamespacedName{
		Namespace: req.Namespace,
		Name:      kubegresName,
	}

	kubegresResource := &kubegresv1.Kubegres{}
	err := r.Client.Get(ctx, kubegresNamespacedName, kubegresResource)
	if err != nil {
		return &kubegresv1.Kubegres{}, err
	}

	return kubegresResource, nil
}
