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
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kubegresv1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
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

	// ### 1. Create restore context
	// Get KubegresRestore resource
	restoreJob, err := r.getDeployedKubegresRestoreResource(ctx, req)
	if err != nil {
		return ctrl.Result{}, nil
	}

	// Get RestoreContext
	restoreJobContext, err := resources.CreateRestoreJobContext(restoreJob, ctx, r.Logger, r.Client, r.Recorder)
	if err != nil {
		return ctrl.Result{}, err
	}

	// ### 2. Check kubegres restore spec
	specCheckResult, err := restoreJobContext.RestoreSpecChecker.CheckSpec()
	if err != nil {
		return r.returnn(ctrl.Result{}, err, restoreJobContext)

	} else if specCheckResult.HasSpecFatalError {
		return r.returnn(ctrl.Result{}, nil, restoreJobContext)
	}

	restoreJobContext.RestoreResourcesStatesLogger.Log()

	// ### 3. Enforce resources
	return r.returnn(ctrl.Result{}, restoreJobContext.ResourcesCountSpecEnforcer.EnforceSpec(), restoreJobContext)
}

// SetupWithManager sets up the controller with the Manager.
func (r *KubegresRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kubegresv1.KubegresRestore{}, ctx.RestoreJobKubegresTargetField, func(rawObj client.Object) []string {
		kubegresRestore := rawObj.(*kubegresv1.KubegresRestore)

		if kubegresRestore.Spec.ClusterName == "" {
			return nil
		}

		return []string{kubegresRestore.Spec.ClusterName}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&kubegresv1.KubegresRestore{}).
		Watches(
			&source.Kind{Type: &kubegresv1.Kubegres{}},
			handler.EnqueueRequestsFromMapFunc(r.findObjectsForKubegres),
		).
		Owns(&batchv1.Job{}).
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
	// Allow Kubernetes to update its system.
	time.Sleep(1 * time.Second)

	restoreJob := &kubegresv1.KubegresRestore{}
	err := r.Client.Get(ctx, req.NamespacedName, restoreJob)
	if err == nil {
		return restoreJob, nil
	}

	r.Logger.Info("KubegresRestore resource does not exist")
	return &kubegresv1.KubegresRestore{}, err
}

func (r *KubegresRestoreReconciler) findObjectsForKubegres(kubegres client.Object) []reconcile.Request {
	kubegresRestoreList := &kubegresv1.KubegresRestoreList{}
	listOps := &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(ctx.RestoreJobKubegresTargetField, kubegres.GetName()),
		Namespace:     kubegres.GetNamespace(),
	}
	err := r.Client.List(context.Background(), kubegresRestoreList, listOps)
	if err != nil {
		r.Logger.Error(err, "Unable to list all kubegres restore resources", "Kubegres", kubegres.GetName())
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, len(kubegresRestoreList.Items))
	for i, item := range kubegresRestoreList.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}

	r.Logger.Info("KUBEGRES UPDATE", "Kubegres name", kubegres.GetName(), "Requests", requests)
	return requests
}
