package resources

import (
	"context"
	"errors"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/record"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"reactive-tech.io/kubegres/controllers/spec/enforcer/resources_count_spec"
	"reactive-tech.io/kubegres/controllers/states"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RestoreJobContext struct {
	LogWrapper             log.LogWrapper[*v1.KubegresRestore]
	KubegresRestoreContext ctx.KubegresRestoreContext
	RestoreStatusWrapper   *status.RestoreStatusWrapper
	RestoreJobStates       states.RestoreJobStates

	ResourcesCountSpecEnforcer resources_count_spec.ResourcesCountSpecEnforcer
	kubegresCountSpecEnforcer  resources_count_spec.KubegresCountSpecEnforcer
	jobCountSpecEnforcer       resources_count_spec.JobCountSpecEnforcer
}

func CreateRestoreJobContext(kubegresRestore *v1.KubegresRestore,
	ctx2 context.Context,
	logger logr.Logger,
	client client.Client,
	recorder record.EventRecorder) (*RestoreJobContext, error) {

	rc := &RestoreJobContext{}

	rc.LogWrapper = log.LogWrapper[*v1.KubegresRestore]{Resource: kubegresRestore, Logger: logger, Recorder: recorder}
	rc.RestoreStatusWrapper = &status.RestoreStatusWrapper{
		KubegresRestore: kubegresRestore,
		Ctx:             ctx2,
		Log:             rc.LogWrapper,
		Client:          client,
	}
	rc.KubegresRestoreContext = ctx.KubegresRestoreContext{
		KubegresRestore: kubegresRestore,
		Status:          rc.RestoreStatusWrapper,
		Ctx:             ctx2,
		Log:             rc.LogWrapper,
		Client:          client,
	}

	var err error
	rc.RestoreJobStates, err = states.LoadRestoreJobStates(rc.KubegresRestoreContext)
	if err != nil {
		return rc, err
	}

	kubegresSpec, err := rc.getKubegresSpec()
	if err != nil {
		rc.LogWrapper.ErrorEvent("GetKubegresSpecError", err, "Unable to get kubegres spec.", "Get Kubegres spec from existing cluster", rc.KubegresRestoreContext.ShouldRestoreFromExistingCluster())
		return rc, err
	}

	rc.addResourcesCountSpecEnforcers(kubegresSpec)

	return rc, nil
}

func (r *RestoreJobContext) addResourcesCountSpecEnforcers(kubegresSpec v1.KubegresSpec) {
	r.ResourcesCountSpecEnforcer = resources_count_spec.ResourcesCountSpecEnforcer{}
	r.kubegresCountSpecEnforcer = resources_count_spec.CreateKubegresCountSpecEnforcer(r.KubegresRestoreContext, r.RestoreJobStates, kubegresSpec)
	r.jobCountSpecEnforcer = resources_count_spec.CreateJobCountSpecEnforcer(r.KubegresRestoreContext, r.RestoreJobStates, kubegresSpec)

	r.ResourcesCountSpecEnforcer = resources_count_spec.ResourcesCountSpecEnforcer{}
	r.ResourcesCountSpecEnforcer.AddSpecEnforcer(&r.kubegresCountSpecEnforcer)
	r.ResourcesCountSpecEnforcer.AddSpecEnforcer(&r.jobCountSpecEnforcer)
}

func (r *RestoreJobContext) getKubegresSpec() (v1.KubegresSpec, error) {
	if r.KubegresRestoreContext.ShouldRestoreFromExistingCluster() {
		return r.getKubegresSpecFromExistingCluster()
	} else {
		return v1.KubegresSpec{}, errors.New("not implemented yet")
	}
}

func (r *RestoreJobContext) getKubegresSpecFromExistingCluster() (v1.KubegresSpec, error) {
	cluster := &v1.Kubegres{}
	clusterKey := r.KubegresRestoreContext.GetNamespacesresourceName(r.KubegresRestoreContext.KubegresRestore.Spec.DataSource.Cluster.ClusterName)
	err := r.KubegresRestoreContext.Client.Get(r.KubegresRestoreContext.Ctx, clusterKey, cluster)
	if err != nil {
		return v1.KubegresSpec{}, err
	}

	return cluster.Spec, nil
}
