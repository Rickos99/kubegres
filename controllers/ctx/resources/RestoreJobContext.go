package resources

import (
	"context"
	"errors"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"reactive-tech.io/kubegres/controllers/spec/checker"
	"reactive-tech.io/kubegres/controllers/spec/enforcer/resources_count_spec"
	"reactive-tech.io/kubegres/controllers/states"
	log2 "reactive-tech.io/kubegres/controllers/states/log"
)

type RestoreJobContext struct {
	LogWrapper                   log.LogWrapper[*v1.KubegresRestore]
	KubegresRestoreContext       ctx.KubegresRestoreContext
	RestoreStatusWrapper         *status.RestoreStatusWrapper
	RestoreResourceStates        states.RestoreResourceStates
	RestoreResourcesStatesLogger log2.RestoreResourcesStatesLogger
	RestoreSpecChecker           checker.RestoreSpecChecker

	ResourcesCountSpecEnforcer resources_count_spec.ResourcesCountSpecEnforcer
	kubegresCountSpecEnforcer  resources_count_spec.KubegresCountSpecEnforcer
	jobCountSpecEnforcer       resources_count_spec.JobCountSpecEnforcer
}

func CreateRestoreJobContext(kubegresRestore *v1.KubegresRestore,
	ctx2 context.Context,
	logger logr.Logger,
	client client.Client,
	recorder record.EventRecorder) (rc *RestoreJobContext, err error) {

	rc = &RestoreJobContext{}

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

	rc.RestoreResourceStates, err = states.LoadRestoreResourceStates(rc.KubegresRestoreContext)
	if err != nil {
		return rc, err
	}

	rc.RestoreResourcesStatesLogger = log2.CreateRestoreResourcesStatesLogger(rc.KubegresRestoreContext, rc.RestoreResourceStates)
	kubegresSpec, err := rc.getKubegresSpec()
	if err != nil {
		rc.LogWrapper.ErrorEvent("GetKubegresSpecError", err, "Unable to get kubegres spec.", "Get Kubegres spec from existing cluster", rc.KubegresRestoreContext.ShouldRestoreFromExistingCluster())
		return rc, err
	}

	rc.RestoreSpecChecker = checker.CreateRestoreSpecChecker(rc.KubegresRestoreContext, rc.RestoreResourceStates)

	rc.addResourcesCountSpecEnforcers(kubegresSpec)

	return rc, nil
}

func (r *RestoreJobContext) addResourcesCountSpecEnforcers(kubegresSpec v1.KubegresSpec) {
	r.ResourcesCountSpecEnforcer = resources_count_spec.ResourcesCountSpecEnforcer{}
	r.kubegresCountSpecEnforcer = resources_count_spec.CreateKubegresCountSpecEnforcer(r.KubegresRestoreContext, r.RestoreResourceStates, kubegresSpec)
	r.jobCountSpecEnforcer = resources_count_spec.CreateJobCountSpecEnforcer(r.KubegresRestoreContext, r.RestoreResourceStates, kubegresSpec)

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
	if err != nil && apierrors.IsNotFound(err) {
		err = nil
	}

	return cluster.Spec, err
}
