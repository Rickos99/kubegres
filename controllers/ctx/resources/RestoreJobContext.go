package resources

import (
	"context"

	"github.com/go-logr/logr"
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

	rc.KubegresRestoreContext, err = ctx.CreateKubegresRestoreContext(kubegresRestore, rc.RestoreStatusWrapper, ctx2, rc.LogWrapper, client)
	if err != nil {
		return rc, err
	}

	rc.RestoreResourceStates, err = states.LoadRestoreResourceStates(rc.KubegresRestoreContext)
	if err != nil {
		return rc, err
	}

	rc.RestoreResourcesStatesLogger = log2.CreateRestoreResourcesStatesLogger(rc.KubegresRestoreContext, rc.RestoreResourceStates)
	rc.RestoreSpecChecker = checker.CreateRestoreSpecChecker(rc.KubegresRestoreContext, rc.RestoreResourceStates)

	rc.addResourcesCountSpecEnforcers(rc.KubegresRestoreContext.SourceKubegresClusterSpec)

	return rc, nil
}

func (r *RestoreJobContext) addResourcesCountSpecEnforcers(sourceKubegresSpec v1.KubegresSpec) {
	r.ResourcesCountSpecEnforcer = resources_count_spec.ResourcesCountSpecEnforcer{}
	kubegresCountSpecEnforcer := resources_count_spec.CreateKubegresCountSpecEnforcer(r.KubegresRestoreContext, r.RestoreResourceStates, sourceKubegresSpec)
	jobCountSpecEnforcer := resources_count_spec.CreateJobCountSpecEnforcer(r.KubegresRestoreContext, r.RestoreResourceStates, sourceKubegresSpec)

	r.ResourcesCountSpecEnforcer = resources_count_spec.ResourcesCountSpecEnforcer{}
	r.ResourcesCountSpecEnforcer.AddSpecEnforcer(&kubegresCountSpecEnforcer)
	r.ResourcesCountSpecEnforcer.AddSpecEnforcer(&jobCountSpecEnforcer)
}
