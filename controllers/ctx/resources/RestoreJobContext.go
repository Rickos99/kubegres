package resources

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/record"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"reactive-tech.io/kubegres/controllers/states"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RestoreJobContext struct {
	LogWrapper             log.LogWrapper[*v1.KubegresRestore]
	KubegresRestoreContext ctx.KubegresRestoreContext
	RestoreStatusWrapper   *status.RestoreStatusWrapper
	RestoreJobStates       states.RestoreJobStates

	// TODO: Implement job creator to deploy a job. Not enforcer since no updates are allowed. Created once and then never changed.
	// RestoreJobCreator creator.RestoreJob
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

	return rc, nil
}

func (r *RestoreJobContext) CreateKubegresSpecFromExistingCluster() (v1.KubegresSpec, error) {
	// TODO: Implement
	cluster := &v1.Kubegres{}
	clusterKey := r.KubegresRestoreContext.GetNamespacesresourceName(r.KubegresRestoreContext.KubegresRestore.Spec.ClusterName)
	err := r.KubegresRestoreContext.Client.Get(r.KubegresRestoreContext.Ctx, clusterKey, cluster)
	if err != nil {
		return v1.KubegresSpec{}, err
	}

	return cluster.Spec, nil
}
