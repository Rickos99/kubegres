package resources

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/record"
	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/ctx/status"
	"reactive-tech.io/kubegres/controllers/spec/enforcer"
	"reactive-tech.io/kubegres/controllers/states"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RestoreJobContext struct {
	LogWrapper             log.LogWrapper[*v1.KubegresRestore]
	KubegresRestoreContext ctx.KubegresRestoreContext
	// KubegresContext        ctx.KubegresContext
	RestoreStatusWrapper *status.RestoreStatusWrapper
	RestoreJobStates     states.RestoreJobStates
	RestoreJobEnforcer   enforcer.JobSpecEnforcer
}

func CreateRestoreJobContext(kubegresRestore *v1.KubegresRestore,
	// kubegres *v1.Kubegres,
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

	// kubegresLogwrapper := log.LogWrapper[*v1.Kubegres]{Resource: kubegres, Logger: logger, Recorder: recorder}
	// kubegresStatusWrapper := &status.KubegresStatusWrapper{
	// 	Kubegres: kubegres,
	// 	Ctx:      ctx2,
	// 	Log:      kubegresLogwrapper,
	// 	Client:   client,
	// }
	// rc.KubegresContext = ctx.KubegresContext{
	// 	Kubegres: kubegres,
	// 	Status:   kubegresStatusWrapper,
	// 	Ctx:      ctx2,
	// 	Log:      kubegresLogwrapper,
	// 	Client:   client,
	// }
	rc.RestoreJobEnforcer = enforcer.CreateJobSpecEnforcer(rc.KubegresRestoreContext)

	var err error
	rc.RestoreJobStates, err = states.LoadRestoreJobStates(rc.KubegresRestoreContext)
	if err != nil {
		return rc, err
	}

	return rc, nil
}

func (r *RestoreJobContext) CreateKubegresSpecFromExistingCluster() (v1.KubegresSpec, error) {
	cluster := &v1.Kubegres{}
	clusterKey := r.KubegresRestoreContext.GetNamespacesresourceName(r.KubegresRestoreContext.KubegresRestore.Spec.DataSource.Cluster.ClusterName)
	err := r.KubegresRestoreContext.Client.Get(r.KubegresRestoreContext.Ctx, clusterKey, cluster)
	if err != nil {
		return v1.KubegresSpec{}, err
	}

	return cluster.Spec, nil
}

func (r *RestoreJobContext) CreateClusterFromSpec(kubegresSpec v1.KubegresSpec) error {
	var replicas int32 = 1
	kubegres := &v1.Kubegres{}
	kubegres.Spec = kubegresSpec
	kubegres.ObjectMeta.Name = r.KubegresRestoreContext.KubegresRestore.Spec.ClusterName
	kubegres.ObjectMeta.Namespace = r.KubegresRestoreContext.KubegresRestore.Namespace
	kubegres.Spec.Replicas = &replicas
	kubegres.Spec.Resources = r.KubegresRestoreContext.KubegresRestore.Spec.Resources

	err := r.KubegresRestoreContext.Client.Create(r.KubegresRestoreContext.Ctx, kubegres)
	if err != nil {
		return err
	}

	r.KubegresRestoreContext.Log.InfoEvent("KubegresClusterCreated", "Created a new kubegres cluster", "Cluster name", kubegres.Name)

	return nil
}
