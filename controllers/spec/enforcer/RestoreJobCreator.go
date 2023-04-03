package enforcer

import (
	kubegresv1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/spec/template"
)

type JobSpecEnforcer struct {
	kubegresRestoreContext ctx.KubegresRestoreContext
	resourcesCreator       template.RestoreJobResourcesCreatorTemplate
}

func CreateJobSpecEnforcer(kubegresRestoreContext ctx.KubegresRestoreContext) JobSpecEnforcer {
	resourcesCreator := template.CreateRestoreJobCreator(kubegresRestoreContext)
	return JobSpecEnforcer{
		kubegresRestoreContext: kubegresRestoreContext,
		resourcesCreator:       resourcesCreator,
	}
}

func (r *JobSpecEnforcer) Enforce(kubegresSpec kubegresv1.KubegresSpec) error {
	restoreJobTemplate, err := r.resourcesCreator.CreateRestoreJob(kubegresSpec)
	if err != nil {
		return err
	}

	err = r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, &restoreJobTemplate)
	return err
}
