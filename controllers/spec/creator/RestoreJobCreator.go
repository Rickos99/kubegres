package creator

import (
	"errors"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	kubegresv1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"reactive-tech.io/kubegres/controllers/spec/template/yaml"
)

type RestoreJobCreator struct {
	kubegresRestoreContext ctx.KubegresRestoreContext

	LogWrapper log.LogWrapper[*kubegresv1.KubegresRestore]
}

func CreateRestoreJobCreator(kubegresRestoreContext ctx.KubegresRestoreContext,
	logWrapper log.LogWrapper[*kubegresv1.KubegresRestore]) *RestoreJobCreator {
	return &RestoreJobCreator{
		kubegresRestoreContext: kubegresRestoreContext,
		LogWrapper:             logWrapper,
	}
}

func (r *RestoreJobCreator) CreateFromSpec() error {
	restoreJobTemplate, err := r.loadRestoreJobFromTemplate()
	if err != nil {
		return err
	}

	restoreJobTemplate.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = r.kubegresRestoreContext.KubegresRestore.Spec.DataSource.File.PvcName
	if r.kubegresRestoreContext.KubegresRestore.Spec.CustomConfig != "" {
		restoreJobTemplate.Spec.Template.Spec.Volumes[1].ConfigMap.Name = r.kubegresRestoreContext.KubegresRestore.Spec.CustomConfig
	} else {
		restoreJobTemplate.Spec.Template.Spec.Volumes[1].ConfigMap.Name = ctx.BaseConfigMapName
	}

	// container := &restoreJobTemplate.Spec.Template.Spec.Containers[0]
	return errors.New("not implemented yet")

	// err = r.kubegresRestoreContext.Client.Create(r.kubegresRestoreContext.Ctx, &restoreJobTemplate)
	// return err
}

func (r *RestoreJobCreator) loadRestoreJobFromTemplate() (restoreTemplate batchv1.Job, err error) {
	obj, err := r.decodeYaml(yaml.RestoreJob)

	if err != nil {
		r.LogWrapper.Error(err, "Unable to load Kubegres Restore Job. Given error:")
		return batchv1.Job{}, err
	}
	return *obj.(*batchv1.Job), nil
}

func (r *RestoreJobCreator) decodeYaml(yamlContents string) (runtime.Object, error) {

	decode := scheme.Codecs.UniversalDeserializer().Decode

	obj, _, err := decode([]byte(yamlContents), nil, nil)

	if err != nil {
		r.LogWrapper.Error(err, "Error in decode: ", "obj", obj)
	}

	return obj, err
}
