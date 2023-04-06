package status

import (
	"context"

	v1 "reactive-tech.io/kubegres/api/v1"
	"reactive-tech.io/kubegres/controllers/ctx/log"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RestoreStatusWrapper struct {
	KubegresRestore      *v1.KubegresRestore
	Ctx                  context.Context
	Log                  log.LogWrapper[*v1.KubegresRestore]
	Client               client.Client
	statusFieldsToUpdate map[string]interface{}
}

func (r *RestoreStatusWrapper) GetIsCompleted() bool {
	return r.KubegresRestore.Status.IsCompleted
}

func (r *RestoreStatusWrapper) SetIsCompleted(value bool) {
	r.addStatusFieldToUpdate("IsCompleted", value)
	r.KubegresRestore.Status.IsCompleted = value
}

func (r *RestoreStatusWrapper) GetCurrentStage() string {
	return r.KubegresRestore.Status.CurrentStage
}

func (r *RestoreStatusWrapper) SetCurrentStage(value string) {
	r.addStatusFieldToUpdate("CurrentStage", value)
	r.KubegresRestore.Status.CurrentStage = value
}

func (r *RestoreStatusWrapper) UpdateStatusIfChanged() error {
	if r.statusFieldsToUpdate == nil {
		return nil
	}

	for statusFieldName, statusFieldValue := range r.statusFieldsToUpdate {
		r.Log.Info("Updating KubegresRestore' status: ",
			"Field", statusFieldName, "New value", statusFieldValue)

	}

	err := r.Client.Status().Update(r.Ctx, r.KubegresRestore)

	if err != nil {
		r.Log.Error(err, "Failed to update KubegresRestore status")

	} else {
		r.Log.Info("KubegresRestore status updated.")
	}

	return err
}

func (r *RestoreStatusWrapper) addStatusFieldToUpdate(statusFieldName string, newValue interface{}) {

	if r.statusFieldsToUpdate == nil {
		r.statusFieldsToUpdate = make(map[string]interface{})
	}

	r.statusFieldsToUpdate[statusFieldName] = newValue
}
