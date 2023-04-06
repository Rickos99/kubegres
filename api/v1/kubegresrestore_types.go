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

package v1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ----------------------- SPEC -------------------------------------------

type File struct {
	PvcName   string `json:"pvcName,omitempty"`
	Mountpath string `json:"mountPath,omitempty"`
	Snapshot  string `json:"snapshot,omitempty"`
}

type Cluster struct {
	// Name of cluster to duplicate
	ClusterName string `json:"clusterName,omitempty"`

	// Specification of new cluster
	ClusterSpec KubegresSpec `json:"clusterSpec,omitempty"`
}

type DataSource struct {
	File    File    `json:"file,omitempty"`
	Cluster Cluster `json:"cluster,omitempty"`
}

type KubegresRestoreSpec struct {
	CustomConfig string                  `json:"customConfig,omitempty"`
	DataSource   DataSource              `json:"dataSource,omitempty"`
	Resources    v1.ResourceRequirements `json:"resources,omitempty"`
	ClusterName  string                  `json:"clusterName,omitempty"`
	DatabaseName string                  `json:"databaseName,omitempty"`
}

// ----------------------- STATUS -----------------------------------------

type KubegresRestoreStatus struct {
	IsCompleted  bool   `json:"isCompleted,omitempty"`
	CurrentStage string `json:"stage,omitempty"`

	//TODO: Display this in an event instead
	// Reason       string `json:"reason,omitempty"`
}

// ----------------------- RESOURCE ---------------------------------------

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// KubegresRestore is the Schema for the kubegresrestores API
type KubegresRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KubegresRestoreSpec   `json:"spec,omitempty"`
	Status KubegresRestoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// KubegresRestoreList contains a list of KubegresRestore
type KubegresRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubegresRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KubegresRestore{}, &KubegresRestoreList{})
}
