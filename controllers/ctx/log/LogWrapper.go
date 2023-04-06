/*
Copyright 2021 Reactive Tech Limited.
"Reactive Tech Limited" is a company located in England, United Kingdom.
https://www.reactive-tech.io

Lead Developer: Alex Arica

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

package log

import (
	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

type LogWrapper[T runtime.Object] struct {
	Resource T
	Logger   logr.Logger
	Recorder record.EventRecorder
}

func (r *LogWrapper[T]) WithValues(keysAndValues ...interface{}) {
	r.Logger = r.Logger.WithValues(keysAndValues...)
}

func (r *LogWrapper[T]) WithName(name string) {
	r.Logger = r.Logger.WithName(name)
}

func (r *LogWrapper[T]) Info(msg string, keysAndValues ...interface{}) {
	r.Logger.Info(msg, keysAndValues...)
}

func (r *LogWrapper[T]) Error(err error, msg string, keysAndValues ...interface{}) {
	r.Logger.Error(err, msg, keysAndValues...)
}

func (r *LogWrapper[T]) Warning(msg string, keysAndValues ...interface{}) {
	r.Logger.Info("Warning: "+msg, keysAndValues...)
}

func (r *LogWrapper[T]) InfoEvent(eventReason string, msg string, keysAndValues ...interface{}) {
	r.Info(msg, keysAndValues...)
	r.Recorder.Eventf(r.Resource, v1.EventTypeNormal, eventReason, r.constructFullMsg(msg, keysAndValues))
}

func (r *LogWrapper[T]) ErrorEvent(eventReason string, err error, msg string, keysAndValues ...interface{}) {
	r.Error(err, msg, keysAndValues...)
	r.Recorder.Eventf(r.Resource, v1.EventTypeWarning, eventReason, r.constructFullErrMsg(err, msg, keysAndValues))
}

func (r *LogWrapper[T]) WarningEvent(eventReason string, msg string, keysAndValues ...interface{}) {
	r.Warning(msg, keysAndValues...)
	r.Recorder.Eventf(r.Resource, v1.EventTypeWarning, eventReason, r.constructFullMsg(msg, keysAndValues))
}

func (r *LogWrapper[T]) constructFullMsg(msg string, keysAndValues ...interface{}) string {
	if msg == "" {
		return ""
	}

	keysAndValuesStr := InterfacesToStr(keysAndValues...)
	if keysAndValuesStr != "" {
		return msg + " " + keysAndValuesStr
	}
	return msg
}

func (r *LogWrapper[T]) constructFullErrMsg(err error, msg string, keysAndValues ...interface{}) string {
	msgToReturn := ""
	separator := ""

	customErrMsg := r.constructFullMsg(msg, keysAndValues...)
	if customErrMsg != "" {
		msgToReturn = customErrMsg
		separator = " - "
	}

	msgFromErr := err.Error()
	if msgFromErr != "" {
		msgToReturn += separator + msgFromErr
	}

	return msgToReturn
}
