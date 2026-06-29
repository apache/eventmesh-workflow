// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/constants"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/dal/model"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/third_party/jqer"
)

type localRuntimeTask struct {
	baseTask
	action     *model.WorkflowTaskAction
	transition *model.WorkflowTaskRelation
	definition map[string]interface{}
}

func NewLocalRuntimeTask(instance *model.WorkflowTaskInstance) Task {
	var t localRuntimeTask
	if instance == nil || instance.Task == nil {
		return nil
	}
	t.baseTask = baseTask{taskID: instance.TaskID, taskInstanceID: instance.TaskInstanceID, input: instance.Input,
		workflowID: instance.WorkflowID, workflowInstanceID: instance.WorkflowInstanceID, taskType: instance.Task.TaskType}
	if len(instance.Task.Actions) > 0 {
		t.action = instance.Task.Actions[0]
	}
	if len(instance.Task.ChildTasks) > 0 {
		t.transition = instance.Task.ChildTasks[0]
	}
	if instance.Task.TaskDefinition != "" {
		_ = json.Unmarshal([]byte(instance.Task.TaskDefinition), &t.definition)
	}
	return &t
}

func (t *localRuntimeTask) Run() error {
	nextInput, err := t.execute()
	if err != nil {
		return err
	}
	return publishNextOrComplete(t.baseTask, t.transition, nextInput)
}

func (t *localRuntimeTask) execute() (string, error) {
	switch t.taskType {
	case constants.TaskTypeSet:
		return t.executeSet()
	case constants.TaskTypeWait:
		return t.executeWait()
	case constants.TaskTypeRaise:
		return "", t.executeRaise()
	case constants.TaskTypeRun:
		return t.input, t.executeRun()
	case constants.TaskTypeDo, constants.TaskTypeFork, constants.TaskTypeFor, constants.TaskTypeTry:
		return t.input, nil
	case constants.TaskTypeEmit:
		return t.input, t.executeEmit()
	default:
		return t.input, nil
	}
}

func (t *localRuntimeTask) executeSet() (string, error) {
	setValue, ok := t.definition[constants.TaskTypeSet]
	if !ok {
		return t.input, nil
	}
	var input interface{}
	if t.input != "" {
		if err := json.Unmarshal([]byte(t.input), &input); err != nil {
			return "", err
		}
	}
	jq := jqer.NewJQ()
	ret, err := jq.Object(input, setValue)
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(ret)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (t *localRuntimeTask) executeWait() (string, error) {
	waitDef := asMap(t.definition[constants.TaskTypeWait])
	durationText := firstNonEmpty(asString(waitDef["duration"]), asString(waitDef["seconds"]), asString(waitDef["milliseconds"]))
	if durationText == "" {
		return t.input, nil
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil {
		return "", err
	}
	if duration > 0 {
		time.Sleep(duration)
	}
	return t.input, nil
}

func (t *localRuntimeTask) executeRaise() error {
	raiseDef := asMap(t.definition[constants.TaskTypeRaise])
	if len(raiseDef) == 0 {
		return errors.New("workflow raise task failed")
	}
	errorDef := asMap(raiseDef["error"])
	if len(errorDef) == 0 {
		return fmt.Errorf("workflow raise task failed: %v", raiseDef)
	}
	return fmt.Errorf("workflow raise task failed: type=%s title=%s status=%s detail=%s",
		asString(errorDef["type"]), asString(errorDef["title"]), asString(errorDef["status"]), asString(errorDef["detail"]))
}

func (t *localRuntimeTask) executeRun() error {
	if t.action == nil || t.action.OperationName == "" {
		return nil
	}
	return publishEvent(t.workflowInstanceID, t.taskInstanceID, t.action.OperationName, t.input)
}

func (t *localRuntimeTask) executeEmit() error {
	if t.action == nil || t.action.OperationName == "" {
		return nil
	}
	return publishEvent(t.workflowInstanceID, t.taskInstanceID, t.action.OperationName, t.input)
}
