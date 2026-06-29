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
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/filter"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/third_party/jqer"
)

type localRuntimeTask struct {
	baseTask
	action      *model.WorkflowTaskAction
	transitions []*model.WorkflowTaskRelation
	definition  map[string]interface{}
}

func NewLocalRuntimeTask(instance *model.WorkflowTaskInstance) Task {
	var t localRuntimeTask
	if instance == nil || instance.Task == nil {
		return nil
	}
	t.baseTask = newBaseTask(instance)
	if len(instance.Task.Actions) > 0 {
		t.action = instance.Task.Actions[0]
	}
	t.transitions = instance.Task.ChildTasks
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
	nextInput = filter.FilterWorkflowTaskOutputData(nextInput, t.outputFilter)
	if len(t.transitions) == 0 || t.transitions[0] == nil || t.transitions[0].ToTaskID == constants.TaskEndID {
		return completeWorkflow(t.baseTask)
	}
	if len(t.transitions) == 1 {
		return publishNextOrComplete(t.baseTask, t.transitions[0], nextInput)
	}
	return publishNextTasks(t.baseTask, t.transitions, nextInput)
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
	case constants.TaskTypeFork:
		return t.input, nil
	case constants.TaskTypeTry:
		return t.executeTry()
	case constants.TaskTypeFor:
		return t.executeFor()
	case constants.TaskTypeDo:
		return t.executeDo()
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

func (t *localRuntimeTask) executeTry() (string, error) {
	tryTasks := asSlice(t.definition["try"])
	if len(tryTasks) == 0 {
		return t.input, nil
	}
	for _, taskItem := range tryTasks {
		taskMap := asMap(taskItem)
		for taskName, taskDef := range taskMap {
			def := asMap(taskDef)
			taskType := detectTaskType(def)
			switch taskType {
			case constants.TaskTypeSet:
				return t.executeSetFromDef(def)
			case constants.TaskTypeRaise:
				return "", fmt.Errorf("try task %s raised: %v", taskName, def)
			case constants.TaskTypeCall:
				return t.input, nil
			}
		}
	}
	return t.input, nil
}

func (t *localRuntimeTask) executeFor() (string, error) {
	forDef := asMap(t.definition["for"])
	forDo := asMap(forDef["do"])
	if len(forDo) == 0 {
		return t.input, nil
	}
	var items []interface{}
	if t.input != "" {
		var inputData interface{}
		if err := json.Unmarshal([]byte(t.input), &inputData); err == nil {
			if arr, ok := inputData.([]interface{}); ok {
				items = arr
			}
		}
	}
	if len(items) == 0 {
		items = append(items, t.input)
	}
	for _, item := range items {
		itemInput, _ := json.Marshal(item)
		for _, taskItem := range asSlice(forDo["do"]) {
			taskMap := asMap(taskItem)
			for _, taskDef := range taskMap {
				def := asMap(taskDef)
				taskType := detectTaskType(def)
				switch taskType {
				case constants.TaskTypeSet:
					result, err := executeSetWithInput(def, string(itemInput))
					if err != nil {
						return "", err
					}
					itemInput = []byte(result)
				}
			}
		}
	}
	return t.input, nil
}

func (t *localRuntimeTask) executeDo() (string, error) {
	doList := asSlice(t.definition["do"])
	if len(doList) == 0 {
		return t.input, nil
	}
	currentInput := t.input
	for _, taskItem := range doList {
		taskMap := asMap(taskItem)
		for _, taskDef := range taskMap {
			def := asMap(taskDef)
			taskType := detectTaskType(def)
			switch taskType {
			case constants.TaskTypeSet:
				result, err := t.executeSetFromDef(def)
				if err != nil {
					return "", err
				}
				if result != "" {
					currentInput = result
				}
			case constants.TaskTypeRaise:
				return "", fmt.Errorf("do task raised: %v", def)
			}
		}
	}
	return currentInput, nil
}

func (t *localRuntimeTask) executeSetFromDef(def map[string]interface{}) (string, error) {
	setExpr := asMap(def["set"])
	if len(setExpr) == 0 {
		return t.input, nil
	}
	var input interface{}
	if t.input != "" {
		if err := json.Unmarshal([]byte(t.input), &input); err != nil {
			return "", err
		}
	}
	jq := jqer.NewJQ()
	ret, err := jq.Object(input, setExpr)
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(ret)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func executeSetWithInput(def map[string]interface{}, input string) (string, error) {
	setExpr := asMap(def["set"])
	if len(setExpr) == 0 {
		return input, nil
	}
	var inputData interface{}
	if input != "" {
		if err := json.Unmarshal([]byte(input), &inputData); err != nil {
			return "", err
		}
	}
	jq := jqer.NewJQ()
	ret, err := jq.Object(inputData, setExpr)
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(ret)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func detectTaskType(def map[string]interface{}) string {
	if _, ok := def["call"]; ok {
		return constants.TaskTypeCall
	}
	for _, key := range []string{constants.TaskTypeSet, constants.TaskTypeWait, constants.TaskTypeRaise, constants.TaskTypeRun, constants.TaskTypeEmit} {
		if _, ok := def[key]; ok {
			return key
		}
	}
	return constants.TaskTypeCall
}
