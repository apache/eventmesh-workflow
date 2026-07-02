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

package swf

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func Parse(source string) (*Workflow, error) {
	if len(strings.TrimSpace(source)) == 0 {
		return nil, nil
	}
	var raw map[string]interface{}
	if err := yaml.Unmarshal([]byte(source), &raw); err != nil {
		return nil, err
	}
	var wf *Workflow
	var err error
	if _, ok := raw["document"]; ok {
		wf, err = parseV1Workflow(raw)
	} else {
		wf, err = parseLegacyWorkflow(raw)
	}
	if err != nil {
		return nil, err
	}
	if wf == nil {
		return nil, nil
	}
	if verr := wf.Validate(); verr != nil {
		return nil, verr
	}
	return wf, nil
}

func parseV1Workflow(raw map[string]interface{}) (*Workflow, error) {
	doc := asMap(raw["document"])
	name := asString(doc["name"])
	if name == "" {
		return nil, errors.New("workflow document.name is required")
	}
	version := asString(doc["version"])
	if version == "" {
		return nil, errors.New("workflow document.version is required")
	}
	dsl := asString(doc["dsl"])
	if dsl == "" {
		return nil, errors.New("workflow document.dsl is required")
	}
	tasks, err := parseV1TaskList(raw["do"])
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, errors.New("workflow do must contain at least one task")
	}
	return &Workflow{
		ID:        name,
		Name:      name,
		Version:   version,
		DSL:       dsl,
		Namespace: asString(doc["namespace"]),
		Start:     tasks[0].Name,
		Tasks:     tasks,
		Functions: parseReusableFunctions(raw),
		Schedule:  parseSchedule(raw["schedule"]),
		Input:     parseWorkflowInput(raw["input"]),
		Output:    expressionString(asMap(raw["output"])["as"]),
		Legacy:    false,
	}, nil
}

func parseLegacyWorkflow(raw map[string]interface{}) (*Workflow, error) {
	id := asString(raw["id"])
	if id == "" {
		return nil, errors.New("workflow id is required")
	}
	states := asSlice(raw["states"])
	if len(states) == 0 {
		return nil, errors.New("workflow states must contain at least one state")
	}
	functions := make(map[string]*Function)
	for _, item := range asSlice(raw["functions"]) {
		fnMap := asMap(item)
		name := asString(fnMap["name"])
		if name == "" {
			continue
		}
		functions[name] = &Function{Name: name, Operation: asString(fnMap["operation"]), Type: asString(fnMap["type"])}
	}
	var tasks []*Task
	for _, item := range states {
		stateMap := asMap(item)
		name := asString(stateMap["name"])
		if name == "" {
			continue
		}
		task := &Task{Name: name, Type: asString(stateMap["type"]), Then: legacyThen(stateMap), ExplicitThen: true}
		if filter := asMap(stateMap["stateDataFilter"]); len(filter) > 0 {
			task.InputFilter = asString(filter["input"])
		}
		task.Actions = parseLegacyActions(stateMap, functions)
		if task.Type == TaskTypeSwitch {
			task.Cases = parseLegacySwitchCases(stateMap)
		}
		tasks = append(tasks, task)
	}
	start := asString(raw["start"])
	if start == "" && len(tasks) > 0 {
		start = tasks[0].Name
	}
	return &Workflow{
		ID:        id,
		Name:      asString(raw["name"]),
		Version:   asString(raw["version"]),
		DSL:       asString(raw["specVersion"]),
		Start:     start,
		Tasks:     tasks,
		Functions: functions,
		Legacy:    true,
	}, nil
}

func parseSchedule(value interface{}) *Schedule {
	scheduleMap := asMap(value)
	if len(scheduleMap) == 0 {
		return nil
	}
	return &Schedule{
		Start: asString(scheduleMap["start"]),
		Cron:  asString(scheduleMap["cron"]),
		After: asString(scheduleMap["after"]),
	}
}

func parseWorkflowInput(value interface{}) string {
	inputMap := asMap(value)
	if len(inputMap) == 0 {
		return ""
	}
	return expressionString(inputMap["from"])
}

func parseV1TaskList(value interface{}) ([]*Task, error) {
	items := asSlice(value)
	if len(items) == 0 {
		return nil, nil
	}
	var tasks []*Task
	for _, item := range items {
		itemMap := asMap(item)
		for taskName, taskDef := range itemMap {
			task, err := parseV1Task(taskName, asMap(taskDef))
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func parseV1Task(taskName string, def map[string]interface{}) (*Task, error) {
	if taskName == "" {
		return nil, errors.New("task name is required")
	}
	task := &Task{Name: taskName, Type: detectV1TaskType(def), Raw: mustJSON(def)}
	if input := asMap(def["input"]); len(input) > 0 {
		task.InputFilter = expressionString(input["from"])
	}
	if data := asString(def["data"]); data != "" {
		task.InlineData = data
	}
	if output := asMap(def["output"]); len(output) > 0 {
		task.OutputFilter = expressionString(output["as"])
	}
	if _, ok := def["then"]; ok {
		task.Then = expressionString(def["then"])
		task.ExplicitThen = true
	}
	switch task.Type {
	case TaskTypeOperation:
		task.Actions = []*Action{parseV1CallAction(def)}
	case TaskTypeEvent:
		task.Actions = []*Action{parseV1CallAction(def)}
	case TaskTypeSet:
		task.Actions = []*Action{{OperationName: taskName, OperationType: TaskTypeSet}}
	case TaskTypeSwitch:
		task.Cases = parseV1SwitchCases(def["switch"])
	case TaskTypeDo:
		children, err := parseV1TaskList(def["do"])
		if err != nil {
			return nil, err
		}
		task.Children = children
	case TaskTypeFork:
		task.Children = parseV1ForkBranches(def["fork"])
	case TaskTypeFor:
		children, err := parseV1TaskList(asMap(def["for"])["do"])
		if err != nil {
			return nil, err
		}
		task.Children = children
	case TaskTypeTry:
		children, err := parseV1TaskList(def["try"])
		if err != nil {
			return nil, err
		}
		task.Children = children
		if catchMap := asMap(def["catch"]); len(catchMap) > 0 {
			if when := catchMap["when"]; when != nil {
				catchTasks, err := parseV1TaskList(when)
				if err == nil {
					task.Children = append(task.Children, catchTasks...)
				}
			}
		}
	case TaskTypeEmit, TaskTypeListen, TaskTypeWait, TaskTypeRaise, TaskTypeRun:
		task.Actions = []*Action{{OperationName: taskName, OperationType: task.Type}}
	}
	return task, nil
}

func detectV1TaskType(def map[string]interface{}) string {
	if _, ok := def["call"]; ok {
		return TaskTypeOperation
	}
	for _, key := range []string{TaskTypeSwitch, TaskTypeSet, TaskTypeDo, TaskTypeFork, TaskTypeFor, TaskTypeTry, TaskTypeWait, TaskTypeRaise, TaskTypeRun, TaskTypeEmit, TaskTypeListen} {
		if _, ok := def[key]; ok {
			return key
		}
	}
	return TaskTypeOperation
}

func parseV1CallAction(def map[string]interface{}) *Action {
	operationType := expressionString(def["call"])
	operationName := operationType
	with := asMap(def["with"])
	switch operationType {
	case "http":
		operationName = expressionString(with["endpoint"])
	case "openapi":
		operationName = firstNonEmpty(expressionString(with["operationId"]), expressionString(with["operation"]), expressionString(with["document"]))
	case "asyncapi":
		operationName = firstNonEmpty(expressionString(with["operation"]), expressionString(with["channel"]), expressionString(with["document"]))
	case "grpc":
		operationName = firstNonEmpty(expressionString(with["service"]), expressionString(with["method"]))
	default:
		operationName = firstNonEmpty(operationName, expressionString(with["operation"]), expressionString(with["endpoint"]))
	}
	return &Action{OperationName: operationName, OperationType: operationType}
}

func parseReusableFunctions(raw map[string]interface{}) map[string]*Function {
	functions := make(map[string]*Function)
	use := asMap(raw["use"])
	for name, item := range asMap(use["functions"]) {
		def := asMap(item)
		action := parseV1CallAction(def)
		functions[name] = &Function{Name: name, Operation: action.OperationName, Type: action.OperationType}
	}
	return functions
}

func parseV1SwitchCases(value interface{}) []*SwitchCase {
	var cases []*SwitchCase
	for _, item := range asSlice(value) {
		itemMap := asMap(item)
		for caseName, caseDef := range itemMap {
			def := asMap(caseDef)
			cases = append(cases, &SwitchCase{
				Name:      caseName,
				Condition: expressionString(def["when"]),
				Then:      expressionString(def["then"]),
				IsDefault: caseName == "default" || def["when"] == nil,
			})
		}
	}
	return cases
}

func parseV1ForkBranches(value interface{}) []*Task {
	forkDef := asMap(value)
	branches := asSlice(forkDef["branches"])
	var tasks []*Task
	for _, branch := range branches {
		branchTasks, err := parseV1TaskList([]interface{}{branch})
		if err == nil {
			tasks = append(tasks, branchTasks...)
		}
	}
	return tasks
}

func parseLegacyActions(stateMap map[string]interface{}, functions map[string]*Function) []*Action {
	var actionValues []interface{}
	if stateMap["actions"] != nil {
		actionValues = asSlice(stateMap["actions"])
	}
	for _, onEvent := range asSlice(stateMap["onEvents"]) {
		onEventMap := asMap(onEvent)
		actionValues = append(actionValues, asSlice(onEventMap["actions"])...)
	}
	var actions []*Action
	for _, actionValue := range actionValues {
		actionMap := asMap(actionValue)
		functionRef := asMap(actionMap["functionRef"])
		functionName := asString(functionRef["refName"])
		fn := functions[functionName]
		if fn == nil {
			continue
		}
		actions = append(actions, &Action{OperationName: fn.Operation, OperationType: fn.Type})
	}
	return actions
}

func parseLegacySwitchCases(stateMap map[string]interface{}) []*SwitchCase {
	var cases []*SwitchCase
	for _, conditionValue := range asSlice(stateMap["dataConditions"]) {
		condition := asMap(conditionValue)
		then := asString(condition["transition"])
		if then == "" && asBool(condition["end"]) {
			then = "end"
		}
		cases = append(cases, &SwitchCase{Name: asString(condition["name"]), Condition: expressionString(condition["condition"]), Then: then})
	}
	defaultCondition := asMap(stateMap["defaultCondition"])
	if len(defaultCondition) > 0 {
		then := asString(defaultCondition["transition"])
		if then == "" && asBool(defaultCondition["end"]) {
			then = "end"
		}
		cases = append(cases, &SwitchCase{Name: "default", Then: then, IsDefault: true})
	}
	return cases
}

func legacyThen(stateMap map[string]interface{}) string {
	if transition := asString(stateMap["transition"]); transition != "" {
		return transition
	}
	if asBool(stateMap["end"]) {
		return "end"
	}
	return ""
}

func asMap(value interface{}) map[string]interface{} {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]interface{}); ok {
		return m
	}
	if m, ok := value.(map[interface{}]interface{}); ok {
		res := make(map[string]interface{}, len(m))
		for key, item := range m {
			res[fmt.Sprint(key)] = item
		}
		return res
	}
	return nil
}

func asSlice(value interface{}) []interface{} {
	if value == nil {
		return nil
	}
	if items, ok := value.([]interface{}); ok {
		return items
	}
	return nil
}

func asString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func asBool(value interface{}) bool {
	if b, ok := value.(bool); ok {
		return b
	}
	return strings.EqualFold(asString(value), "true")
}

func expressionString(value interface{}) string {
	text := strings.TrimSpace(asString(value))
	text = strings.TrimPrefix(text, "${")
	text = strings.TrimSuffix(text, "}")
	return strings.TrimSpace(text)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func mustJSON(value interface{}) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(bytes)
}
