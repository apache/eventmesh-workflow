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
	"errors"
	"fmt"
)

const (
	LatestDSLVersion = "1.0.3"

	TaskTypeOperation = "operation"
	TaskTypeEvent     = "event"
	TaskTypeSwitch    = "switch"
	TaskTypeSet       = "set"
	TaskTypeDo        = "do"
	TaskTypeFork      = "fork"
	TaskTypeFor       = "for"
	TaskTypeTry       = "try"
	TaskTypeWait      = "wait"
	TaskTypeRaise     = "raise"
	TaskTypeRun       = "run"
	TaskTypeEmit      = "emit"
	TaskTypeListen    = "listen"
)

type Schedule struct {
	Start string
	Cron  string
	After string
}

type Workflow struct {
	ID        string
	Name      string
	Version   string
	DSL       string
	Namespace string
	Start     string
	Tasks     []*Task
	Functions map[string]*Function
	Schedule  *Schedule
	Input     string
	Output    string
	Legacy    bool
}

type Function struct {
	Name      string
	Operation string
	Type      string
}

type Task struct {
	Name         string
	Type         string
	InputFilter  string
	OutputFilter string
	InlineData   string
	Then         string
	ExplicitThen bool
	Actions      []*Action
	Cases        []*SwitchCase
	Children     []*Task
	Raw          string
}

type Action struct {
	OperationName string
	OperationType string
}

type SwitchCase struct {
	Name      string
	Condition string
	Then      string
	IsDefault bool
}

func (w *Workflow) FlattenTasks() []*Task {
	if w == nil {
		return nil
	}
	var tasks []*Task
	var walk func(items []*Task)
	walk = func(items []*Task) {
		for _, item := range items {
			if item == nil {
				continue
			}
			tasks = append(tasks, item)
			walk(item.Children)
		}
	}
	walk(w.Tasks)
	return tasks
}

func (w *Workflow) Validate() error {
	if w == nil {
		return nil
	}
	allTasks := w.FlattenTasks()
	if len(allTasks) == 0 {
		return errors.New("workflow must define at least one task")
	}
	taskNames := make(map[string]bool, len(allTasks))
	for _, task := range allTasks {
		if task.Name == "" {
			return errors.New("all tasks must have a name")
		}
		if taskNames[task.Name] {
			return fmt.Errorf("duplicate task name: %s", task.Name)
		}
		taskNames[task.Name] = true
	}
	for _, task := range allTasks {
		if err := validateTaskFlow(task, taskNames); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskFlow(task *Task, validNames map[string]bool) error {
	if task == nil {
		return nil
	}
	if task.ExplicitThen {
		if task.Then != "" && !isValidFlowTarget(task.Then, validNames) {
			return fmt.Errorf("task %s: then target %s not found", task.Name, task.Then)
		}
	}
	for _, c := range task.Cases {
		if c == nil {
			continue
		}
		if c.Then != "" && !isValidFlowTarget(c.Then, validNames) {
			return fmt.Errorf("task %s: switch case then target %s not found", task.Name, c.Then)
		}
	}
	for _, child := range task.Children {
		if err := validateTaskFlow(child, validNames); err != nil {
			return err
		}
	}
	return nil
}

func isValidFlowTarget(target string, validNames map[string]bool) bool {
	switch target {
	case "end", "exit", "continue":
		return true
	}
	return validNames[target]
}
