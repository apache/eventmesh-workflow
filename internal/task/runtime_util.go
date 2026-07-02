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
	"context"
	"fmt"
	"strings"

	"github.com/apache/incubator-eventmesh/eventmesh-server-go/config"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/constants"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/dal"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/dal/model"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/queue"
	"github.com/google/uuid"
)

func isLocalRuntimeTask(taskType string) bool {
	switch strings.ToLower(taskType) {
	case constants.TaskTypeSet, constants.TaskTypeDo, constants.TaskTypeFork, constants.TaskTypeFor,
		constants.TaskTypeTry, constants.TaskTypeWait, constants.TaskTypeRaise, constants.TaskTypeRun,
		constants.TaskTypeEmit:
		return true
	default:
		return false
	}
}

func publishNextOrComplete(base baseTask, transition *model.WorkflowTaskRelation, input string) error {
	workflowDAL := base.workflowDAL
	if workflowDAL == nil {
		workflowDAL = dal.NewWorkflowDAL()
	}
	if transition == nil || transition.ToTaskID == constants.TaskEndID {
		return workflowDAL.UpdateInstance(context.Background(),
			&model.WorkflowInstance{WorkflowInstanceID: base.workflowInstanceID,
				WorkflowStatus: constants.WorkflowInstanceSuccessStatus})
	}
	observeQueue := base.queue
	if observeQueue == nil {
		observeQueue = queue.GetQueue(config.GlobalConfig().Flow.Queue.Store)
	}
	var taskInstance = model.WorkflowTaskInstance{WorkflowInstanceID: base.workflowInstanceID,
		WorkflowID: base.workflowID, TaskID: transition.ToTaskID, TaskInstanceID: uuid.New().String(),
		Status: constants.TaskInstanceWaitStatus, Input: input}
	return observeQueue.Publish([]*model.WorkflowTaskInstance{&taskInstance})
}

func publishNextTasks(base baseTask, relations []*model.WorkflowTaskRelation, input string) error {
	if len(relations) == 0 {
		return publishNextOrComplete(base, nil, input)
	}
	observeQueue := base.queue
	if observeQueue == nil {
		observeQueue = queue.GetQueue(config.GlobalConfig().Flow.Queue.Store)
	}
	var instances []*model.WorkflowTaskInstance
	for _, rel := range relations {
		if rel == nil || rel.ToTaskID == constants.TaskEndID {
			continue
		}
		instances = append(instances, &model.WorkflowTaskInstance{
			WorkflowInstanceID: base.workflowInstanceID,
			WorkflowID:         base.workflowID,
			TaskID:             rel.ToTaskID,
			TaskInstanceID:     uuid.New().String(),
			Status:             constants.TaskInstanceWaitStatus,
			Input:              input,
		})
	}
	if len(instances) == 0 {
		return publishNextOrComplete(base, nil, input)
	}
	return observeQueue.Publish(instances)
}

func completeWorkflow(base baseTask) error {
	workflowDAL := base.workflowDAL
	if workflowDAL == nil {
		workflowDAL = dal.NewWorkflowDAL()
	}
	return workflowDAL.UpdateInstance(context.Background(),
		&model.WorkflowInstance{WorkflowInstanceID: base.workflowInstanceID,
			WorkflowStatus: constants.WorkflowInstanceSuccessStatus})
}

func asMap(value interface{}) map[string]interface{} {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]interface{}); ok {
		return m
	}
	if m, ok := value.(map[interface{}]interface{}); ok {
		result := make(map[string]interface{}, len(m))
		for key, item := range m {
			result[fmt.Sprint(key)] = item
		}
		return result
	}
	return nil
}

func asString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
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
