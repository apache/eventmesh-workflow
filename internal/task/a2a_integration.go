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
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/bridge"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/dal/model"
)

// isA2ATask returns true if the task action targets an A2A agent.
// An action is considered A2A if OperationType is "a2a" or "agent",
// or if OperationName starts with "a2a:" or "agent:".
func isA2ATask(action *model.WorkflowTaskAction) bool {
	if action == nil {
		return false
	}
	switch action.OperationType {
	case "a2a", "agent":
		return true
	}
	if len(action.OperationName) > 0 {
		if action.OperationName[:3] == "a2a" {
			return true
		}
	}
	return false
}

// newA2AExecutorFromAction creates an A2A executor from a task action.
// The OperationName acts as the agent URL for A2A calls.
func newA2AExecutorFromAction(action *model.WorkflowTaskAction) *bridge.A2AExecutor {
	agentURL := action.OperationName
	if agentURL == "" {
		agentURL = "http://localhost:9090"
	}
	return bridge.NewA2AExecutor(agentURL)
}
