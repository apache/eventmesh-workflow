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

package bridge

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/dal/model"
)

const (
	A2ATaskStatusWorking   = "working"
	A2ATaskStatusCompleted = "completed"
	A2ATaskStatusFailed    = "failed"
)

// A2AExecutor bridges workflow tasks with A2A agents.
type A2AExecutor struct {
	client    *A2AClient
	pollDelay time.Duration
	maxRetry  int
}

func NewA2AExecutor(agentURL string) *A2AExecutor {
	return &A2AExecutor{
		client:    NewA2AClient(agentURL),
		pollDelay: 2 * time.Second,
		maxRetry:  30,
	}
}

// Execute sends a workflow task to an A2A agent and waits for the result.
// Returns the agent output as JSON string suitable for passing to the next workflow task.
func (e *A2AExecutor) Execute(input string, metadata map[string]string) (string, error) {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["source"] = "eventmesh-workflow"
	metadata["timestamp"] = time.Now().UTC().Format(time.RFC3339)

	resp, err := e.client.SendTask(input, metadata)
	if err != nil {
		return "", fmt.Errorf("A2A send task failed: %w", err)
	}
	if resp.Error != nil {
		return "", fmt.Errorf("A2A task error: %s", resp.Error.Message)
	}
	if resp.Status == A2ATaskStatusCompleted {
		return e.extractOutput(resp), nil
	}
	return e.pollUntilComplete(resp.ID)
}

func (e *A2AExecutor) pollUntilComplete(taskID string) (string, error) {
	for i := 0; i < e.maxRetry; i++ {
		time.Sleep(e.pollDelay)
		resp, err := e.client.GetTaskResult(taskID)
		if err != nil {
			continue
		}
		if resp.Error != nil {
			return "", fmt.Errorf("A2A task poll error: %s", resp.Error.Message)
		}
		switch resp.Status {
		case A2ATaskStatusCompleted:
			return e.extractOutput(resp), nil
		case A2ATaskStatusFailed:
			return "", fmt.Errorf("A2A task %s failed", taskID)
		}
	}
	return "", fmt.Errorf("A2A task %s timed out after %d retries", taskID, e.maxRetry)
}

func (e *A2AExecutor) extractOutput(resp *A2ATaskResponse) string {
	if len(resp.Artifacts) > 0 && len(resp.Artifacts[0].Parts) > 0 {
		return resp.Artifacts[0].Parts[0].Text
	}
	if len(resp.Message.Parts) > 0 {
		return resp.Message.Parts[0].Text
	}
	return ""
}

// ExecuteFromAction executes based on a workflow task action model.
func (e *A2AExecutor) ExecuteFromAction(action *model.WorkflowTaskAction, input string) (string, error) {
	metadata := map[string]string{
		"operation_name": action.OperationName,
		"operation_type": action.OperationType,
		"workflow_task":  action.TaskID,
	}
	return e.Execute(input, metadata)
}

// BuildAgentCard creates an A2A agent card for workflow trigger capability.
func BuildAgentCard(workflowID string, workflowName string, baseURL string) A2AAgentCard {
	return A2AAgentCard{
		Name:        workflowName,
		Description: fmt.Sprintf("Workflow execution agent for %s", workflowID),
		URL:         baseURL,
		Version:     "1.0.0",
		Capabilities: A2ACapabilities{
			Streaming: false,
		},
		Skills: []A2ASkill{
			{
				ID:          "execute-workflow",
				Name:        "Execute Workflow",
				Description: fmt.Sprintf("Execute workflow %s and return result", workflowID),
			},
		},
	}
}

// ToJSON serializes an agent card to JSON bytes.
func (c A2AAgentCard) ToJSON() ([]byte, error) {
	return json.MarshalIndent(c, "", "  ")
}
