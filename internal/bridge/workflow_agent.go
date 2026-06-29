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
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/apache/incubator-eventmesh/eventmesh-server-go/log"
	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/flow"
	"github.com/google/uuid"
)

// WorkflowAgent exposes workflow execution capabilities through the A2A protocol.
// A2A clients can submit tasks that trigger workflow executions and monitor their status.

type WorkflowAgent struct {
	card           A2AAgentCard
	engine         *flow.Engine
	mu             sync.RWMutex
	taskToWorkflow map[string]string // A2A task ID → workflow instance ID
}

func NewWorkflowAgent(name, baseURL string) *WorkflowAgent {
	return &WorkflowAgent{
		card:           BuildAgentCard(name, name, baseURL),
		engine:         flow.NewEngine(),
		taskToWorkflow: make(map[string]string),
	}
}

func (wa *WorkflowAgent) AgentCard() A2AAgentCard {
	return wa.card
}

func (wa *WorkflowAgent) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/.well-known/agent-card.json", wa.handleAgentCard)
	mux.HandleFunc("/a2a/tasks", wa.handleSubmitTask)
	mux.HandleFunc("/a2a/tasks/", wa.handleTaskStatus)
	mux.HandleFunc("/a2a/health", wa.handleHealth)
}

// A2A HTTP Handlers

func (wa *WorkflowAgent) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	body, _ := wa.card.ToJSON()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(body)
}

func (wa *WorkflowAgent) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req A2ATaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, A2ATaskResponse{
			Error: &A2AError{Message: "invalid request: " + err.Error()},
		})
		return
	}
	input := extractTaskInput(req)
	if input == "" {
		writeJSON(w, http.StatusBadRequest, A2ATaskResponse{
			Error: &A2AError{Message: "task message must contain text"},
		})
		return
	}

	// Start workflow execution
	instanceID := uuid.New().String()
	flowParam := &flow.WorkflowParam{ID: wa.card.Name, Input: input}
	wfInstanceID, err := wa.engine.Start(context.Background(), flowParam)
	if err != nil {
		log.Errorf("WorkflowAgent: start workflow failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, A2ATaskResponse{
			Error: &A2AError{Message: "workflow start failed: " + err.Error()},
		})
		return
	}

	wa.mu.Lock()
	wa.taskToWorkflow[instanceID] = wfInstanceID
	wa.mu.Unlock()

	a2aTaskID := instanceID
	if req.ID != "" {
		a2aTaskID = req.ID
		wa.mu.Lock()
		wa.taskToWorkflow[a2aTaskID] = wfInstanceID
		wa.mu.Unlock()
	}

	resp := A2ATaskResponse{
		ID:     a2aTaskID,
		Status: A2ATaskStatusWorking,
		Message: A2AMessage{
			Role: "agent",
			Parts: []A2ATextPart{
				{Type: "text", Text: "Workflow execution started: " + wfInstanceID},
			},
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (wa *WorkflowAgent) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	taskID := extractTaskIDFromPath(r.URL.Path, "/a2a/tasks/")
	if taskID == "" {
		http.NotFound(w, r)
		return
	}

	wa.mu.RLock()
	wfInstanceID, ok := wa.taskToWorkflow[taskID]
	wa.mu.RUnlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, A2ATaskResponse{
			Error: &A2AError{Message: "task not found"},
		})
		return
	}

	resp := A2ATaskResponse{
		ID:     taskID,
		Status: A2ATaskStatusWorking,
		Message: A2AMessage{
			Role: "agent",
			Parts: []A2ATextPart{
				{Type: "text", Text: "Workflow instance: " + wfInstanceID},
			},
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (wa *WorkflowAgent) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(`{"status":"ok"}`))
}

// Helper functions

func extractTaskInput(req A2ATaskRequest) string {
	for _, part := range req.Message.Parts {
		if part.Type == "text" && part.Text != "" {
			return part.Text
		}
	}
	return ""
}

func extractTaskIDFromPath(path string, prefix string) string {
	if len(path) <= len(prefix) {
		return ""
	}
	return path[len(prefix):]
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(statusCode)
	body, _ := json.Marshal(data)
	w.Write(body)
}
