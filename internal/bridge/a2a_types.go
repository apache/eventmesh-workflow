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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// A2A message types compatible with A2A protocol v1.0

type A2AAgentCard struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	URL          string          `json:"url"`
	Version      string          `json:"version"`
	Capabilities A2ACapabilities `json:"capabilities"`
	Skills       []A2ASkill      `json:"skills,omitempty"`
}

type A2ACapabilities struct {
	Streaming bool `json:"streaming,omitempty"`
}

type A2ASkill struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type A2ATaskRequest struct {
	ID       string            `json:"id"`
	Message  A2AMessage        `json:"message"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type A2AMessage struct {
	Role  string        `json:"role"`
	Parts []A2ATextPart `json:"parts"`
}

type A2ATextPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type A2ATaskResponse struct {
	ID        string        `json:"id"`
	Status    string        `json:"status"`
	Message   A2AMessage    `json:"message,omitempty"`
	Artifacts []A2AArtifact `json:"artifacts,omitempty"`
	Error     *A2AError     `json:"error,omitempty"`
}

type A2AArtifact struct {
	Name  string        `json:"name"`
	Parts []A2ATextPart `json:"parts"`
}

type A2AError struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message"`
}

// A2AClient is a lightweight client for calling A2A agents via HTTP.

type A2AClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewA2AClient(baseURL string) *A2AClient {
	return &A2AClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *A2AClient) GetAgentCard() (*A2AAgentCard, error) {
	body, err := c.doGet("/.well-known/agent-card.json")
	if err != nil {
		return nil, err
	}
	var card A2AAgentCard
	if err := json.Unmarshal(body, &card); err != nil {
		return nil, err
	}
	return &card, nil
}

func (c *A2AClient) SendTask(input string, metadata map[string]string) (*A2ATaskResponse, error) {
	req := A2ATaskRequest{
		Message: A2AMessage{
			Role: "user",
			Parts: []A2ATextPart{
				{Type: "text", Text: input},
			},
		},
		Metadata: metadata,
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	body, err := c.doPost("/a2a/tasks", reqBody)
	if err != nil {
		return nil, err
	}
	var taskResp A2ATaskResponse
	if err := json.Unmarshal(body, &taskResp); err != nil {
		return nil, err
	}
	return &taskResp, nil
}

func (c *A2AClient) GetTaskResult(taskID string) (*A2ATaskResponse, error) {
	body, err := c.doGet("/a2a/tasks/" + taskID)
	if err != nil {
		return nil, err
	}
	var taskResp A2ATaskResponse
	if err := json.Unmarshal(body, &taskResp); err != nil {
		return nil, err
	}
	return &taskResp, nil
}

func (c *A2AClient) doPost(path string, body []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("A2A POST %s: status=%d body=%s", path, resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

func (c *A2AClient) doGet(path string) ([]byte, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("A2A GET %s: status=%d body=%s", path, resp.StatusCode, string(respBody))
	}
	return respBody, nil
}
