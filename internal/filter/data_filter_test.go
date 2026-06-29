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

package filter

import (
	"encoding/json"
	"testing"

	"github.com/apache/incubator-eventmesh/eventmesh-workflow-go/internal/dal/model"
	"github.com/stretchr/testify/assert"
)

func TestFilterWorkflowTaskInputData(t *testing.T) {
	task := mockWorkflowInstance()
	FilterWorkflowTaskInputData(task)
	assert.JSONEq(t, `{ "order_no": "123456789"}`, task.Input)
}

func TestFilterWorkflowTaskInputDataWithV1RuntimeExpression(t *testing.T) {
	task := mockWorkflowInstance()
	task.Task.TaskInputFilter = `{order_no: .order_no}`
	FilterWorkflowTaskInputData(task)
	assert.JSONEq(t, `{ "order_no": "123456789"}`, task.Input)
}

func TestFilterWorkflowTaskInputDataKeepsValidJSON(t *testing.T) {
	task := mockWorkflowInstance()
	FilterWorkflowTaskInputData(task)
	var jsonObj map[string]interface{}
	assert.NoError(t, json.Unmarshal([]byte(task.Input), &jsonObj))
}

func mockWorkflowInstance() *model.WorkflowTaskInstance {
	return &model.WorkflowTaskInstance{
		Input: `{ "order_no": "123456789", "status":1}`,
		Task: &model.WorkflowTask{
			TaskInputFilter: `${ {order_no: .order_no} }`,
		},
	}
}
