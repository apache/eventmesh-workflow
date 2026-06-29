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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseConfigFileLegacy(t *testing.T) {
	data, err := os.ReadFile("../../configs/testcreateworkflow.yaml")
	if err != nil {
		t.Skipf("legacy config file not found: %v", err)
		return
	}
	wf, err := Parse(string(data))
	assert.Nil(t, err)
	assert.NotNil(t, wf)
	assert.True(t, wf.Legacy)
	assert.NotEmpty(t, wf.Tasks)
	t.Logf("legacy workflow: id=%s, tasks=%d", wf.ID, len(wf.Tasks))
}

func TestParseConfigFileV1(t *testing.T) {
	data, err := os.ReadFile("../../configs/testcreateworkflow-v1.yaml")
	if err != nil {
		t.Skipf("v1 config file not found: %v", err)
		return
	}
	wf, err := Parse(string(data))
	assert.Nil(t, err)
	assert.NotNil(t, wf)
	assert.False(t, wf.Legacy)
	assert.NotEmpty(t, wf.Tasks)
	t.Logf("v1 workflow: id=%s, dsl=%s, tasks=%d", wf.ID, wf.DSL, len(wf.FlattenTasks()))
}

func TestParseLegacyConfigBuiltTaskTypes(t *testing.T) {
	data, err := os.ReadFile("../../configs/testcreateworkflow.yaml")
	if err != nil {
		t.Skipf("config file not found: %v", err)
		return
	}
	wf, _ := Parse(string(data))
	assert.NotNil(t, wf)
	flattened := wf.FlattenTasks()
	assert.GreaterOrEqual(t, len(flattened), 1)
	for _, task := range flattened {
		assert.NotEmpty(t, task.Name)
		assert.NotEmpty(t, task.Type)
		t.Logf("task: name=%s type=%s", task.Name, task.Type)
	}
}

func TestParseV1ConfigBuiltTaskTypes(t *testing.T) {
	data, err := os.ReadFile("../../configs/testcreateworkflow-v1.yaml")
	if err != nil {
		t.Skipf("config file not found: %v", err)
		return
	}
	wf, _ := Parse(string(data))
	assert.NotNil(t, wf)
	flattened := wf.FlattenTasks()
	assert.GreaterOrEqual(t, len(flattened), 1)
	for _, task := range flattened {
		assert.NotEmpty(t, task.Name)
		assert.NotEmpty(t, task.Type)
		t.Logf("task: name=%s type=%s", task.Name, task.Type)
	}
}
