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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLegacyWorkflow(t *testing.T) {
	var source = "id: storeorderworkflow\nversion: '1.0'\nspecVersion: '0.8'\nname: Store Order Management Workflow\nstart: Receive New Order Event\nstates:\n  - name: Receive New Order Event\n    type: event\n    onEvents:\n      - eventRefs:\n          - NewOrderEvent\n        actions:\n          - functionRef:\n              refName: \"OrderServiceSendEvent\"\n    transition: Check New Order Result\n  - name: Check New Order Result\n    type: switch\n    dataConditions:\n      - name: New Order Successfull\n        condition: \"${{ .order.order_no != '' }}\"\n        transition: Send Order Payment\n      - name: New Order Failed\n        condition: \"${{ .order.order_no == '' }}\"\n        end: true\n    defaultCondition:\n      end: true\n  - name: Send Order Payment\n    type: operation\n    actions:\n      - functionRef:\n          refName: \"PaymentServiceSendEvent\"\n    transition: Check Payment Status\n  - name: Check Payment Status\n    type: switch\n    dataConditions:\n      - name: Payment Successfull\n        condition: \"${{ .payment.order_no != '' }}\"\n        transition: Send Order Shipment\n      - name: Payment Denied\n        condition: \"${{ .payment.order_no == '' }}\"\n        end: true\n    defaultCondition:\n      end: true\n  - name: Send Order Shipment\n    type: operation\n    actions:\n      - functionRef:\n          refName: \"ShipmentServiceSendEvent\"\n    end: true\nevents:\n  - name: NewOrderEvent\n    source: store/order\n    type: online.store.newOrder\nfunctions:\n  - name: OrderServiceSendEvent\n    operation: file://orderService.yaml#sendOrder\n    type: asyncapi\n  - name: PaymentServiceSendEvent\n    operation: file://paymentService.yaml#sendPayment\n    type: asyncapi\n  - name: ShipmentServiceSendEvent\n    operation: file://shipmentService.yaml#sendShipment\n    type: asyncapi\n"
	wf, err := Parse(source)
	assert.Nil(t, err)
	assert.True(t, wf.Legacy)
	assert.Equal(t, "storeorderworkflow", wf.ID)
	assert.Equal(t, "Receive New Order Event", wf.Start)
	assert.Equal(t, 5, len(wf.Tasks))
	assert.Equal(t, "end", wf.Tasks[1].Cases[2].Then)
}

func TestParseV1Workflow(t *testing.T) {
	var source = "document:\n  dsl: '1.0.3'\n  namespace: eventmesh.apache.org\n  name: store-order-management\n  version: '1.0.0'\ndo:\n  - receiveNewOrderEvent:\n      listen:\n        to:\n          one:\n            with:\n              type: online.store.newOrder\n      then: checkNewOrderResult\n  - checkNewOrderResult:\n      switch:\n        - newOrderSuccessful:\n            when: .order_no != \"\"\n            then: sendOrderPayment\n        - newOrderFailed:\n            then: end\n  - sendOrderPayment:\n      call: asyncapi\n      with:\n        operation: file://paymentapp.yaml#sendPayment\n      then: checkPaymentStatus\n  - checkPaymentStatus:\n      switch:\n        - paymentSuccessful:\n            when: .order_no != \"\"\n            then: sendOrderShipment\n        - paymentDenied:\n            then: end\n  - sendOrderShipment:\n      call: asyncapi\n      with:\n        operation: file://expressapp.yaml#sendExpress\n      then: end\n"
	wf, err := Parse(source)
	assert.Nil(t, err)
	assert.False(t, wf.Legacy)
	assert.Equal(t, "store-order-management", wf.ID)
	assert.Equal(t, "1.0.3", wf.DSL)
	assert.Equal(t, "receiveNewOrderEvent", wf.Start)
	assert.Equal(t, 5, len(wf.Tasks))
	assert.Equal(t, TaskTypeListen, wf.Tasks[0].Type)
	assert.Equal(t, TaskTypeSwitch, wf.Tasks[1].Type)
	assert.Equal(t, "file://paymentapp.yaml#sendPayment", wf.Tasks[2].Actions[0].OperationName)
}

func TestParseV1StructuredTasks(t *testing.T) {
	var source = "document:\n  dsl: '1.0.3'\n  namespace: default\n  name: structured-tasks\n  version: '1.0.0'\ndo:\n  - prepare:\n      set:\n        orderId: 1\n      then: batch\n  - batch:\n      do:\n        - enrich:\n            call: http\n            with:\n              endpoint: https://example.com/enrich\n      then: choice\n  - choice:\n      switch:\n        - ok:\n            when: .ok == true\n            then: done\n        - default:\n            then: end\n  - done:\n      raise:\n        error:\n          type: https://serverlessworkflow.io/spec/1.0.0/errors/runtime\n      then: end\n"
	wf, err := Parse(source)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(wf.FlattenTasks()))
	assert.Equal(t, TaskTypeSet, wf.Tasks[0].Type)
	assert.Equal(t, TaskTypeDo, wf.Tasks[1].Type)
	assert.Equal(t, TaskTypeRaise, wf.Tasks[3].Type)
	assert.Equal(t, "enrich", wf.Tasks[1].Children[0].Name)
}

func TestValidateDuplicateTaskNames(t *testing.T) {
	source := "document:\n  dsl: '1.0.3'\n  name: dup\n  version: '1.0.0'\ndo:\n  - a:\n      set:\n        x: 1\n      then: a\n  - a:\n      set:\n        y: 2\n      then: end\n"
	_, err := Parse(source)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestValidateUnknownThenV1(t *testing.T) {
	source := "document:\n  dsl: '1.0.3'\n  name: unknown-then\n  version: '1.0.0'\ndo:\n  - task1:\n      set:\n        x: 1\n      then: ghost\n"
	_, err := Parse(source)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "ghost")
}

func TestValidateUnknownThenLegacy(t *testing.T) {
	source := "id: jump\nversion: '1.0'\nspecVersion: '0.8'\nstart: task1\nstates:\n  - name: task1\n    type: operation\n    actions:\n      - functionRef:\n          refName: fn\n    transition: nowhere\nfunctions:\n  - name: fn\n    operation: http://x\n    type: rest\n"
	_, err := Parse(source)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "nowhere")
}

func TestValidateEmpty(t *testing.T) {
	wf, err := Parse("")
	assert.Nil(t, err)
	assert.Nil(t, wf)
}

func TestValidateSetTaskTypeRaw(t *testing.T) {
	source := "document:\n  dsl: '1.0.3'\n  name: set-test\n  version: '1.0.0'\ndo:\n  - init:\n      set:\n        count: 0\n      then: end\n"
	wf, err := Parse(source)
	assert.Nil(t, err)
	assert.NotNil(t, wf)
	assert.Equal(t, 1, len(wf.Tasks))
	assert.NotEmpty(t, wf.Tasks[0].Raw)
}
