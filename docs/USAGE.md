# EventMesh Workflow User Guide

> **Version**: v1.0.0 | **Updated**: 2026-06-29

---

## 1. Quick Start

### 1.1 Prerequisites

| Dependency | Version | Purpose |
| --- | --- | --- |
| Go | >= 1.18 | Compilation & runtime |
| MySQL | >= 5.7 | Persistent storage |
| EventMesh | - | Event bus / Catalog service |

### 1.2 Initialize Database

```bash
mysql -u root -p < distribution/mysql-schema.sql
```

### 1.3 Build

```bash
make build
# Output: bin/eventmesh-workflow
```

### 1.4 Run

```bash
# Controller (HTTP API)
./bin/eventmesh-workflow controller --config configs/controller.yaml

# Engine (Task Runner)
./bin/eventmesh-workflow engine --config configs/engine.yaml
```

The Controller default port is set in `configs/controller.yaml` under `server.port`. Swagger docs are at `/swagger/index.html`.

---

## 2. DSL Writing Guide

### 2.1 DSL 1.0.3 (Recommended)

Use `document` + `do` structure:

```yaml
document:
  dsl: '1.0.3'
  namespace: eventmesh.apache.org
  name: my-first-workflow
  version: '1.0.0'

do:
  - step1:
      call: http
      with:
        endpoint: https://api.example.com/data
      then: step2

  - step2:
      set:
        result: '${ .data.value }'
      then: end
```

### 2.2 DSL 0.8 (Legacy Compatible)

Use `id` + `states` structure:

```yaml
id: my-legacy-workflow
version: '1.0'
specVersion: '0.8'
start: FirstState
states:
  - name: FirstState
    type: operation
    actions:
      - functionRef:
          refName: "myFunction"
    transition: SecondState
  - name: SecondState
    type: operation
    actions:
      - functionRef:
          refName: "myFunction2"
    end: true
functions:
  - name: myFunction
    operation: file://app.yaml#action
    type: asyncapi
```

Both formats can coexist; the parser auto-detects.

---

## 3. Task Types Reference

### 3.1 call — Invoke External Operations

```yaml
- sendOrder:
    call: asyncapi
    with:
      operation: file://order.yaml#sendOrder
    then: nextStep
```

Supported call types:

| call Value | with Params | Resolution Behavior |
| --- | --- | --- |
| `http` | `endpoint` | HTTP endpoint as operation name |
| `asyncapi` | `operation` / `channel` / `document` | EventMesh catalog query |
| `openapi` | `operationId` / `operation` / `document` | REST API operation |
| `grpc` | `service` / `method` | gRPC service method |
| `a2a` | `endpoint` | A2A Agent URL |

A2A call example:

```yaml
- askAgent:
    call: a2a
    with:
      endpoint: http://localhost:9090
    then: processResult
```

### 3.2 listen — Listen for Events

```yaml
- waitForEvent:
    listen:
      to:
        one:
          with:
            type: order.created
            source: store/order
    then: processOrder
```

### 3.3 switch — Conditional Branching

```yaml
- checkResult:
    switch:
      - success:
          when: .status == "ok"
          then: handleSuccess
      - failure:
          when: .status == "error"
          then: handleError
      - otherwise:
          then: end
```

Condition expressions use JQ syntax; `.field` references input JSON fields.

### 3.4 set — Data Transformation

```yaml
- transform:
    set:
      fullName: '${ .firstName + " " + .lastName }'
      amount: '${ .price * .quantity }'
    then: nextStep
```

### 3.5 do — Sub-task Sequence

```yaml
- processBatch:
    do:
      - stepA:
          set:
            validated: true
      - stepB:
          set:
            enriched: true
    then: nextStep
```

Sub-tasks within `do` currently support `set` and `raise`.

### 3.6 fork — Parallel Branches

```yaml
- parallelTasks:
    fork:
      branches:
        - notifyEmail:
            call: http
            with:
              endpoint: https://api.example.com/email
        - notifySMS:
            call: http
            with:
              endpoint: https://api.example.com/sms
    then: joinPoint
```

### 3.7 for — Loop Iteration

```yaml
- processItems:
    for:
      each: .items
      do:
        - enrichItem:
            set:
              processed: true
              timestamp: '${ now }'
    then: nextStep
```

### 3.8 try — Error Handling

```yaml
- safeOperation:
    try:
      - riskyTask:
          call: http
          with:
            endpoint: https://api.example.com/may-fail
    catch:
      when:
        - fallback:
            set:
              status: fallback
              error: '${ .message }'
    then: continue
```

### 3.9 wait — Delay

```yaml
- pause:
    wait:
      seconds: '10s'
    then: nextStep
```

Supports Go duration format: `10s`, `1m`, `500ms`, `1h30m`.

### 3.10 raise — Throw Error

```yaml
- validate:
    set:
      error: '${ .result }'
    then: checkError

- checkError:
    switch:
      - hasError:
          when: .error != null
          then: raiseError
    then: end

- raiseError:
    raise:
      error:
        type: ValidationError
        status: '400'
        title: Input Validation Failed
        detail: '${ .error }'
```

### 3.11 run — Publish Event

```yaml
- fireEvent:
    run:
      with:
        event: order.processed
    then: end
```

### 3.12 emit — Emit Event (same as run)

```yaml
- emitEvent:
    emit:
      event: notification.sent
    then: end
```

---

## 4. Data Input & Output

### 4.1 Workflow-Level Input

```yaml
document:
  dsl: '1.0.3'
  name: order-workflow
  version: '1.0.0'

input:
  from: ${ .order }

do:
  - step1:
      call: asyncapi
      with:
        operation: file://order.yaml#process
```

The JSON passed when starting a workflow is filtered through `input.from` before being passed to the first task.

### 4.2 Task-Level Input/Output Filters

```yaml
- step1:
    call: http
    with:
      endpoint: https://api.example.com/data
    input:
      from: ${ { userId: .user.id, amount: .price } }
    output:
      as: ${ { result: .data } }
    then: step2
```

- `input.from`: Extracts fields from upstream data as task input
- `output.as`: Extracts fields from task output to pass downstream

### 4.3 Inline Data

```yaml
- step1:
    set:
      greeting: '${ "Hello, " + .name }'
    data: '{"name": "World"}'
    then: end
```

The `data` field provides static initial data; lower priority than externally passed input.

---

## 5. Schedule Configuration

```yaml
document:
  dsl: '1.0.3'
  name: daily-report
  version: '1.0.0'

schedule:
  start: '2026-07-01T00:00:00Z'
  cron: '0 0 9 * * ?'
  after: 'PT5M'

do:
  - generateReport:
      call: http
      with:
        endpoint: https://api.example.com/report
      then: end
```

| Field | Description |
| --- | --- |
| `start` | ISO 8601 datetime, schedule start time |
| `cron` | Cron expression |
| `after` | ISO 8601 interval (PT5M = 5 minutes) |

---

## 6. Flow Control

### 6.1 then Directive

```yaml
then: nextTaskName    # Jump to named task
then: end             # Terminate workflow
then: exit            # Same as end
then: continue        # Same as end (loop semantics not yet implemented)
```

### 6.2 Implicit Sequencing

If a task has no explicit `then` but has nested sub-tasks (do/fork/for/try), it defaults to the first child task.

---

## 7. REST API Reference

### 7.1 Workflow CRUD

| Method | Path | Description |
| --- | --- | --- |
| POST | `/workflow` | Create/update workflow |
| GET | `/workflow` | List workflows |
| GET | `/workflow/:workflowId` | Get workflow details |
| DELETE | `/workflow/:workflowId` | Delete workflow |
| GET | `/workflow/instances` | Query running instances |

### 7.2 Create / Update Workflow

```bash
curl -X POST http://localhost:8080/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "order-management",
    "workflow_name": "Order Management Workflow",
    "definition": "document:\n  dsl: \"1.0.3\"\n  name: order-management\n  version: \"1.0.0\"\n  namespace: eventmesh.apache.org\ndo:\n  - receiveOrder:\n      listen:\n        to:\n          one:\n            with:\n              type: online.store.newOrder\n      then: end"
  }'
```

**Note**: `definition` is the full DSL YAML text. When creating, `workflow_id` must match `document.name` or `id` in the DSL.

### 7.3 List Workflows

```bash
curl "http://localhost:8080/workflow?page=1&size=20"
```

Response:

```json
{
  "total": 10,
  "workflows": [
    {
      "workflow_id": "order-management",
      "workflow_name": "Order Management Workflow",
      "version": "1.0.0",
      "total_instances": 5,
      "total_running_instances": 3,
      "total_failed_instances": 2
    }
  ]
}
```

### 7.4 Delete Workflow

```bash
curl -X DELETE http://localhost:8080/workflow/order-management
```

Performs soft delete (status → -1); related tasks, relations, and instances are also marked.

---

## 8. A2A Integration

### 8.1 Workflow as Agent

Start the built-in A2A endpoint:

```go
agent := bridge.NewWorkflowAgent("my-workflow", "http://localhost:9090")
mux := http.NewServeMux()
agent.RegisterRoutes(mux)
http.ListenAndServe(":9090", mux)
```

External A2A Client calls:

```bash
# Get Agent Card
curl http://localhost:9090/.well-known/agent-card.json

# Submit task (triggers workflow execution)
curl -X POST http://localhost:9090/a2a/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "role": "user",
      "parts": [{"type": "text", "text": "{\"order_id\": \"12345\"}"}]
    }
  }'

# Query status
curl http://localhost:9090/a2a/tasks/{task_id}
```

### 8.2 Workflow Calling A2A Agent

Configure `call: a2a` in DSL:

```yaml
- aiAnalysis:
    call: a2a
    with:
      endpoint: http://ai-agent:8080
    then: processResult
```

---

## 9. Complete Examples

### 9.1 Order Processing Workflow (DSL 1.0.3)

File: `configs/testcreateworkflow-v1.yaml`

```yaml
document:
  dsl: '1.0.3'
  namespace: eventmesh.apache.org
  name: store-order-management
  version: '1.0.0'

do:
  - receiveNewOrderEvent:
      listen:
        to:
          one:
            with:
              type: online.store.newOrder
              source: store/order
      then: checkNewOrderResult

  - checkNewOrderResult:
      switch:
        - newOrderSuccessful:
            when: .order_no != ""
            then: sendOrderPayment
        - newOrderFailed:
            then: end

  - sendOrderPayment:
      call: asyncapi
      with:
        operation: file://paymentapp.yaml#sendPayment
      then: checkPaymentStatus

  - checkPaymentStatus:
      switch:
        - paymentSuccessful:
            when: .order_no != ""
            then: sendOrderShipment
        - paymentDenied:
            then: end

  - sendOrderShipment:
      call: asyncapi
      with:
        operation: file://expressapp.yaml#sendExpress
      then: end
```

### 9.2 Data Transform + Parallel Processing

```yaml
document:
  dsl: '1.0.3'
  name: data-pipeline
  version: '1.0.0'

do:
  - fetchData:
      call: http
      with:
        endpoint: https://api.example.com/raw-data
      then: transformData

  - transformData:
      set:
        normalized: '${ .data | map({ id, value: .amount * 100 }) }'
        timestamp: '${ now }'
      then: parallelNotify

  - parallelNotify:
      fork:
        branches:
          - emailNotify:
              call: http
              with:
                endpoint: https://api.example.com/email
          - slackNotify:
              call: http
              with:
                endpoint: https://api.example.com/slack
      then: end
```

---

## 10. Development & Debugging

### 10.1 Run Tests

```bash
make test
```

Test coverage:

| Package | Test Count | Content |
| --- | --- | --- |
| third_party/swf | 17 | V1/legacy parsing, Fork/Try/For, Schedule/Output, validation, integration tests |
| internal/filter | 1 | JQ filtering happy/error paths |

### 10.2 Formatting

```bash
make fmt       # goimports + gofmt
```

### 10.3 Linting

```bash
make lint      # golangci-lint
```

### 10.4 Coverage

```bash
make cover     # Generate HTML coverage report
```

### 10.5 Local Debugging Tips

1. Use **InMemoryQueue** (`engine.yaml`: `flow.queue.store: in-memory`) to avoid EventMesh dependency
2. OperationTask's `runA2AAction` can be verified with a Mock A2A Agent
3. LocalRuntimeTask is fully self-contained and can be tested independently

---

## 11. Configuration Reference

### controller.yaml

```yaml
server:
  port: 8080
  name: eventmesh-workflow-controller

database:
  dsn: "root:password@tcp(127.0.0.1:3306)/db_workflow?charset=utf8mb4&parseTime=True&loc=Local"
  max_idle_conns: 10
  max_open_conns: 100
```

### engine.yaml

```yaml
flow:
  protocol: eventmesh       # eventmesh | http
  queue:
    store: in-memory        # in-memory | eventmesh
    eventmesh:
      topic: workflow-task

catalog:
  server_name: eventmesh-catalog
```

---

## 12. Limitations & Roadmap

### Current Limitations

| Item | Description |
| --- | --- |
| for iteration | Body currently supports only set tasks |
| try/catch | Limited task types within when blocks |
| nested do | Runtime only executes set/raise; complex sub-tasks not yet supported |
| A2A streaming | Streaming responses not yet supported |
| error retry | Global retry_attempts=5, not per-task configurable |

### Planned Features

- [ ] Full task type support inside for/do/try
- [ ] Sub-workflow invocation (subFlow)
- [ ] Conditional retry and timeout configuration
- [ ] A2A streaming support
- [ ] WorkflowAgent full instance status query
- [ ] DSL 1.0.3 auth/error/timeout full field support
