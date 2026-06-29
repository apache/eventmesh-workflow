# EventMesh Workflow Design Document

> **Version**: v1.0.0 | **Updated**: 2026-06-29 | **DSL**: Serverless Workflow 1.0.3 (0.8 compatible)

---

## 1. Overview

EventMesh Workflow is the workflow runtime in the Apache EventMesh ecosystem. It parses Serverless Workflow DSL definitions, builds task graphs, schedules execution, and orchestrates microservices on the EventMesh event bus.

### 1.1 Core Capabilities

| Capability | Description |
| --- | --- |
| **DSL Parsing** | Supports Serverless Workflow 1.0.3 and 0.8 dual formats, zero external DSL dependency |
| **Task Graph** | Compiles DSL descriptions into a DAG (START -> Task -> Transition -> END) |
| **Runtime Scheduling** | Queue-based multi-instance scheduling engine for operation/event/switch/structural tasks |
| **EventMesh Integration** | Queries operation definitions via gRPC Catalog, publishes events asynchronously |
| **A2A Bridge** | Workflow exposed as A2A Agent; supports calling external A2A Agents |
| **Structural Tasks** | 9 built-in executors for fork/try/for/do/set and other structural tasks |

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                    Controller (HTTP API)                   │
│   POST /workflow   GET /workflow   DELETE /workflow       │
│   POST /workflow/start   GET /workflow/instances          │
└───────────────────────┬──────────────────────────────────┘
                        │ DAL (GORM + MySQL)
                        ▼
┌──────────────────────────────────────────────────────────┐
│              DSL Parser (third_party/swf)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ V1 Parser    │  │ Legacy Parser│  │ Task Validator  │  │
│  │ (document+do)│  │ (id+states)  │  │ (name/then/graph)│  │
│  └──────────────┘  └──────────────┘  └────────────────┘  │
└───────────────────────┬──────────────────────────────────┘
                        │ Workflow + Tasks + Relations
                        ▼
┌──────────────────────────────────────────────────────────┐
│                   Flow Engine                             │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ Start()      │  │ Transition() │  │ Queue.Publish()│  │
│  │ (instantiate) │  │ (state trans) │  │ (task enqueue) │  │
│  └──────────────┘  └──────────────┘  └────────────────┘  │
└───────────────────────┬──────────────────────────────────┘
                        │ ObserveQueue
                        ▼
┌──────────────────────────────────────────────────────────┐
│                   Task Executors                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐ │
│  │Operation │ │  Event   │ │  Switch  │ │Local Runtime │ │
│  │ Task     │ │  Task    │ │  Task    │ │  Task        │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────┘ │
└───────────────────────┬──────────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌──────────┐ ┌──────────┐ ┌──────────────┐
   │ EventMesh│ │   A2A    │ │  Local Ops   │
   │  Queue   │ │  Bridge  │ │ (set/wait/…) │
   └──────────┘ └──────────┘ └──────────────┘
```

### 2.1 Component Roles

| Component | Path | Responsibility |
| --- | --- | --- |
| **Controller** | `cmd/controller/` | HTTP API service, Gin framework, Swagger docs |
| **Flow Engine** | `flow/` | Workflow instance start, task state transitions |
| **DSL Parser** | `third_party/swf/` | YAML -> Workflow/Task structs, dual-format parsing |
| **DAL** | `internal/dal/` | GORM + MySQL persistence, task graph construction |
| **Task Executors** | `internal/task/` | 4 executor types: operation/event/switch/local-runtime |
| **Queue** | `internal/queue/` | Task queue abstraction: In-Memory / EventMesh |
| **Filter** | `internal/filter/` | JQ expression input/output data filtering |
| **A2A Bridge** | `internal/bridge/` | A2A protocol client + WorkflowAgent HTTP endpoint |
| **Metrics** | `internal/metrics/` | Prometheus metrics collection |

---

## 3. DSL Parser Design

### 3.1 Dual-Format Compatibility

The parser entry point `swf.Parse()` detects the DSL version by checking for a `document` key at the top level:

```go
func Parse(source string) (*Workflow, error) {
    var raw map[string]interface{}
    yaml.Unmarshal([]byte(source), &raw)
    if _, ok := raw["document"]; ok {
        return parseV1Workflow(raw)    // DSL 1.0.3
    }
    return parseLegacyWorkflow(raw)    // DSL 0.8
}
```

### 3.2 DSL 1.0.3 Parsing Flow

```
YAML Source
    │
    ▼
parseV1Workflow(raw)
    │
    ├─ document.name / version / dsl → Workflow metadata
    ├─ raw["do"] → parseV1TaskList → []*Task
    ├─ raw["use"].functions → map[string]*Function
    ├─ raw["schedule"] → Schedule (cron/start/after)
    ├─ raw["input"] → data input filter
    ├─ raw["output"].as → output filter
    │
    ▼
wf.Validate()
    ├─ FlattenTasks()  → flatten nested tasks
    ├─ Check task name uniqueness
    ├─ Validate then/switch.when targets
    │
    ▼
*Workflow (normalized result)
```

### 3.3 Task Type Detection

```go
func detectV1TaskType(def map[string]interface{}) string {
    if _, ok := def["call"]; ok  → TaskTypeOperation
    // Detect standard DSL keys:
    // switch / set / do / fork / for / try
    // wait / raise / run / emit / listen
    return TaskTypeOperation  // default fallback
}
```

### 3.4 Data Model

```
Workflow
  ├─ ID, Name, Version, DSL, Namespace
  ├─ Start: entry task name
  ├─ Tasks: []*Task
  │   ├─ Name, Type, InputFilter, OutputFilter
  │   ├─ InlineData, Then, ExplicitThen
  │   ├─ Actions: []*Action {OperationName, OperationType}
  │   ├─ Cases: []*SwitchCase {Name, Condition, Then, IsDefault}
  │   └─ Children: []*Task (nested sub-tasks)
  ├─ Functions: map[string]*Function {Name, Operation, Type}
  └─ Schedule: {Start, Cron, After}
```

---

## 4. Task Graph Construction

### 4.1 DAL.create() Flow

```go
func (w *workflowDALImpl) create(ctx context.Context, tx *gorm.DB, record *model.Workflow) error {
    wf, _ := swf.Parse(record.Definition)
    
    // 1. Build WorkflowTask (flatten all nested tasks)
    tasks := w.buildTask(wf)          // FlattenTasks → model.WorkflowTask
    
    // 2. Build WorkflowTaskRelation (inter-task edges)
    relations := w.buildTaskRelation(wf, tasks)
    
    // 3. Concurrent write to MySQL
}
```

### 4.2 Task Relation Construction Rules

```
START → first task (workflow.Start)

For each task:
  ┌─ Switch type → buildSwitchTaskRelation
  │                 each case.Then → target TaskID (or END)
  ├─ Fork type   → buildForkTaskRelation
  │                 each child → independent branch edge
  ├─ Explicit then → resolveNextTaskID(Then)
  │                 "end"/"exit"/"continue" → END
  │                 named target → taskIDs[name]
  └─ Default      → Children[0].Name (nested structure) or END
```

### 4.3 Fork Task Graph Example

```
      ┌─────────┐
      │  FORK   │
      └────┬────┘
     ┌─────┴─────┐
     ▼           ▼
  branch_a    branch_b
     │           │
     ▼           ▼
    END         END
```

During fork construction, all child branches are published in parallel via `publishNextTasks()`.

---

## 5. Task Executors

### 5.1 Task Dispatch Factory

```go
func New(instance *model.WorkflowTaskInstance) Task {
    if isLocalRuntimeTask(taskType) → NewLocalRuntimeTask
    switch taskType:
        operation → NewOperationTask
        event     → NewEventTask
        switch    → NewSwitchTask
        default   → NewOperationTask  // fallback
}
```

### 5.2 Executor Types

| Executor | Handles | Run() Behavior |
| --- | --- | --- |
| **OperationTask** | call / listen / run | Executes catalog operations, publishes EventMesh events / A2A calls |
| **EventTask** | event / listen | Delegates to OperationTask |
| **SwitchTask** | switch | JQ condition matching → branch selection → publishOrComplete |
| **LocalRuntimeTask** | set / do / fork / for / try / wait / raise / emit | Built-in executors, see §5.3 |

### 5.3 LocalRuntimeTask Built-in Executors

| Task | execute() Method | Logic |
| --- | --- | --- |
| **set** | executeSet() | JQ Object() applies set expressions to input JSON |
| **do** | executeDo() | Sequentially executes sub-tasks in do list (set / raise) |
| **fork** | (no local exec) | DAL builds multi-branch relations; run() then publishNextTasks |
| **for** | executeFor() | Parses JSON array → iterates each element through do body set tasks |
| **try** | executeTry() | Sequentially attempts try list tasks; skips on failure |
| **wait** | executeWait() | time.ParseDuration → time.Sleep |
| **raise** | executeRaise() | Constructs structured error and returns it |
| **run** | executeRun() | Publishes to EventMesh (delegates publishEvent) |
| **emit** | executeEmit() | Publishes to EventMesh (delegates publishEvent) |

### 5.4 Data Flow

```
Input ──→ [InputFilter] ──→ Task.Execute() ──→ output ──→ [OutputFilter] ──→ Next Task
                                    │
                          publishNextOrComplete()
                          (publish next task to Queue)
```

- **InputFilter**: Applied during task enqueue via `FilterWorkflowTaskInputData()`
- **OutputFilter**: Applied before LocalRuntimeTask.Run() returns via `FilterWorkflowTaskOutputData()`
- Filters use JQ expression syntax: `${ .field }`

---

## 6. Queue & Scheduling

### 6.1 Queue Abstraction

```go
type ObserveQueue interface {
    Publish(instances []*model.WorkflowTaskInstance) error
    Subscribe(handler func(*model.WorkflowTaskInstance))
    UnSubscribe() error
}
```

Two implementations:
- **InMemoryQueue**: Dev/test environment, memory channel
- **EventMeshQueue**: Production, published via EventMesh SDK

### 6.2 Scheduling Flow

```
Engine.Start(param)
  → SelectStartTask → find the first task linked to START
  → InsertInstance → create workflow instance record
  → Queue.Publish(taskInstance) → enqueue

Consumer processing:
  → task.New(instance) → create corresponding executor
  → task.Run()
    → OperationTask: publish EventMesh event (async) + enqueue next task (sleep)
    → EventTask: delegate to OperationTask
    → SwitchTask: match condition → publish next task
    → LocalRuntimeTask: synchronous execution → publish next task

Engine.Transition(param)  ← EventMesh callback
  → SelectTransitionTask (sleep → wait)
  → Queue.Publish → re-enqueue for consumption
```

---

## 7. A2A Bridge

### 7.1 Architecture

```
┌────────────────────┐       A2A Protocol        ┌──────────────────┐
│ EventMesh Workflow │ ──── call a2a: ──────→    │  External A2A    │
│  (A2A Client)      │ ←─── task result ────     │  Agent           │
└────────────────────┘                            └──────────────────┘

┌────────────────────┐       A2A Protocol        ┌──────────────────┐
│  External A2A      │ ──── POST /a2a/tasks ──→  │ EventMesh Workflow│
│  Client            │ ←─── task status ─────    │  (WorkflowAgent)  │
└────────────────────┘                            └──────────────────┘
```

### 7.2 A2A Client

`A2AExecutor` implements polling-based task execution:

```go
SendTask(input, metadata) → pollUntilComplete(taskID)
  // Poll /a2a/tasks/{id} every 2 seconds, max 30 attempts
```

Task detection via `isA2ATask()`: `OperationType == "a2a"` or `OperationName` starts with `"a2a"`.

### 7.3 WorkflowAgent (Server)

Exposes workflows as A2A Agents:

```
GET  /.well-known/agent-card.json → Agent Card
POST /a2a/tasks                   → Start workflow instance
GET  /a2a/tasks/{id}              → Query workflow instance status
GET  /a2a/health                  → Health check
```

### 7.4 A2A Message Format (v1.0 Compatible)

```
TaskRequest {
    id, message: { role, parts: [{ type: "text", text }] },
    metadata: { source, timestamp, ... }
}

TaskResponse {
    id, status: "working"|"completed"|"failed",
    message, artifacts: [{ name, parts }], error: { message }
}
```

---

## 8. Database Model

### 8.1 ER Diagram

```
t_workflow ──1:N── t_workflow_task ──1:N── t_workflow_task_action
     │                    │
     │                    └──1:N── t_workflow_task_relation
     │
     └──1:N── t_workflow_instance ──1:N── t_workflow_task_instance
```

### 8.2 Core Tables

| Table | Purpose | Key Fields |
| --- | --- | --- |
| t_workflow | Workflow definition | workflow_id, definition(DSL YAML), version |
| t_workflow_task | Flattened task nodes | task_id, task_name, task_type, task_input_filter, task_output_filter |
| t_workflow_task_action | Task operation definitions | operation_name, operation_type |
| t_workflow_task_relation | Inter-task edges | from_task_id, to_task_id, condition |
| t_workflow_instance | Workflow instances | workflow_instance_id, workflow_status |
| t_workflow_task_instance | Task instances | task_instance_id, status, input |

### 8.3 State Machine

```
Task instance: SLEEP(1) → WAIT(2) → PROCESS(3) → SUCCESS(4) / FAIL(5)

Workflow instance: PROCESS(1) → SUCCESS(2)
```

- **SLEEP**: OperationTask enqueues the next task immediately after publishing an event, with SLEEP status; waits for EventMesh callback Transition
- **WAIT**: Ready, awaiting consumption
- **PROCESS**: Being consumed (actual execution)
- **SUCCESS** / **FAIL**: Terminal state

---

## 9. Data Filtering

JQ (itchyny/gojq) based JSON data filtering:

```go
FilterWorkflowTaskInputData(task)
  → filterJsonData(task.TaskInputFilter, task.Input)
    → jqer.Object(jsonObj, "${ filterExp }")

FilterWorkflowTaskOutputData(input, outputFilter)
  → filterJsonData(outputFilter, input)
    → jqer.Object(jsonObj, "${ filterExp }")
```

Expression normalization: bare expressions are auto-wrapped as `${ expr }`.

---

## 10. Module Dependencies

```
cmd/controller ──→ internal/dal
cmd/engine ──→ flow ──→ internal/dal
                     ──→ internal/queue

internal/dal ──→ third_party/swf
             ──→ internal/dal/model
             ──→ internal/util

internal/task ──→ internal/dal
             ──→ internal/dal/model
             ──→ internal/queue
             ──→ internal/bridge
             ──→ internal/filter
             ──→ third_party/jqer
             ──→ third_party/swf (type constants)
```

Zero external SWF SDK dependency. `third_party/swf/` is a fully self-contained DSL parser.

---

## 11. Design Decisions

| Decision | Rationale |
| --- | --- |
| Zero external DSL dependency | sdk-go/v2 only supports 0.8 and cannot upgrade; fully controllable custom parser |
| Dual-format compatibility | Gradual migration; existing 0.8 workflows unaffected |
| Flattened task graph | Unified task graph model simplifies runtime scheduling |
| LocalRuntimeTask | Built-in executors avoid external EventMesh call overhead for structural tasks |
| A2A polling mode | Simple and reliable; suitable for non-streaming Agent call scenarios |
| GORM + MySQL | Consistent with EventMesh ecosystem; transaction support |
| JQ expression filtering | Lightweight JSON transformation, zero new dependencies (reuses gojq) |
