# Feature Request: Upgrade to Serverless Workflow DSL 1.0.3

**Labels**: `enhancement`

---

## Feature Request

### Motivation

`eventmesh-workflow` currently targets Serverless Workflow DSL `0.8` and depends on the external `sdk-go/v2` library for model parsing. This creates several limitations:

1. **Outdated DSL spec** — DSL 0.8 uses the legacy `id` + `states` model. DSL 1.0.3 introduces structured constructs (`do`, `fork`, `try`, `for`) that enable richer workflow orchestration.
2. **External dependency** — `sdk-go/v2` pins the project to a third-party model layer with its own compatibility and update cadence.
3. **Missing structured task executors** — fork/branch parallelism, error handling (try/catch), and loop iteration (for) have no runtime support.
4. **No A2A (Agent-to-Agent) integration** — workflows cannot participate as A2A agents and cannot invoke external A2A agents as task steps.

### Desired Outcomes

| # | Requirement | Priority |
|---|---|---|
| 1 | Parse DSL 1.0.3 `document` + `do` workflows with zero external SWF dependency | P0 |
| 2 | Maintain backward compatibility with DSL 0.8 `id` + `states` format | P0 |
| 3 | Execute 12 task types: `call`, `listen`, `switch`, `set`, `do`, `fork`, `for`, `try`, `wait`, `raise`, `run`, `emit` | P0 |
| 4 | Support fork parallel branch execution | P1 |
| 5 | Support try/catch error handling semantics | P1 |
| 6 | Support for loop iteration over JSON arrays | P1 |
| 7 | Support `input.from` / `output.as` filter pipeline with JQ expressions | P1 |
| 8 | Support workflow schedule fields (`start`, `cron`, `after`) | P2 |
| 9 | Bidirectional A2A bridge — invoke A2A agents from workflows, expose workflows as A2A agents | P2 |
| 10 | Maintain existing EventMesh integration (AsyncAPI/catalog operations) | P0 |

---

## Solution

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                  DSL 1.0.3 YAML Document                          │
│  document → use → do [task, fork, try, for, ...] → output        │
└───────────────────────┬──────────────────────────────────────────┘
                        │ swf.Parse()
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│              third_party/swf/ (Zero-External-Dep Parser)          │
│  model.go: Workflow, Task, SwitchCase, Function structs          │
│  swf.go:   ParseV1() + ParseLegacy(), FlattenTasks(), Validate() │
└───────────────────────┬──────────────────────────────────────────┘
                        │ DAL: buildTaskGraph()
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Task Runtime Dispatcher                       │
│  ┌────────────┐ ┌───────────┐ ┌──────────────┐ ┌──────────────┐ │
│  │ HTTP/GRPC  │ │  Switch   │ │ LocalRuntime │ │   A2A Exec   │ │
│  │ Executor   │ │ Executor  │ │  Executor    │ │   (Bridge)   │ │
│  │            │ │           │ │              │ │              │ │
│  │ call       │ │ switch    │ │ set/do/fork  │ │ a2a call     │ │
│  │ operation  │ │ when      │ │ for/try/wait │ │ agent invoke │ │
│  │            │ │           │ │ raise/run    │ │              │ │
│  └────────────┘ └───────────┘ └──────────────┘ └──────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### 1. Custom SWF Parser (`third_party/swf/`)

Replaced `sdk-go/v2` with a custom parser that handles both DSL generations:

- **`model.go`**: Go structs for `Workflow`, `Task`, `SwitchCase`, `Function`, `Schedule`, `Document`, etc. Includes `FlattenTasks()` for tree→flat list conversion and `Validate()` for schema checks.
- **`swf.go`**: `Parse()` dispatches to `ParseV1()` (DSL 1.0.3 `document` shape) or `ParseLegacy()` (DSL 0.8 `states` shape) based on `specVersion`. Handles `input`, `output`, `schedule`, `data` top-level and task-level fields.

**Key design**: All 12 task types are parsed into a unified `Task` struct. Nested tasks (inside `fork.branches[].do`, `try.do`, `for.do`) are flattened during DAL graph construction, not during parse.

### 2. Structured Task Executors

Added `LocalRuntime` executor (`internal/task/local_runtime.go`) for tasks that run inside the workflow engine without external calls:

| Task | Implementation | Semantics |
|------|---------------|-----------|
| **set** | `executeSet()` | Assign JQ expressions to workflow data context |
| **do** | `executeDo()` | Execute children sequentially, propagate output |
| **fork** | `executeFork()` | Publish all branch-first tasks in parallel via `publishNextTasks()` |
| **for** | `executeFor()` | Parse JSON array from `input`, iterate each element through `do` body |
| **try** | `executeTry()` | Execute `do` children; on error, run `catch` children (parsed from `catch.when`) |
| **wait** | `executeWait()` | Sleep for configured duration |
| **raise** | `executeRaise()` | Raise a configurable error |
| **run** | `executeRun()` | Execute inline script/container (placeholder) |
| **emit** | `executeEmit()` | Emit event data (placeholder) |

**Fork relation building** in DAL: `buildForkBranchRelations()` creates separate `WorkflowTaskRelation` entries for each branch head task, enabling parallel dispatch.

**Try relation building**: `catch.when` is parsed as `Children` of the try task, flagged with `TaskType = "try"` to distinguish from normal do-children.

### 3. A2A Bidirectional Bridge

```
┌─────────────────────┐         ┌──────────────────────┐
│   Workflow Engine   │         │   External A2A Agent │
│                     │  HTTP   │                      │
│  OperationTask      │────────▶│  POST /tasks/send    │
│  operationType=a2a  │  poll   │                      │
│                     │◀────────│  GET /tasks/{id}     │
│     A2AExecutor     │         │                      │
└─────────────────────┘         └──────────────────────┘

┌─────────────────────┐         ┌──────────────────────┐
│   External Client   │         │   Workflow Engine    │
│                     │  HTTP   │                      │
│  A2A Client         │────────▶│  POST /a2a/agent     │
│  (SendTask)         │         │                      │
│                     │         │  WorkflowAgent       │
│                     │         │  (exposes workflows  │
│                     │         │   as A2A agents)     │
└─────────────────────┘         └──────────────────────┘
```

- **`A2AExecutor`** (`internal/bridge/a2a_executor.go`): Polling-based A2A client. Calls `POST /tasks/send` with task definition, then polls `GET /tasks/{id}` until completion or timeout. Integrated into `OperationTask` via `OperationType="a2a"` dispatch.
- **`WorkflowAgent`** (`internal/bridge/workflow_agent.go`): HTTP endpoint that exposes workflows as A2A agents. Handles incoming `POST /a2a/agent` requests, maps to workflow execution, returns `AgentCard` metadata and task results.
- **`a2a_integration.go`**: `runA2AAction()` bridges task discovery → A2A executor invocation.

### 4. Data Pipeline

**Input filtering**: `input.from` JQ expressions extract data from workflow context before task execution.

**Output filtering**: `TaskOutputFilter` stored in DAL model `WorkflowTask`. After each task completes, `FilterWorkflowTaskOutputData()` applies JQ expressions from `output.as` to shape the result before storing to workflow data context.

**Schedule fields**: Parsed at workflow level: `start` (ISO datetime), `cron` (expression), `after` (delay duration). Stored in workflow document, usable for scheduling engine integration.

### 5. Backward Compatibility

All existing DSL 0.8 workflows pass through `ParseLegacy()`, which maps `states` → flat task list + implicit transitions. The internal task graph model (`WorkflowTask` + `WorkflowTaskRelation`) is unchanged, so runtime execution is transparent to the DSL version.

### Test Coverage

```
third_party/swf/swf_test.go                  12 tests  (V1 parse, legacy parse, Fork/Try/For/Schedule/Output, Validate)
third_party/swf/parser_integration_test.go    4 tests  (real YAML fixture parsing)
internal/filter/data_filter_test.go           1 test   (output filter pipeline)
─────────────────────────────────────────────────────────
Total: 17 tests  |  go test ./... : PASS  |  go vet ./... : clean
```

### Files Changed (Code Only)

```
new file:   third_party/swf/model.go
new file:   third_party/swf/swf.go
new file:   third_party/swf/swf_test.go
new file:   third_party/swf/parser_integration_test.go
new file:   internal/task/local_runtime.go
new file:   internal/task/runtime_util.go
new file:   internal/bridge/a2a_types.go
new file:   internal/bridge/a2a_executor.go
new file:   internal/bridge/workflow_agent.go
new file:   internal/task/a2a_integration.go
modified:   internal/dal/workflow.go
modified:   internal/dal/model/workflow_task.go
modified:   internal/task/task.go
modified:   internal/task/operation_task.go
modified:   internal/constants/constants.go
modified:   internal/filter/data_filter.go
modified:   go.mod
modified:   configs/testcreateworkflow-v1.yaml
```

### Related Commits

| Commit | Description |
|--------|-------------|
| `3a1662f` | feat: upgrade to Serverless Workflow DSL 1.0.3 with full structured task support |
| `514db08` | feat: fork/try/for executors + output/schedule/data + A2A bridge |
