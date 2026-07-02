# eventmesh-workflow

Apache EventMesh workflow runtime — Serverless Workflow DSL `1.0.3` compatible.

## Documentation / 文档

| English | 中文 | Description / 说明 |
| --- | --- | --- |
| [DESIGN.md](docs/DESIGN.md) | [DESIGN_CN.md](docs/DESIGN_CN.md) | Architecture design, components, data model, task executors, A2A bridge |
| [USAGE.md](docs/USAGE.md) | [USAGE_CN.md](docs/USAGE_CN.md) | Quick start, DSL writing guide, 12 task types, REST API, complete examples |

## Key Features

- **Dual DSL Compatibility**: Serverless Workflow 1.0.3 (`document` + `do`) and 0.8 (`id` + `states`)
- **Zero External DSL Dependency**: Custom `third_party/swf/` parser replacing sdk-go/v2
- **12 Task Types**: call / listen / switch / set / do / fork / for / try / wait / raise / run / emit
- **Built-in Structural Executors**: fork (parallel) / try (error handling) / for (loop) / do (sequence)
- **A2A Bidirectional Bridge**: Workflow can call external A2A Agents; can also be exposed as an A2A Agent
- **JQ Data Filtering**: Input/output level JSON filter pipeline
- **4 Runtime Executors**: Operation / Event / Switch / LocalRuntime

## Architecture at a Glance

```
Controller(HTTP API) → DAL(MySQL) ← DSL Parser(swf)
                              ↓
Flow Engine → Queue(In-Memory/EventMesh) → Task Executors
                                              ├─ OperationTask → EventMesh/A2A
                                              ├─ EventTask
                                              ├─ SwitchTask → JQ condition matching
                                              └─ LocalRuntimeTask → set/do/fork/for/try/wait/raise/run/emit
```

## Implementation Status

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Local DSL parser replacing sdk-go/v2 | Done |
| 2 | DSL 1.0.3 `document` + `do` parsing | Done |
| 3 | DSL 0.8 legacy compatibility | Done |
| 4 | 12 task type mapping | Done |
| 5 | Task relation graph construction (then / switch / fork) | Done |
| 6 | Structural task built-in executors | Done |
| 7 | output/schedule/data field support | Done |
| 8 | A2A bidirectional bridge (Client + WorkflowAgent) | Done |
| 9 | Full Go test suite | Done |

## Quick Start

```bash
# Initialize database
mysql -u root -p < distribution/mysql-schema.sql

# Build
make build

# Start services
./bin/eventmesh-workflow controller --config configs/controller.yaml
./bin/eventmesh-workflow engine --config configs/engine.yaml

# Register a workflow
curl -X POST http://localhost:8080/workflow \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "demo", "workflow_name": "demo", "definition": "document:\n  dsl: \"1.0.3\"\n  name: demo\n  version: \"1.0.0\"\ndo:\n  - hello:\n      set:\n        greeting: \"Hello, World!\"\n      then: end"}'
```

Example workflows: `configs/testcreateworkflow-v1.yaml` (DSL 1.0.3) / `configs/testcreateworkflow.yaml` (DSL 0.8)
