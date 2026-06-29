# eventmesh-workflow

Apache EventMesh workflow runtime.

## Serverless Workflow Spec Plan

The project now targets Serverless Workflow DSL `1.0.3` while keeping backward compatibility with existing `specVersion: '0.8'` definitions.

### Goals

1. Accept latest DSL documents with the top-level `document`, `use`, `do`, `input`, `output`, `timeout`, and `schedule` shape.
2. Keep legacy workflows working during migration.
3. Normalize both DSL generations into the internal EventMesh task graph: `START -> task -> relation -> END`.
4. Preserve current EventMesh integration through AsyncAPI/EventMesh catalog operations.
5. Provide tests and examples for the latest DSL.

### Implementation Plan

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Replace direct dependency on Serverless Workflow v0.8 model with a local compatibility parser | Done |
| 2 | Add latest DSL v1.0.3 parser path for `document` + `do` workflows | Done |
| 3 | Keep legacy v0.8 parser path for `id` + `states` workflows | Done |
| 4 | Map v1 task kinds to runtime task types: `call`, `listen`, `switch`, `set`, `do`, `fork`, `for`, `try`, `wait`, `raise`, `run`, `emit` | Done |
| 5 | Build workflow task relations from `then`, `switch.when`, default cases, and termination directives | Done |
| 6 | Harden runtime task dispatch so newly recognized task types degrade to local operation execution where a specialized executor is not required yet | Done |
| 7 | Add v1 sample workflow and parser coverage | Done |
| 8 | Run full Go test suite and fix compatibility issues | Pending |

### Supported DSL Coverage

- `document.dsl/name/namespace/version` metadata.
- `do` task lists and nested task lists.
- `call` tasks for `asyncapi`, `http`, `openapi`, `grpc`, and custom operation names.
- `listen` tasks mapped to EventMesh event tasks.
- `switch` tasks with `when`, default branches, and `then` directives.
- Structural tasks: `set`, `do`, `fork`, `for`, `try`, `wait`, `raise`, `run`, `emit`.
- Flow directives: `end`, `exit`, and `continue` currently terminate the EventMesh workflow graph unless a named next task is supplied.

See `configs/testcreateworkflow-v1.yaml` for a Serverless Workflow DSL `1.0.3` example.
