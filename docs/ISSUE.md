# Upgrade to Serverless Workflow DSL 1.0.3 with Structured Tasks & A2A Bridge

**Status**: ✅ Complete
**Labels**: enhancement, documentation

---

## Summary

Upgrade eventmesh-workflow from Serverless Workflow DSL 0.8 to `1.0.3` with full structured task support, A2A bidirectional bridge, and bilingual documentation.

## Changes

### DSL Parser (3a1662f)
- Replace `sdk-go/v2` dependency with custom `third_party/swf/` parser (zero external SWF dependency)
- Support DSL 1.0.3 `document` + `do` top-level structure
- Maintain backward compatibility with DSL 0.8 `id` + `states` format
- Full `use`, `input`, `output`, `schedule` field support

### Structured Task Executors (514db08)
- **fork**: Parallel branch execution via `publishNextTasks()`
- **try**: Error handling with `catch.when` → Children mapping
- **for**: JSON array iteration through `do` body
- **do**: Sequential sub-task execution
- Built-in executors for `set`, `wait`, `raise`, `run`, `emit`

### A2A Bidirectional Bridge (514db08)
- `A2AExecutor`: Polling-based A2A Client (SendTask → WaitForCompletion)
- `WorkflowAgent`: Expose workflows as A2A Agents via HTTP endpoints
- `OperationType="a2a"` auto-dispatch in operation_task

### Data Pipeline (514db08)
- Task-level `input.from` / `output.as` JSON filter pipeline
- JQ expression support via `FilterWorkflowTaskOutputData()`
- Schedule fields: `start`, `cron`, `after`

### Documentation (25d1a3f, 1af4bee, b733973)
- `docs/DESIGN.md` / `docs/DESIGN_CN.md`: Architecture design (EN + CN)
- `docs/USAGE.md` / `docs/USAGE_CN.md`: User guide with 12 task type examples (EN + CN)
- `README.md`: Bilingual doc index, English-first

### Test Coverage
- 17 parser tests (V1/legacy parsing, Fork/Try/For, Schedule/Output, validation, integration)
- `go test ./...` all pass
- `go vet ./...` zero warnings

## Breaking Changes
- Removed `sdk-go/v2` import from `go.mod` — all SWF model references now use `third_party/swf/`

## Files Changed
- 35+ files modified/added across 5 commits
- +3500+ lines of code and documentation

## Related Commits
1. `3a1662f` — feat: upgrade to Serverless Workflow DSL 1.0.3 with full structured task support
2. `514db08` — feat: fork/try/for executors + output/schedule/data + A2A bridge
3. `25d1a3f` — docs: add design and usage docs for DSL 1.0.3 workflow engine
4. `1af4bee` — docs: translate all docs to English as default language
5. `b733973` — docs: add Chinese versions alongside English docs
