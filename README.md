# eventmesh-workflow

Apache EventMesh workflow runtime — Serverless Workflow DSL `1.0.3` compatible.

## 文档

| 文档 | 说明 |
| --- | --- |
| [docs/DESIGN.md](docs/DESIGN.md) | 架构设计、组件分工、数据模型、任务执行器、A2A 桥接 |
| [docs/USAGE.md](docs/USAGE.md) | 快速开始、DSL 编写指南、12 种任务类型、REST API、完整示例 |

## 核心特性

- **双 DSL 兼容**: Serverless Workflow 1.0.3 (`document` + `do`) 和 0.8 (`id` + `states`)
- **零外部 DSL 依赖**: 自研 `third_party/swf/` 解析器，替换 sdk-go/v2
- **12 种任务类型**: call / listen / switch / set / do / fork / for / try / wait / raise / run / emit
- **结构型任务内建执行器**: fork(并行) / try(错误处理) / for(循环) / do(序列)
- **A2A 双向桥接**: 工作流可调用外部 A2A Agent，也可作为 A2A Agent 暴露
- **JQ 数据过滤**: input/output 级 JSON 过滤管道
- **4 类运行时执行器**: Operation / Event / Switch / LocalRuntime

## 架构速览

```
Controller(HTTP API) → DAL(MySQL) ← DSL Parser(swf)
                              ↓
Flow Engine → Queue(In-Memory/EventMesh) → Task Executors
                                              ├─ OperationTask → EventMesh/A2A
                                              ├─ EventTask
                                              ├─ SwitchTask → JQ 条件匹配
                                              └─ LocalRuntimeTask → set/do/fork/for/try/wait/raise/run/emit
```

## 实现状态

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | 本地 DSL 解析器替换 sdk-go/v2 | Done |
| 2 | DSL 1.0.3 `document` + `do` 解析 | Done |
| 3 | DSL 0.8 旧版兼容 | Done |
| 4 | 12 种任务类型映射 | Done |
| 5 | 任务关系图构建 (then / switch / fork) | Done |
| 6 | 结构型任务内建执行器 | Done |
| 7 | output/schedule/data 字段支持 | Done |
| 8 | A2A 双向桥接 (Client + WorkflowAgent) | Done |
| 9 | 全文 Go test suite | Done |

## 快速开始

```bash
# 初始化数据库
mysql -u root -p < distribution/mysql-schema.sql

# 编译
make build

# 启动
./bin/eventmesh-workflow controller --config configs/controller.yaml
./bin/eventmesh-workflow engine --config configs/engine.yaml

# 注册工作流
curl -X POST http://localhost:8080/workflow \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "demo", "workflow_name": "demo", "definition": "document:\n  dsl: \"1.0.3\"\n  name: demo\n  version: \"1.0.0\"\ndo:\n  - hello:\n      set:\n        greeting: \"Hello, World!\"\n      then: end"}'
```

示例工作流: `configs/testcreateworkflow-v1.yaml` (DSL 1.0.3) / `configs/testcreateworkflow.yaml` (DSL 0.8)
