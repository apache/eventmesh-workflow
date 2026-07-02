# EventMesh Workflow 设计文档

> **版本**: v1.0.0 | **更新时间**: 2026-06-29 | **DSL 版本**: Serverless Workflow 1.0.3 (兼容 0.8)

---

## 1. 概述

EventMesh Workflow 是 Apache EventMesh 生态的工作流运行时，负责解析 Serverless Workflow DSL 定义、构建任务图、调度执行并在 EventMesh 事件总线上编排微服务。

### 1.1 核心能力

| 能力 | 说明 |
| --- | --- |
| **DSL 解析** | 支持 Serverless Workflow 1.0.3 和 0.8 双格式，零外部 DSL 依赖 |
| **任务图构建** | 将 DSL 描述编译为有向任务图 (START → Task → Transition → END) |
| **运行时调度** | 基于队列的多实例调度引擎，支持操作/事件/开关/结构型任务 |
| **EventMesh 集成** | 通过 gRPC Catalog 查询操作定义，异步发布事件 |
| **A2A 桥接** | 工作流作为 A2A Agent 暴露，支持调用外部 A2A Agent |
| **结构型任务** | fork/try/for/do/set 等 9 种结构型任务内建执行器 |

---

## 2. 架构总览

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
│  │ (实例化)      │  │ (状态转移)    │  │ (任务入队)      │  │
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

### 2.1 组件分工

| 组件 | 路径 | 职责 |
| --- | --- | --- |
| **Controller** | `cmd/controller/` | HTTP API 服务，Gin 框架，Swagger 文档 |
| **Flow Engine** | `flow/` | 工作流实例启动、任务状态转移 |
| **DSL Parser** | `third_party/swf/` | YAML → Workflow/Task 结构体，双格式解析 |
| **DAL** | `internal/dal/` | GORM + MySQL 持久化，任务图构建 |
| **Task Executors** | `internal/task/` | 4 类执行器：操作/事件/开关/本地运行时 |
| **Queue** | `internal/queue/` | 任务队列抽象，支持 In-Memory / EventMesh |
| **Filter** | `internal/filter/` | JQ 表达式输入/输出数据过滤 |
| **A2A Bridge** | `internal/bridge/` | A2A 协议客户端 + WorkflowAgent HTTP 端点 |
| **Metrics** | `internal/metrics/` | Prometheus 指标采集 |

---

## 3. DSL 解析器设计

### 3.1 双格式兼容

解析器入口 `swf.Parse()` 通过检测顶层是否包含 `document` 键来区分 DSL 版本：

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

### 3.2 DSL 1.0.3 解析流程

```
YAML Source
    │
    ▼
parseV1Workflow(raw)
    │
    ├─ document.name / version / dsl → Workflow 元信息
    ├─ raw["do"] → parseV1TaskList → []*Task
    ├─ raw["use"].functions → map[string]*Function
    ├─ raw["schedule"] → Schedule (cron/start/after)
    ├─ raw["input"] → data input filter
    ├─ raw["output"].as → output filter
    │
    ▼
wf.Validate()
    ├─ FlattenTasks()  → 展平嵌套任务
    ├─ 检查任务名唯一性
    ├─ 校验 then/switch.when 目标有效性
    │
    ▼
*Workflow (标准化结果)
```

### 3.3 任务类型检测

```go
func detectV1TaskType(def map[string]interface{}) string {
    if _, ok := def["call"]; ok  → TaskTypeOperation
    // 按 DSL 标准键检测:
    // switch / set / do / fork / for / try
    // wait / raise / run / emit / listen
    return TaskTypeOperation  // 默认 fallback
}
```

### 3.4 数据模型

```
Workflow
  ├─ ID, Name, Version, DSL, Namespace
  ├─ Start: 入口任务名
  ├─ Tasks: []*Task
  │   ├─ Name, Type, InputFilter, OutputFilter
  │   ├─ InlineData, Then, ExplicitThen
  │   ├─ Actions: []*Action {OperationName, OperationType}
  │   ├─ Cases: []*SwitchCase {Name, Condition, Then, IsDefault}
  │   └─ Children: []*Task (嵌套子任务)
  ├─ Functions: map[string]*Function {Name, Operation, Type}
  └─ Schedule: {Start, Cron, After}
```

---

## 4. 任务图构建

### 4.1 DAL.create() 流程

```go
func (w *workflowDALImpl) create(ctx context.Context, tx *gorm.DB, record *model.Workflow) error {
    wf, _ := swf.Parse(record.Definition)
    
    // 1. 构建 WorkflowTask (展平所有嵌套任务)
    tasks := w.buildTask(wf)          // FlattenTasks → model.WorkflowTask
    
    // 2. 构建 WorkflowTaskRelation (任务间连线)
    relations := w.buildTaskRelation(wf, tasks)
    
    // 3. 并发写入 MySQL
}
```

### 4.2 任务关系构建规则

```
START → 第一个任务 (workflow.Start)

对每个任务:
  ┌─ Switch 类型 → buildSwitchTaskRelation
  │                 每个 case.Then → 目标 TaskID (或 END)
  ├─ Fork 类型   → buildForkTaskRelation
  │                 每个 child → 独立分支连线
  ├─ 显式 then   → resolveNextTaskID(Then)
  │                 "end"/"exit"/"continue" → END
  │                 命名目标 → taskIDs[name]
  └─ 默认        → Children[0].Name (嵌套结构) 或 END
```

### 4.3 Fork 任务图示例

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

Fork 构建时通过 `publishNextTasks()` 并行发布所有子分支。

---

## 5. 任务执行器

### 5.1 任务分发工厂

```go
func New(instance *model.WorkflowTaskInstance) Task {
    if isLocalRuntimeTask(taskType) → NewLocalRuntimeTask
    switch taskType:
        operation → NewOperationTask
        event     → NewEventTask
        switch    → NewSwitchTask
        default   → NewOperationTask  // 兜底
}
```

### 5.2 执行器类型

| 执行器 | 处理类型 | Run() 行为 |
| --- | --- | --- |
| **OperationTask** | call / listen / run | 执行 catalog 的 operation，发布 EventMesh 事件 / A2A 调用 |
| **EventTask** | event / listen | 委托给 OperationTask 执行 |
| **SwitchTask** | switch | JQ 条件匹配 → 选择分支 → publishOrComplete |
| **LocalRuntimeTask** | set / do / fork / for / try / wait / raise / emit | 内建执行器，详见 §5.3 |

### 5.3 LocalRuntimeTask 内建执行器

| 任务 | execute() 方法 | 逻辑 |
| --- | --- | --- |
| **set** | executeSet() | JQ Object() 将 set 表达式应用到输入 JSON |
| **do** | executeDo() | 顺序执行 do 列表中的子任务（set / raise） |
| **fork** | 不做本地执行 | 由 DAL 构建多分支 relations，run() 之后 publishNextTasks |
| **for** | executeFor() | 解析 JSON 数组 → 逐元素执行 do body 中的 set |
| **try** | executeTry() | 顺序尝试 try 列表中的任务，失败跳过 |
| **wait** | executeWait() | time.ParseDuration → time.Sleep |
| **raise** | executeRaise() | 构造结构化错误并返回 |
| **run** | executeRun() | 发布到 EventMesh (委托 publishEvent) |
| **emit** | executeEmit() | 发布到 EventMesh (委托 publishEvent) |

### 5.4 数据流转

```
Input ──→ [InputFilter] ──→ Task.Execute() ──→ output ──→ [OutputFilter] ──→ Next Task
                                    │
                          publishNextOrComplete()
                          (发布下一个任务到 Queue)
```

- **InputFilter**: 在任务入队时通过 `FilterWorkflowTaskInputData()` 应用
- **OutputFilter**: 在 LocalRuntimeTask.Run() 返回前通过 `FilterWorkflowTaskOutputData()` 应用
- 过滤器基于 JQ 表达式：`${ .field }` 语法

---

## 6. 队列与调度

### 6.1 队列抽象

```go
type ObserveQueue interface {
    Publish(instances []*model.WorkflowTaskInstance) error
    Subscribe(handler func(*model.WorkflowTaskInstance))
    UnSubscribe() error
}
```

两种实现：
- **InMemoryQueue**: 开发/测试环境，内存 chan
- **EventMeshQueue**: 生产环境，通过 EventMesh SDK 发布

### 6.2 调度流程

```
Engine.Start(param)
  → SelectStartTask → 找出 START 关联的第一个任务
  → InsertInstance → 创建工作流实例记录
  → Queue.Publish(taskInstance) → 入队

Consumer 消费:
  → task.New(instance) → 创建对应执行器
  → task.Run()
    → OperationTask: 发布 EventMesh 事件 (异步) + 入队下一任务 (sleep)
    → EventTask: 委托 OperationTask
    → SwitchTask: 匹配条件 → 发布下一任务
    → LocalRuntimeTask: 同步执行 → 发布下一任务

Engine.Transition(param)  ← EventMesh 回调
  → SelectTransitionTask (sleep → wait)
  → Queue.Publish → 重新入队消费
```

---

## 7. A2A 桥接

### 7.1 架构

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

`A2AExecutor` 实现轮询式任务执行：

```go
SendTask(input, metadata) → pollUntilComplete(taskID)
  // 每 2 秒轮询 /a2a/tasks/{id}，最多 30 次
```

任务检测 `isA2ATask()`: `OperationType == "a2a"` 或 `OperationName` 以 `"a2a"` 开头。

### 7.3 WorkflowAgent (Server)

将工作流暴露为 A2A Agent：

```
GET  /.well-known/agent-card.json → Agent Card
POST /a2a/tasks                   → 启动工作流实例
GET  /a2a/tasks/{id}              → 查询工作流实例状态
GET  /a2a/health                  → 健康检查
```

### 7.4 A2A 消息格式 (v1.0 兼容)

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

## 8. 数据库模型

### 8.1 ER 图

```
t_workflow ──1:N── t_workflow_task ──1:N── t_workflow_task_action
     │                    │
     │                    └──1:N── t_workflow_task_relation
     │
     └──1:N── t_workflow_instance ──1:N── t_workflow_task_instance
```

### 8.2 核心表

| 表 | 用途 | 关键字段 |
| --- | --- | --- |
| t_workflow | 工作流定义 | workflow_id, definition(DSL YAML), version |
| t_workflow_task | 展平后的任务节点 | task_id, task_name, task_type, task_input_filter, task_output_filter |
| t_workflow_task_action | 任务操作定义 | operation_name, operation_type |
| t_workflow_task_relation | 任务间连线 | from_task_id, to_task_id, condition |
| t_workflow_instance | 工作流实例 | workflow_instance_id, workflow_status |
| t_workflow_task_instance | 任务实例 | task_instance_id, status, input |

### 8.3 状态机

```
任务实例: SLEEP(1) → WAIT(2) → PROCESS(3) → SUCCESS(4) / FAIL(5)

工作流实例: PROCESS(1) → SUCCESS(2)
```

- **SLEEP**: OperationTask 发布事件后立即入队下一任务，状态为 SLEEP；等待 EventMesh 回调 Transition
- **WAIT**: 就绪，待消费
- **PROCESS**: 消费中（实际执行）
- **SUCCESS** / **FAIL**: 终态

---

## 9. 数据过滤

基于 JQ (itchyny/gojq) 实现 JSON 数据过滤：

```go
FilterWorkflowTaskInputData(task)
  → filterJsonData(task.TaskInputFilter, task.Input)
    → jqer.Object(jsonObj, "${ filterExp }")

FilterWorkflowTaskOutputData(input, outputFilter)
  → filterJsonData(outputFilter, input)
    → jqer.Object(jsonObj, "${ filterExp }")
```

表达式规范化：裸表达式自动包装为 `${ expr }`。

---

## 10. 模块依赖

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
             ──→ third_party/swf (类型常量)
```

零外部 SWF SDK 依赖。`third_party/swf/` 是完全自主实现的 DSL 解析器。

---

## 11. 设计决策

| 决策 | 理由 |
| --- | --- |
| 零外部 DSL 依赖 | sdk-go/v2 只支持 0.8，无法升级；自主解析器完全掌控 |
| 双格式兼容 | 渐进迁移，已有 0.8 工作流不受影响 |
| 展平任务图 | 统一 task graph 模型，简化运行时调度 |
| LocalRuntimeTask | 内建执行器避免对结构型任务的外部 EventMesh 调用开销 |
| A2A 轮询模式 | 简单可靠，适用于非流式 Agent 调用场景 |
| GORM + MySQL | 与 EventMesh 生态一致，支持事务 |
| JQ 表达式过滤 | 轻量 JSON 转换，零新依赖（复用 gojq） |
