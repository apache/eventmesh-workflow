# EventMesh Workflow 使用说明

> **版本**: v1.0.0 | **更新时间**: 2026-06-29

---

## 1. 快速开始

### 1.1 环境准备

| 依赖 | 版本 | 说明 |
| --- | --- | --- |
| Go | ≥ 1.18 | 编译运行 |
| MySQL | ≥ 5.7 | 持久化存储 |
| EventMesh | - | 事件总线 / Catalog 服务 |

### 1.2 初始化数据库

```bash
mysql -u root -p < distribution/mysql-schema.sql
```

### 1.3 编译

```bash
make build
# 产物: bin/eventmesh-workflow
```

### 1.4 启动

```bash
# Controller (HTTP API)
./bin/eventmesh-workflow controller --config configs/controller.yaml

# Engine (Task Runner)
./bin/eventmesh-workflow engine --config configs/engine.yaml
```

Controller 默认端口见 `configs/controller.yaml` 的 `server.port`，Swagger 文档在 `/swagger/index.html`。

---

## 2. DSL 编写指南

### 2.1 DSL 1.0.3 (推荐)

使用 `document` + `do` 结构：

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

### 2.2 DSL 0.8 (兼容)

使用 `id` + `states` 结构：

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

两种格式可混用，解析器自动检测。

---

## 3. 任务类型详解

### 3.1 call — 调用外部操作

```yaml
- sendOrder:
    call: asyncapi
    with:
      operation: file://order.yaml#sendOrder
    then: nextStep
```

支持的 call 类型：

| call 值 | with 参数 | 解析行为 |
| --- | --- | --- |
| `http` | `endpoint` | HTTP 端点作为 operation name |
| `asyncapi` | `operation` / `channel` / `document` | EventMesh catalog 查询 |
| `openapi` | `operationId` / `operation` / `document` | REST API 操作 |
| `grpc` | `service` / `method` | gRPC 服务方法 |
| `a2a` | `endpoint` | A2A Agent URL |

A2A 调用示例：

```yaml
- askAgent:
    call: a2a
    with:
      endpoint: http://localhost:9090
    then: processResult
```

### 3.2 listen — 监听事件

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

### 3.3 switch — 条件分支

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

条件表达式使用 JQ 语法，`.field` 引用输入 JSON 字段。

### 3.4 set — 数据转换

```yaml
- transform:
    set:
      fullName: '${ .firstName + " " + .lastName }'
      amount: '${ .price * .quantity }'
    then: nextStep
```

### 3.5 do — 子任务序列

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

do 内的子任务目前支持 `set` 和 `raise`。

### 3.6 fork — 并行分支

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

### 3.7 for — 循环迭代

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

### 3.8 try — 错误处理

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

### 3.9 wait — 延迟

```yaml
- pause:
    wait:
      seconds: '10s'
    then: nextStep
```

支持 Go duration 格式：`10s`, `1m`, `500ms`, `1h30m`。

### 3.10 raise — 抛出错误

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

### 3.11 run — 发布事件

```yaml
- fireEvent:
    run:
      with:
        event: order.processed
    then: end
```

### 3.12 emit — 发出事件 (同 run)

```yaml
- emitEvent:
    emit:
      event: notification.sent
    then: end
```

---

## 4. 数据输入输出

### 4.1 工作流级 input

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

启动工作流时传入的 JSON 会通过 `input.from` 过滤后再传递给第一个任务。

### 4.2 任务级 input/output 过滤

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

- `input.from`: 从上游数据中提取字段作为本任务输入
- `output.as`: 从本任务输出中提取字段传递给下游

### 4.3 内联数据

```yaml
- step1:
    set:
      greeting: '${ "Hello, " + .name }'
    data: '{"name": "World"}'
    then: end
```

`data` 字段提供静态初始数据，优先级低于 input 传入的数据。

---

## 5. 调度配置

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

| 字段 | 说明 |
| --- | --- |
| `start` | ISO 8601 时间，调度开始时间 |
| `cron` | Cron 表达式 |
| `after` | ISO 8601 间隔 (PT5M = 5 分钟) |

---

## 6. 流程控制

### 6.1 then 指令

```yaml
then: nextTaskName    # 跳转到命名任务
then: end             # 终止工作流
then: exit            # 同 end
then: continue        # 同 end（当前未实现循环语义）
```

### 6.2 隐式顺序

如果任务未指定 `then` 而有嵌套子任务（do/fork/for/try），默认跳转到第一个子任务。

---

## 7. REST API 参考

### 7.1 工作流 CRUD

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflow` | 创建/更新工作流 |
| GET | `/workflow` | 查询工作流列表 |
| GET | `/workflow/:workflowId` | 查询工作流详情 |
| DELETE | `/workflow/:workflowId` | 删除工作流 |
| GET | `/workflow/instances` | 查询运行实例 |

### 7.2 创建/更新工作流

```bash
curl -X POST http://localhost:8080/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "order-management",
    "workflow_name": "Order Management Workflow",
    "definition": "document:\n  dsl: \"1.0.3\"\n  name: order-management\n  version: \"1.0.0\"\n  namespace: eventmesh.apache.org\ndo:\n  - receiveOrder:\n      listen:\n        to:\n          one:\n            with:\n              type: online.store.newOrder\n      then: end"
  }'
```

**注意**: `definition` 是完整的 DSL YAML 文本，创建时 `workflow_id` 必须与 DSL 中的 `document.name` 或 `id` 一致。

### 7.3 查询工作流列表

```bash
curl "http://localhost:8080/workflow?page=1&size=20"
```

响应：

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

### 7.4 删除工作流

```bash
curl -X DELETE http://localhost:8080/workflow/order-management
```

执行软删除（status → -1），关联的任务、关系、实例同时标记。

---

## 8. A2A 集成

### 8.1 工作流作为 Agent

启动内置 A2A 端点：

```go
agent := bridge.NewWorkflowAgent("my-workflow", "http://localhost:9090")
mux := http.NewServeMux()
agent.RegisterRoutes(mux)
http.ListenAndServe(":9090", mux)
```

外部 A2A Client 调用：

```bash
# 获取 Agent Card
curl http://localhost:9090/.well-known/agent-card.json

# 提交任务 (触发工作流执行)
curl -X POST http://localhost:9090/a2a/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "role": "user",
      "parts": [{"type": "text", "text": "{\"order_id\": \"12345\"}"}]
    }
  }'

# 查询状态
curl http://localhost:9090/a2a/tasks/{task_id}
```

### 8.2 工作流调用 A2A Agent

在 DSL 中配置 `call: a2a`：

```yaml
- aiAnalysis:
    call: a2a
    with:
      endpoint: http://ai-agent:8080
    then: processResult
```

---

## 9. 完整示例

### 9.1 订单处理工作流 (DSL 1.0.3)

文件: `configs/testcreateworkflow-v1.yaml`

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

### 9.2 数据转换 + 并行处理

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

## 10. 开发调试

### 10.1 运行测试

```bash
make test
```

测试覆盖：

| 包 | 测试数量 | 内容 |
| --- | --- | --- |
| third_party/swf | 17 | V1/旧版解析、Fork/Try/For、Schedule/Output、校验、集成测试 |
| internal/filter | 1 | JQ 过滤正常/异常路径 |

### 10.2 格式化

```bash
make fmt       # goimports + gofmt
```

### 10.3 代码检查

```bash
make lint      # golangci-lint
```

### 10.4 覆盖率

```bash
make cover     # 生成 HTML 覆盖率报告
```

### 10.5 本地调试技巧

1. 使用 **InMemoryQueue**（配置 `engine.yaml` 中 `flow.queue.store: in-memory`）避免依赖 EventMesh
2. OperationTask 的 `runA2AAction` 可通过 Mock A2A Agent 验证
3. LocalRuntimeTask 完全自包含，可独立测试

---

## 11. 配置参考

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

## 12. 限制与规划

### 当前限制

| 项目 | 说明 |
| --- | --- |
| for 迭代 | body 内目前仅支持 set 任务 |
| try/catch | when 内的任务类型有限 |
| 嵌套 do | 运行时仅执行 set/raise，复杂子任务尚不支持 |
| A2A streaming | 暂不支持流式响应 |
| 错误重试 | 全局 retry_attempts=5，未按任务配置 |

### 规划功能

- [ ] for/do/try 内完整任务类型支持
- [ ] 子工作流调用 (subFlow)
- [ ] 条件重试与超时配置
- [ ] A2A 流式支持
- [ ] WorkflowAgent 完整实例状态查询
- [ ] DSL 1.0.3 auth/error/timeout 完整字段
