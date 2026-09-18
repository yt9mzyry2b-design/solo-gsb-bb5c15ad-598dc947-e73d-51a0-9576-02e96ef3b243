# 多级配额裁决 API（Multi-Level Quota Decider）

使用 **Python 3.11 + FastAPI + SQLite** 实现的本地多级配额裁决服务。
配额策略可分别在 **全局（global）/ 租户（tenant）/ 路由（route）** 三级配置，
每级独立设置桶容量（capacity）与每秒令牌补充率（refill_rate）。

## 核心语义

- **惰性令牌桶**：桶只在被裁决/撤销命中时，按距上次更新经过的时间补充令牌，
  补充量 = `elapsed × refill_rate`，且余额永远不超过桶容量。
- **整组原子裁决**：一次请求命中的所有桶（全局 → 租户 → 路由，缺失的层不限制），
  在**同一个 `BEGIN IMMEDIATE` 事务**、同一个时间戳下完成「补充 → 判足 → 扣减」。
  - 所有桶都足够：全部同时扣减 `cost`，返回 `allowed=true`；
  - 任一桶不足：**一个令牌都不扣**（时间应得的补充仍会落库），返回 `allowed=false`。
- **整组最早可重试时间**：拒绝时，对每个不足桶计算就绪等待
  `(cost - balance) / refill_rate`，取**最大值**作为 `retry_at` / `retry_after_seconds`。
  若 `refill_rate=0` 或 `cost` 超过桶容量（永远不可能满足），返回
  `retry_possible=false`、`retry_at=null`。
- **幂等重放**：相同 `request_id` 且裁决载荷（tenant_id / route_id / cost）一致时，
  直接返回原裁决结果（`replayed=true`），不重复扣减；载荷冲突返回 **409**。
  通过与拒绝的裁决都会存档。
- **撤销**：可撤销已通过的裁决并向当时命中的桶补回令牌，重复撤销保持幂等；
  补回时同样先补充再退还，最终余额**不超过桶容量**。
- **低余额事件**：策略可配置 `low_balance_threshold`（`null` 关闭，否则须在
  `0` 到 `capacity` 之间）。一次**获准**裁决若令某命中桶的余额
  **从阈值之上降至不高于阈值**，会在扣减的同一事务内为该桶写入唯一一条
  `low_balance` 事件（递增 `id`、桶键、扣减前后余额、阈值、`request_id`、时间）。
  拒绝与幂等重放不产生事件；余额已在阈值之下的继续扣减也不算越线。
  当时间补充、撤销或策略更新（如调低阈值）使余额重新高于阈值后，
  后续再次越线会产生**新**事件。事件通过 `GET /events` 按 `id` 稳定递增分页读取。
- **策略定时切换**：通过 `POST /policy-changes` 为**已存在**的策略提交一条带
  唯一 `change_id` 与未来 UTC 时间 `effective_at` 的变更（新容量、新补充速率、
  新低余额阈值）。同一策略至多有一条**待生效（pending）**变更；该期间
  `PUT /policies` 直接更新该策略会返回 **409**。可在生效前用
  `DELETE /policy-changes/{change_id}` 取消（重复取消幂等）。
  **没有后台任务**：每次裁决在**同一个 `BEGIN IMMEDIATE` 事务内**先应用所有命中且
  到期的变更，再执行原有裁决。应用时：① 先按**旧策略**把余额补充到
  `effective_at`；② 再按 `旧余额 / 旧容量 × 新容量` 保持剩余比例（旧容量为 `0`
  时余额为 `0`）；③ 桶时间锚点移到 `effective_at`，此后按**新速率**继续补充。
  变更落库（换算余额 + 覆盖策略 + 标记 applied）与本次裁决原子完成，
  并发请求只能观察到完整的旧状态或完整的新状态。
- **策略版本审计与回滚**：每次 `PUT /policies` 成功、定时切换**实际生效**、
  回滚成功，都会在**同一写事务内**追加一条**不可变**版本记录（同策略
  `version` 从 1 开始单调递增），内容包含完整参数（容量/速率/阈值）、
  变更来源（`put` / `scheduled` / `rollback`，老库升级补登为 `baseline`）、
  关联信息（定时变更的 `change_id`、回滚的 `restored_from_version`）与时间。
  `GET /policies/history` 按 `scope` + `tenant_id` + `route_id` 查询，
  新版本在前；`POST /policies/rollback` 提交策略键与目标 `version`，
  在**同一 `BEGIN IMMEDIATE` 写事务**内恢复该版本参数，已有桶先按**当前**
  策略补充到事务时刻、再按目标容量截断，并追加一条 `source=rollback` 的新版本。
  策略存在 `pending` 定时变更时回滚返回 **409**；未知策略或版本返回 **404**。
- **并发不超发**：SQLite 处于 WAL 模式，写事务一律 `BEGIN IMMEDIATE` 立即加写锁，
  配合 `busy_timeout=30s` 让并发写者排队，读-改-写序列天然串行化。
  单进程多线程、多进程（多个实例共享同一数据库文件）下均不会超发。

## 一条命令启动（需要 Docker + Compose）

```bash
docker compose up --build
# 后台运行：
docker compose up --build -d
```

服务监听 **8000 端口**：

- API 基址：`http://localhost:8000`
- 健康检查：`GET http://localhost:8000/health`
- 交互式文档（Swagger UI）：`http://localhost:8000/docs`
- OpenAPI Schema：`http://localhost:8000/openapi.json`

SQLite 数据文件保存在命名卷 `quota-data`（容器内 `/data/quota.db`），
`docker compose down` 不会删除；如需清空：`docker compose down -v`。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 健康检查 |
| `GET`  | `/policies` | 列出全部策略 |
| `PUT`  | `/policies` | 创建/更新一层策略（按 scope+tenant_id+route_id 幂等 upsert）；该策略有 pending 变更时返回 409；成功后追加一条 `put` 版本 |
| `GET`  | `/policies/history` | 按 scope/tenant_id/route_id 查询单策略的不可变版本历史（新版本在前） |
| `POST` | `/policies/rollback` | 在同一写事务内把策略恢复到指定历史版本并追加 `rollback` 版本；有 pending 变更时 409，未知策略/版本 404 |
| `POST` | `/policy-changes` | 为已存在的策略提交一条未来生效的定时切换 |
| `GET`  | `/policy-changes` | 列出定时变更（可按 `status` / 策略键过滤） |
| `GET`  | `/policy-changes/{change_id}` | 查询单条定时变更 |
| `DELETE` | `/policy-changes/{change_id}` | 在生效前取消待生效变更（重复取消幂等） |
| `GET`  | `/buckets` | 查看所有已创建桶补充后的实时余额（只读，不落库） |
| `POST` | `/decide` | 提交一次多级裁决（事务内先应用命中且到期的变更） |
| `POST` | `/decide/{request_id}/revoke` | 撤销一次已通过的裁决（重复撤销幂等） |
| `GET`  | `/events` | 按 id 递增分页查询 low_balance 事件（`after_id` / `limit`） |

策略对象：

```json
{
  "scope": "global | tenant | route",
  "tenant_id": "acme（tenant/route 必填）",
  "route_id": "/v1/search（route 必填，且必须带 tenant_id）",
  "capacity": 10,
  "refill_rate": 0.5,
  "low_balance_threshold": 2
}
```

> `low_balance_threshold` 可省略或为 `null`：关闭该层的低余额事件；
> 否则必须在 `0` 到 `capacity` 之间（含边界），超出范围返回 `422`。
> `PUT` 为全量替换：更新策略时不传该字段即视为关闭。

定时变更请求（`POST /policy-changes`）：

```json
{
  "change_id": "chg-20261001-global",
  "scope": "global",
  "capacity": 20,
  "refill_rate": 2,
  "low_balance_threshold": 3,
  "effective_at": "2026-10-01T00:00:00Z"
}
```

> - `change_id` 为变更的全局唯一幂等键，重复使用返回 `409`；
> - 目标策略必须已通过 `PUT /policies` 创建，否则返回 `404`；
> - `effective_at` 必须是**未来的、显式带时区**的 UTC 时间（ISO 8601，
>   如 `2026-10-01T00:00:00Z`），不带时区或不在未来返回 `422`；
> - `capacity` / `refill_rate` / `low_balance_threshold` 的取值约束与
>   `PUT /policies` 完全一致；
> - 同一 `scope+tenant_id+route_id` 至多一条 `pending` 变更，再提交返回 `409`；
>   变更被应用或取消后才能提交下一条。

变更对象（响应及 `GET /policy-changes` 返回元素）：

```json
{
  "change_id": "chg-20261001-global",
  "scope": "global",
  "tenant_id": null,
  "route_id": null,
  "capacity": 20.0,
  "refill_rate": 2.0,
  "low_balance_threshold": 3.0,
  "effective_at": "2026-10-01T00:00:00Z",
  "status": "pending",
  "created_at": "2026-09-17T12:56:09.556564Z",
  "applied_at": null,
  "cancelled_at": null
}
```

> `status` 生命周期：`pending`（待生效）→ `applied`（已由某次裁决事务应用）
> 或 `cancelled`（生效前取消）。`GET /policy-changes` 可用
> `?status=pending|applied|cancelled` 及 `scope` / `tenant_id` / `route_id` 过滤。

事件对象（`GET /events` 的返回元素）：

```json
{
  "id": 1,
  "scope": "tenant",
  "tenant_id": "acme",
  "route_id": null,
  "balance_before": 7.0,
  "balance_after": 4.0,
  "threshold": 4.0,
  "request_id": "req-1",
  "created_at": "2026-09-17T12:56:09.556564Z"
}
```

| 字段 | 含义 |
| --- | --- |
| `id` | 单调递增的事件 id，用作分页游标 |
| `scope` / `tenant_id` / `route_id` | 触发事件的桶键 |
| `balance_before` / `balance_after` | 本次裁决扣减前（时间补充后）/ 扣减后的余额 |
| `threshold` | 触发本事件时策略上的低余额阈值 |
| `request_id` | 触发本事件的裁决幂等键 |
| `created_at` | 事件时间（ISO 8601 UTC） |

裁决请求：

```json
{
  "request_id": "req-001",
  "tenant_id": "acme",
  "route_id": "/v1/search",
  "cost": 3
}
```

> `tenant_id`、`route_id` 均可省略；省略即不命中对应层级。
> `cost` 对所有命中桶统一扣减同一数量。

## 示例请求

### 1. 配置三级策略

```bash
# 全局：容量 10，0.5 令牌/秒
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"global","capacity":10,"refill_rate":0.5}'

# 租户 acme：容量 6，1 令牌/秒
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"tenant","tenant_id":"acme","capacity":6,"refill_rate":1}'

# 路由（acme 的 /v1/search）：容量 4，2 令牌/秒
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"route","tenant_id":"acme","route_id":"/v1/search","capacity":4,"refill_rate":2}'
```

### 2. 裁决（通过，三级桶同时扣 3）

```bash
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","tenant_id":"acme","route_id":"/v1/search","cost":3}'
```

响应中 `buckets` 返回每个命中桶的容量、补充率和裁决后余额（7 / 3 / 1）：

```json
{
  "request_id": "req-1",
  "allowed": true,
  "cost": 3.0,
  "tenant_id": "acme",
  "route_id": "/v1/search",
  "decided_at": "2026-09-17T12:56:09.556564Z",
  "retry_at": null,
  "retry_after_seconds": null,
  "retry_possible": true,
  "replayed": false,
  "revoked": false,
  "revoked_at": null,
  "buckets": [
    {"scope": "global", "tenant_id": null, "route_id": null, "capacity": 10.0, "refill_rate": 0.5, "balance": 7.0},
    {"scope": "tenant", "tenant_id": "acme", "route_id": null, "capacity": 6.0, "refill_rate": 1.0, "balance": 3.0},
    {"scope": "route",  "tenant_id": "acme", "route_id": "/v1/search", "capacity": 4.0, "refill_rate": 2.0, "balance": 1.0}
  ]
}
```

### 3. 裁决（路由桶不足 → 整组拒绝，全部不扣）

```bash
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-2","tenant_id":"acme","route_id":"/v1/search","cost":2}'
```

```json
{
  "request_id": "req-2",
  "allowed": false,
  "retry_at": "2026-09-17T12:56:10.056564Z",
  "retry_after_seconds": 0.48,
  "retry_possible": true,
  "buckets": [ {"scope": "global", "balance": 7.0}, {"scope": "tenant", "balance": 3.0}, {"scope": "route", "balance": 1.0} ]
}
```

### 4. 幂等重放与 409 冲突

```bash
# 相同 request_id + 相同载荷：重放原结果（replayed=true），不再扣令牌
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","tenant_id":"acme","route_id":"/v1/search","cost":3}'

# 相同 request_id 但载荷不同（如 cost 改变）：HTTP 409
curl -i -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","tenant_id":"acme","route_id":"/v1/search","cost":4}'
```

### 5. 撤销与重复撤销

```bash
# 撤销已通过的裁决，向当时命中的桶补回令牌（不超过容量）
curl -s -X POST http://localhost:8000/decide/req-1/revoke

# 再次撤销：幂等，返回同一撤销结果（replayed=true）
curl -s -X POST http://localhost:8000/decide/req-1/revoke
```

对被拒绝的裁决撤销返回 409；对不存在的 `request_id` 撤销返回 404。

### 6. 查看桶余额

```bash
curl -s http://localhost:8000/buckets
```

### 7. 低余额事件：配置阈值、触发与分页查询

```bash
# 给租户 acme 配置阈值 2：余额从 2 以上降到 2 或以下时产生事件
curl -s -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"tenant","tenant_id":"acme","capacity":6,"refill_rate":1,"low_balance_threshold":2}'

# 裁决通过，租户桶 6 -> 3：仍在阈值之上，无事件
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-10","tenant_id":"acme","cost":3}'

# 再次裁决，租户桶 3 -> 1：越过阈值，写入一条 low_balance 事件
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-11","tenant_id":"acme","cost":2}'

# 查询事件（默认按 id 升序返回最多 100 条）
curl -s http://localhost:8000/events

# 稳定分页：limit 控制页大小，after_id 传上一页最后一条的 id
curl -s 'http://localhost:8000/events?limit=10'
curl -s 'http://localhost:8000/events?after_id=1&limit=10'
```

事件响应示例：

```json
[
  {
    "id": 1,
    "scope": "tenant",
    "tenant_id": "acme",
    "route_id": null,
    "balance_before": 3.0,
    "balance_after": 1.0,
    "threshold": 2.0,
    "request_id": "req-11",
    "created_at": "2026-09-17T12:56:12.000000Z"
  }
]
```

### 8. 策略定时切换：提交、冲突、取消与到期生效

```bash
# 全局策略当前为容量 10、0.5 令牌/秒（沿用第 1 步的配置）
# 先花掉 5 个令牌，使余额为 5（剩余比例 0.5）
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-20","cost":5}'

# 提交一条 60 秒后生效的变更：容量 20、2 令牌/秒、低余额阈值 3
# （effective_at 必须是未来 UTC 时间，这里用 date 动态生成）
curl -s -X POST http://localhost:8000/policy-changes \
  -H 'Content-Type: application/json' \
  -d "{
    \"change_id\":\"chg-1\",
    \"scope\":\"global\",
    \"capacity\":20,
    \"refill_rate\":2,
    \"low_balance_threshold\":3,
    \"effective_at\":\"$(date -u -d '60 seconds' +%Y-%m-%dT%H:%M:%SZ)\"
  }"

# 待生效期间直接 PUT 该策略 -> 409
curl -i -X PUT http://localhost:8000/policies \
  -H 'Content-Type: application/json' \
  -d '{"scope":"global","capacity":100,"refill_rate":0}'
# HTTP/1.1 409 Conflict：该策略存在待生效的定时变更 change_id='chg-1' ...

# 同一策略再提交一条 pending -> 409；change_id 重复 -> 409
# 目标策略不存在 -> 404；effective_at 已过期或不带时区 -> 422

# 查看 / 过滤待生效变更
curl -s 'http://localhost:8000/policy-changes?status=pending'

# 生效前取消（重复取消幂等；已生效或已到生效时刻再取消返回 409）
curl -s -X DELETE http://localhost:8000/policy-changes/chg-1
```

到期时刻**不需要任何后台动作**：60 秒后的第一次 `POST /decide` 会在同一事务内
先应用变更再裁决。以上面余额 5 / 旧容量 10、新容量 20 为例：

1. 先按旧速率 `0.5/s` 把余额补充到 `effective_at`：`min(10, 5 + 0.5×60) = 10`；
2. 按剩余比例换算到新容量：`10 / 10 × 20 = 20`；
3. 桶的时间锚点移到 `effective_at`，之后按新速率 `2/s` 继续补充，
   新阈值 `3` 立即生效。

```bash
# 到期后的第一次裁决：响应中的桶已是新容量/新速率
curl -s -X POST http://localhost:8000/decide \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-21","cost":1}'
# buckets[0]: capacity=20.0, refill_rate=2.0, balance≈19（补充后再扣 1）

# 变更状态变为 applied；此后 PUT /policies 恢复正常，也可提交新的变更
curl -s http://localhost:8000/policy-changes/chg-1
```

### 9. 策略版本审计与回滚

每次 `PUT /policies` 成功与定时切换实际生效，都会为该策略追加一条不可变版本：

```bash
# 查询某策略的版本历史（按版本号新到旧返回）
curl -s 'http://localhost:8000/policies/history?scope=global'
curl -s 'http://localhost:8000/policies/history?scope=tenant&tenant_id=acme'
curl -s 'http://localhost:8000/policies/history?scope=route&tenant_id=acme&route_id=/v1/search'
```

版本对象：

```json
[
  {
    "version": 3,
    "scope": "global",
    "tenant_id": null,
    "route_id": null,
    "capacity": 20.0,
    "refill_rate": 2.0,
    "low_balance_threshold": 3.0,
    "source": "scheduled",
    "change_id": "chg-1",
    "restored_from_version": null,
    "created_at": "2026-09-18T00:00:00.000000Z"
  },
  {
    "version": 2,
    "scope": "global",
    "tenant_id": null,
    "route_id": null,
    "capacity": 100.0,
    "refill_rate": 0.0,
    "low_balance_threshold": null,
    "source": "put",
    "change_id": null,
    "restored_from_version": null,
    "created_at": "2026-09-17T12:56:09.556564Z"
  },
  {
    "version": 1,
    "scope": "global",
    "tenant_id": null,
    "route_id": null,
    "capacity": 10.0,
    "refill_rate": 0.5,
    "low_balance_threshold": null,
    "source": "put",
    "change_id": null,
    "restored_from_version": null,
    "created_at": "2026-09-17T12:00:00.000000Z"
  }
]
```

> `source` 取值：`put`（`PUT /policies` 成功）、`scheduled`（定时切换由裁决
> 事务实际应用，`change_id` 指向对应变更）、`rollback`（回滚产生，
> `restored_from_version` 指向被恢复的版本）、`baseline`（老库升级时按当时
> 策略补登的 version=1）。

回滚到指定版本（恢复参数 + 桶补充后截断 + 追加新版本在同一写事务内完成）：

```bash
curl -s -X POST http://localhost:8000/policies/rollback \
  -H 'Content-Type: application/json' \
  -d '{"scope":"global","version":1}'
```

```json
{
  "version": 4,
  "scope": "global",
  "tenant_id": null,
  "route_id": null,
  "capacity": 10.0,
  "refill_rate": 0.5,
  "low_balance_threshold": null,
  "source": "rollback",
  "change_id": null,
  "restored_from_version": 1,
  "created_at": "2026-09-18T09:30:00.000000Z"
}
```

> - 回滚不是「跳到旧版本」：恢复后产生的是一个**新版本号**，版本链只增不改；
> - 已有桶先按**当前**策略补充到事务时刻，再按目标版本容量截断
>   （放大容量时余额不会凭空变满）；
> - 策略存在 `pending` 定时变更时返回 `409`，请先取消或等其生效；
> - 目标策略不存在、或该版本不属于该策略，返回 `404`；
> - 回滚成功后无 `pending` 变更，可正常继续 PUT / 提交定时切换 / 再次回滚。

## 行为约定与边界

- 桶首次被命中时按当前容量**满额**创建。
- 更新策略容量时，已有桶先按旧率补充到当前时刻，再截断到新容量
  （缩小容量不会让余额超过新容量）。
- 策略存在 `pending` 变更期间，`PUT /policies` 更新该策略返回 `409`，
  避免直接更新与到期应用两条路径交错破坏余额换算；请先取消变更或等其生效。
- 定时变更到期时由下一次**命中该策略的裁决**惰性应用：未命中该策略的时间内
  什么都不做；应用与裁决在同一 `BEGIN IMMEDIATE` 事务内原子提交，
  并发请求只能看到完整旧状态或完整新状态。
- 到期应用的余额换算：旧策略补充到 `effective_at` →
  `旧余额 / 旧容量 × 新容量`（旧容量为 `0` 时余额为 `0`，随后按新速率从 0 补充）
  → 按新速率从 `effective_at` 补充至裁决时刻。新容量更小时换算结果以新容量为上限。
- 取消只对生效时刻仍在未来的 `pending` 变更有效；已应用的不可撤销
  （如需回退，再用 `PUT /policies`、提交一条反向变更，或直接
  `POST /policies/rollback` 回到历史版本）。
- 版本记录只追加、不修改、不删除：同一策略的 `version` 在写事务内
  `max(version)+1` 单调分配；PUT、定时切换生效、回滚三个来源与触发动作
  在同一 `BEGIN IMMEDIATE` 事务内提交。老库启动时自动为现有策略补登
  version=1 的 `baseline`（幂等），历史策略因此也可直接回滚。
- 低余额事件只在**获准**裁决的扣减事务内产生：拒绝、幂等重放、
  撤销与策略更新/变更应用本身都不写事件；一次裁决中每个越线桶各写一条。
- 阈值判定为「扣减前余额 > 阈值 ≥ 扣减后余额」；余额本就在阈值之下的
  扣减不会重复触发，待余额重新高于阈值后才会再次武装。
- 若一次裁决没有命中任何已配置的层级，返回 `404`（提示先配置策略）。
- `cost ≤ 0` 返回 `422`；`route_id` 必须与 `tenant_id` 一起提供，否则 `422`；
  `low_balance_threshold` 超出 `[0, capacity]` 返回 `422`。
- 裁决只保证「整组原子扣减」与最终一致；重试时间是服务端按补充率给出的建议值，
  期间若有其他裁决/撤销发生，实际结果仍以再次裁决为准。

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
QUOTA_DB_PATH=./quota.db .venv/bin/uvicorn app.main:app --reload --port 8000
```

运行测试（另需 `pip install pytest httpx`）：

```bash
.venv/bin/pip install pytest httpx
.venv/bin/python -m pytest -q
```

测试覆盖：三级原子扣减/整组拒绝不扣、按时间补充、整组重试时间、
幂等重放、载荷冲突 409、撤销补回与幂等、补回不超容量、容量更新截断、
低余额事件的越线触发/拒绝与重放不触发/补充、撤销、阈值调低后重新武装、
多级同时越线各产一条、events 稳定分页、老库迁移补列，
策略定时切换的提交约束（404/409/422）、pending 期间 PUT 冲突、
生效前取消与到期不可取消、生效前后裁决的策略可见性、
余额按旧率补充再按比例缩放再走新率、旧容量 0 归零、
新阈值到期生效、多级变更在同一事务内应用、两阶段并发下新旧状态不串且不超发，
以及 20+ 线程并发与双进程共享同一数据库文件均不超发；
策略版本审计覆盖 PUT/定时生效/回滚三个来源的版本号单调递增、
按策略键新到旧查询、老库 baseline 补登（含幂等）、回滚恢复参数并追加
rollback 版本、桶先按当前策略补充再按目标容量截断、pending 409 与
未知策略/版本 404。

## 项目结构

```
app/
  database.py   # SQLite 连接、WAL/busy_timeout、BEGIN IMMEDIATE 事务、建表与老库迁移（含 baseline 版本补登）
  quota.py      # 策略 upsert、版本审计/回滚、定时切换登记/取消/到期应用、惰性补充、整组裁决、幂等存档、撤销与低余额事件
  schemas.py    # Pydantic 请求/响应模型
  main.py       # FastAPI 路由
tests/
  test_api.py   # 行为与并发测试
Dockerfile
docker-compose.yml
```
