# 接入契约 v1

## 身份与配额

业务后端使用 HTTP Basic 认证，用户名为接入方编号，密码为高熵随机接入密钥。密钥只保存在可信后端。生产入口使用 HTTPS。接入方编号由认证信息确定，请求体中没有可切换接入方的字段。

示例配置提供 `demo` 和 `partner` 两个独立接入方，凭证由 `.env` 生成。Spring Boot 的配置列表整体替换，新增接入方时需提供完整 `sale.clients` 列表，包含每个接入方的 `id`、`secret`、`admin` 以及可选回调设置。`operator=true` 授予查看全局运维指标的权限，示例只向本机演示接入方授予该权限，生产环境应配置独立运维身份。

同一业务的小程序与网页共用接入方编号，并映射到相同业务用户编号。业务后端验证用户登录、资格与操作归属，随后调用服务。独立业务使用独立接入方凭证。

创建活动前，业务系统应先划拨独占配额。例如总库存 1000 件，其中 100 件划给秒杀，普通销售渠道可使用的库存相应减少。每个活动的配额互相独立，活动之间共享外部实物库存的协调由接入方完成。

## 接口

所有路径以 `/api/v1` 开始。创建活动和管理失败通知需要该接入方具有 `admin` 权限。

| 方法 | 路径 | 行为 |
| --- | --- | --- |
| POST | `/activities` | 创建不可变活动，返回 201 |
| GET | `/activities` | 查看当前接入方最近 100 场活动 |
| GET | `/activities/{id}` | 查看活动及剩余配额 |
| POST | `/requests` | Redis 原子预扣并保存待投递申请，返回 202，MySQL 异步落库 |
| GET | `/requests/{requestId}` | 查询当前接入方下的结果 |
| POST | `/requests/{requestId}/confirm` | 支付核验后确认成交 |
| POST | `/requests/{requestId}/release` | 取消排队或有效预占 |
| GET | `/events/failed` | 查看最近 100 条失败业务通知 |
| POST | `/events/{eventId}/retry` | 重新投递一条失败通知，返回 204 |

请求与响应为 JSON（`Content-Type: application/json`）。字段约束如下，跨字段规则还要求 `endsAt > startsAt`、结束时间晚于数据库当前时间、`limitPerUser <= stock`。

| 字段 | 要求 |
| --- | --- |
| `resourceId` / `userId` / `orderId` | 非空白字符串，最多 128 字符 |
| `title` | 非空白字符串，最多 120 字符 |
| `stock` | 整数，1～1,000,000,000 |
| `limitPerUser` / `quantity` | 整数，1～10,000；单次 quantity 不得超过活动限购 |
| `holdSeconds` | 整数，1～86,400，获得预占后开始计时 |
| `queueTimeoutSeconds` | 整数，1～3,600，实际排队截止时间不晚于活动结束 |
| `startsAt` / `endsAt` | ISO 8601 时间，建议使用 UTC，例如 `2030-01-01T08:00:00Z` |
| `requestId` / `activityId` / 路径中的记录编号 | UUID 字符串 |

创建活动请求如下。

```json
{
  "resourceId": "ticket-weekend-standard",
  "title": "周末音乐会普通票",
  "stock": 100,
  "limitPerUser": 2,
  "holdSeconds": 300,
  "queueTimeoutSeconds": 30,
  "startsAt": "2026-11-07T08:00:00Z",
  "endsAt": "2026-11-07T09:00:00Z"
}
```

调用时使用适合实际活动的时间。单资源按数量预占，资源编号由业务系统定义。活动第一版创建后不可编辑，避免运行中变更配额和规则造成歧义。

抢购请求如下。

```json
{
  "requestId": "87601035-6e26-450a-aa94-a0bf9731daa8",
  "activityId": "928380e5-e5df-4ab1-a44c-8b6a14f9738c",
  "userId": "business-user-123",
  "quantity": 1
}
```

请求编号由可信业务后端生成并保存。同一次逻辑操作在超时、429、503 或网络断开后，用相同编号和相同参数重试。编号与原始参数绑定，换参数返回 `409 IDEMPOTENCY_CONFLICT`。重复消息和重复提交只能改变一次库存。

结果状态包括 `QUEUED`、`RESERVED`、`CONFIRMED`、`RELEASED`、`EXPIRED`、`REJECTED`。Redis 入口可直接返回售罄或累计限购失败的 `REJECTED`，对应记录随后异步落库。排队失败原因包括 `SOLD_OUT`、`LIMIT_EXCEEDED`、`QUEUE_TIMEOUT` 和 `ACTIVITY_CLOSED`。入口使用 Redis 时间计算排队期限，预占成功时返回付款到期时间 `expiresAt`，最终处理及成交有效期由服务数据库时间裁定。

确认成交请求如下。

```json
{ "orderId": "business-order-123" }
```

业务后端先核验真实支付，再提交确认。相同预占编号与订单号可重复确认，已确认后不能通过释放接口退库存。售后退款和活动库存回补需要额外业务契约。

## 消息与回调

提交时，Redis Lua 同时保存预扣状态与待投递记录，热路径不写 MySQL。后台使用完整申请消息异步落库，数据库状态变更时在同一事务保存 Redis 同步事件及可选 Webhook。消息队列暂时中断时，请求仍可在 Redis 受理并等待重投，超过排队期限的待处理申请由后台执行数据库超时判断并归还预扣。已发送但尚未完成的申请也可重复投递，消费者通过请求编号和状态幂等处理。

后台按同租户、同活动组成生产端批次（`SALE_BATCH_SIZE`，默认 16，设为 1 即逐条投递），正常业务拒绝只影响对应申请，技术异常使该小批次的数据库写入整体回滚后重试。批次因数据或不变式错误回滚时，消费者逐条重试，把失败范围收窄到单条申请。事务成功返回后确认消息，结果同步和 Webhook 故障由 Outbox 补偿。发布失败默认 1 秒起步退避，上限 30 秒；确认后等待结果默认 15 秒起步，上限 60 秒，均加入 20% 抖动并遵守活动排队期限。配置和令牌隔离见 [投递与重试](architecture.md#投递与重试)。

新版消费者兼容旧单条 JSON 对象和新批量数组。启用批量投递前先完成消费者升级，旧版消费者会拒绝数组消息。本机部署可先停止旧 worker，再统一启动新版。RabbitMQ 队列长度按消息条数统计，一条消息最多承载 16 个逻辑申请；调小 `SALE_BATCH_SIZE` 无需停机，消费端始终接受 16 条。

Redis 部署需使用持久卷、AOF、`appendfsync always` 和 `noeviction`。库存台账与申请去重记录持续保留，首次初始化会导入 MySQL 中已有的活动、额度及申请。升级前停止旧版写入实例，按需归档历史记录并评估初始化成本。备份恢复需协调三项存储，单独清空或回滚 Redis 会丢失未落库申请及预扣依据。活动接口中的剩余库存来自 MySQL，异步落库期间与 Redis 的可预扣数量可能短暂不同。

数据库释放成功而 Redis 同步暂时失败时，台账在后台补偿完成前可能继续占用额度，新申请可能因库存或限购被拒绝。同步任务会持续重试，重复或乱序事件不能重复归还额度或重新占用已释放库存。

通过服务端配置 `SALE_WEBHOOK_URL` 与独立随机 `SALE_WEBHOOK_SECRET` 启用 `demo` 接入方的结果通知，其他接入方可配置对应完整列表。回调地址属于运维配置，调用方不能在请求中指定任意地址。生产回调使用 HTTPS，接收方及时返回 2xx。

请求头包含 `X-Event-Id`、`X-Event-Timestamp` 和 `X-Signature`。签名内容按 UTF-8 编码，格式为以下公式。

```text
X-Signature = "sha256=" + hex(HMAC_SHA256(secret, eventId + "." + timestamp + "." + rawBody))
```

接收方在处理业务前校验签名和时间窗口，例如 5 分钟，并持久化事件编号去重。每次投递重新生成时间戳，相同业务事件的事件编号和正文保持稳定。通知可能重复或乱序，涉及冲突时查询权威预占状态。

`X-Event-Timestamp` 为 Unix 秒数。正文中的 `type` 为 `reservation.reserved`、`reservation.confirmed`、`reservation.released`、`reservation.expired` 或 `reservation.rejected`，`reservation` 使用与查询接口相同的预占结构。例如：

```json
{
  "eventId": "39c824e3-93eb-43ba-b2d2-38b8edf12379",
  "type": "reservation.reserved",
  "occurredAt": "2030-01-01T08:00:01Z",
  "reservation": {
    "requestId": "87601035-6e26-450a-aa94-a0bf9731daa8",
    "activityId": "928380e5-e5df-4ab1-a44c-8b6a14f9738c",
    "userId": "business-user-123",
    "quantity": 1,
    "state": "RESERVED",
    "reason": null,
    "orderId": null,
    "queueDeadline": "2030-01-01T08:00:30Z",
    "expiresAt": "2030-01-01T08:05:01Z",
    "createdAt": "2030-01-01T08:00:00Z",
    "updatedAt": "2030-01-01T08:00:01Z"
  }
}
```

签名必须使用接收到的原始正文，不能先解析 JSON 再重新序列化；比较签名使用恒定时间比较。以上仅为字段示例，不代表可复用的真实事件。

业务通知失败按退避策略重试，累计 12 次失败进入 `DEAD`，可通过接口重投。Redis 同步任务持续重试，直到成功更新库存台账和移除待投递记录。消费者异常重试后会进入死信队列，Redis 待投递申请仍会在租约到期后重新发送；超过排队期限时后台直接执行落库和到期处理。数据库持续不可用时，申请完成与预扣归还也会延后。

## 错误与访问

| HTTP 状态 | 处理方式 |
| --- | --- |
| 400 | 修正请求参数 |
| 401 / 403 | 检查后端凭证、权限 |
| 404 | 记录不存在或不属于当前接入方 |
| 409 | 检查幂等参数、活动状态或预占状态 |
| 429 | 退避后使用原请求编号重试 |
| 503 | 查询原请求结果，再按原编号重试 |

错误响应使用 `code` 和 `message`。确认时恰好发现预占过期会提交释放事务，再返回 409 和已过期结果。若后台已提前完成过期处理，则返回 409 错误对象，业务侧应查询最终状态。

查询结果默认建议 2 秒起步，加入随机抖动和退避，进入终态后停止。接入方应限制查询频率，避免轮询形成新的流量高峰。进入排队的顺序与最终成功顺序可能因调度、重试和多消费者发生变化。

认证失败或权限不足由 Spring Security 处理，调用方应先检查 HTTP 状态，不应假设所有 401/403 都符合业务错误 JSON 结构。

健康检查为 `/actuator/health`。Prometheus 指标位于 `/actuator/prometheus`，要求具有 `operator` 权限的运维身份，业务管理员仅能管理本接入方的活动与通知。当前管理凭证按配置加载，轮换凭证需要更新部署配置并重启实例。

## 业务后端需要实现的接口

本服务没有提供面向终端用户的登录、订单或支付接口。`miniprogram/sale-api.ts` 调用的是业务后端 `/sale` 路径，以下为接入方需要实现的契约，当前仓库未实现这一层。

| 业务后端接口 | 请求/响应与必要处理 | 调用秒杀服务 |
| --- | --- | --- |
| `GET /sale/activities` | 校验用户会话与可见资格，返回允许参与的活动列表 | `GET /api/v1/activities` |
| `POST /sale/requests` | 接收 `requestId`、`activityId`、`quantity`；校验会话和资格，注入可信 `userId`；保存操作与请求编号的映射供重试 | `POST /api/v1/requests` |
| `GET /sale/requests/{id}` | 核验该请求属于当前用户，再返回状态、到期时间与失败原因 | `GET /api/v1/requests/{id}` |
| `POST /sale/requests/{id}/cancel` | 核验请求归属，取消排队或预占；已成交转入业务售后流程 | `POST /api/v1/requests/{id}/release` |
| 业务下单接口（路径由业务定义） | 取得有效预占后创建订单并返回支付参数；下单失败主动释放；避免重复订单 | 查询预占，必要时 release |
| 支付平台回调（路径由业务定义） | 验证签名、商户、订单与金额，去重支付通知；确认失败需查询最终状态并退款或补偿 | `POST /api/v1/requests/{id}/confirm`，提交可信 `orderId` |
| 结果 Webhook（可选，路径由业务配置） | 验签、限制时间窗口、事件去重、可靠保存后返回 2xx；处理乱序或查询权威结果 | 接收服务端通知 |

小程序封装要求提交 `requestId`；业务后端必须为同一次操作生成或受控分配编号，并保证网络重试复用该编号。用户令牌与接入方密钥是两种不同凭证：前者认证用户，后者仅用于业务后端调用本服务。业务后端不能把 `/api/v1` 原样转发给任意用户；本服务只验证接入方身份，不替代用户级归属校验。

接入顺序：划拨独占配额并创建活动 → 统一用户编号 → 提交与查询 → 取得 `RESERVED` 后下单支付 → 验签后确认 → 完成履约；取消、超时、晚到付款和通知重试分别进入既定补偿流程。真实接入需求及验收范围见 [需求文档](requirements.md)，新增功能见 [扩展需求](extensions.md)。
