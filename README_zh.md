# oqbridge

[English](README.md)

一个轻量级代理，用于在 OpenSearch 和 Quickwit 之间实现自动冷热数据分层。

## oqbridge 是什么？

oqbridge 部署在 OpenSearch 和 Quickwit 前端，提供统一的查询接口和透明的数据生命周期管理：

- **热数据**（最近 30 天）→ 存储在 **OpenSearch** 中，提供快速查询性能。
- **冷数据**（超过 30 天）→ 自动迁移至 **Quickwit**，以极低的存储成本和系统开销进行保存。
- **冷数据保留期** — Quickwit 中的数据在可配置的时间后自动删除（默认：365 天）。

## 架构

oqbridge 由两个独立的二进制文件组成，职责分离：

```
OpenSearch 节点                          远端
┌───────────────────────────┐     ┌──────────────┐
│  OpenSearch (localhost)   │     │   Quickwit   │
│          ↑                │     │      ↑       │
│  oqbridge-migrate ────────┼─gzip┼──────┘       │
│  (并行 sliced scroll)     │     │              │
└───────────────────────────┘     └──────────────┘

客户端 ──► oqbridge (代理) ──┬──► OpenSearch  (热数据, <30天)
                             └──► Quickwit    (冷数据, ≥30天, 365天后自动删除)
```

- **`oqbridge`** — 查询代理。可部署在任意节点。根据时间范围将搜索请求路由到正确的后端，透明合并结果。
- **`oqbridge-migrate`** — 迁移工具。部署在 OpenSearch 节点上，本地读取数据后压缩发送到远端 Quickwit，最小化数据传输路径。

## 核心特性

### 代理 (`oqbridge`)

- **透明代理** — 通过反向代理实现完整的 OpenSearch API 兼容。
- **智能查询路由** — 根据查询的时间范围自动将支持的搜索请求路由到正确的后端。
- **通配符索引支持** — `logs-*/_search` 和 `*/_search` 等通配符查询会根据时间范围正确路由，冷数据查询时自动解析通配符匹配的 Quickwit 索引。
- **结果合并** — 并发查询两个后端，无缝合并结果。
- **可配置保留期** — 可按索引调整冷热数据阈值（默认：30 天）。
- **每索引时间字段** — 不同索引可以使用不同的时间戳字段。

### 迁移 (`oqbridge-migrate`)

- **通配符索引模式** — 配置 `indices: ["*"]` 或 `["logs-*"]` 迁移匹配的索引。通配符模式通过 OpenSearch `_cat/indices` API 解析为具体索引名。
- **系统索引过滤** — 自动排除 OpenSearch 内部索引（`.security`、`security-auditlog-*`、`top_queries-*` 等），不会被误迁移。
- **自动创建索引** — 迁移前自动在 Quickwit 中创建索引，使用动态（schemaless）模式，无需预定义 schema。
- **冷数据保留策略** — 创建 Quickwit 索引时自动配置保留策略，超过 `retention.cold_days` 天的数据由 Quickwit 自动删除。
- **并行 Sliced Scroll** — 多个 worker 使用 sliced scroll API 并发读取 OpenSearch。
- **Gzip 压缩** — 压缩传输到 Quickwit 的数据（大数据量下显著节省带宽）。
- **断点续传** — 中断的迁移自动从上次完成的 slice 恢复。
- **多实例安全** — 通过 OpenSearch 实现分布式锁，防止多个 `oqbridge-migrate` 实例同时迁移同一索引。Checkpoint 和 watermark 存储在 OpenSearch 中，所有实例共享迁移进度。
- **实时进度** — 每 10 秒输出 docs/sec、已迁移数量和耗时。
- **两种运行模式** — 单次执行 (`--once`) 适配 crontab，或内置 cron 守护模式。

## 快速开始

### 前置要求

- Go 1.22+
- 一个运行中的 OpenSearch 实例
- 一个运行中的 Quickwit 实例

### 编译

```bash
make build          # 编译两个二进制
make build-proxy    # 仅编译 oqbridge
make build-migrate  # 仅编译 oqbridge-migrate
```

### 配置

复制并编辑默认配置文件：

```bash
cp configs/oqbridge.yaml oqbridge.yaml
# 编辑 oqbridge.yaml，填入连接信息
```

### 运行代理

```bash
./bin/oqbridge -config oqbridge.yaml
```

### 运行迁移工具

```bash
# 单次模式（运行一次后退出，适合 crontab）
./bin/oqbridge-migrate -config oqbridge.yaml --once

# 守护模式（内置 cron 调度器）
./bin/oqbridge-migrate -config oqbridge.yaml
```

## 配置项

详见 [configs/oqbridge.yaml](configs/oqbridge.yaml)。

### 代理配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `server.listen` | `:9200` | 代理监听地址 |
| `opensearch.url` | `http://localhost:9201` | OpenSearch 地址 |
| `quickwit.url` | `http://localhost:7280` | Quickwit 地址 |
| `retention.days` | `30` | 热数据保留天数 |
| `retention.cold_days` | `365` | Quickwit 冷数据保留天数（0 = 永不删除） |
| `retention.timestamp_field` | `@timestamp` | 默认时间戳字段 |
| `retention.index_fields` | — | 每索引时间戳字段覆盖 |

### 迁移配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `migration.schedule` | `0 * * * *` | Cron 调度表达式（守护模式） |
| `migration.migrate_after_days` | `retention.days - 5` | 迁移超过此天数的数据（必须 < `retention.days`） |
| `migration.batch_size` | `5000` | 每批 scroll 文档数 |
| `migration.workers` | `4` | 并行 sliced scroll worker 数 |
| `migration.compress` | `true` | 启用 Gzip 压缩传输 |
| `migration.delete_after_migration` | `false` | 迁移后删除 OpenSearch 中的数据 |
| `migration.indices` | — | 需要迁移的索引模式（支持通配符：`*`、`logs-*`） |

## 数据生命周期

```text
Day 0          Day 25              Day 30                    Day 395
  │              │                   │                         │
  ▼              ▼                   ▼                         ▼
 写入 ──► OpenSearch (热) ──► 迁移到 Quickwit ──► Quickwit 自动删除
           ◄── retention.days ──►
           ◄── migrate_after_days ►
                                   ◄──── retention.cold_days (365) ────►
```

- **第 0–25 天**：数据仅存储在 OpenSearch（热层）。
- **第 25–30 天**：`oqbridge-migrate` 将数据迁移到 Quickwit。具体边界由 `migration.migrate_after_days` 控制。
- **第 30 天以后**：数据从 Quickwit（冷层）查询。如果 `delete_after_migration: true`，同时从 OpenSearch 中删除。
- **第 395 天以后**：Quickwit 自动删除超过 `retention.cold_days`（365 天）的数据。

## 认证

oqbridge 以 OpenSearch 作为唯一的用户认证源。用户只需使用 OpenSearch 的账号（如通过 OpenSearch Dashboards 登录）。Quickwit 由 oqbridge 内部通过服务账号访问，终端用户无需感知 Quickwit 的存在。

```text
OpenSearch Dashboards
        │ (用户的 Authorization header)
        ▼
    oqbridge (代理)
        │
        ├──► OpenSearch: 转发用户的 auth header，由 OpenSearch 验证
        │
        └──► Quickwit:   使用配置文件中的服务账号，用户无感知
```

### 按查询类型的认证流程

| 查询类型 | 认证流程 |
| -------- | -------- |
| **仅热数据**（近期数据） | 反向代理转发客户端的 `Authorization` header 到 OpenSearch，由 OpenSearch 验证用户。 |
| **仅冷数据**（历史数据） | oqbridge 先拿客户端凭证向 OpenSearch 发起认证请求 (`_plugins/_security/authinfo`)，验证通过后才查询 Quickwit。 |
| **跨冷热数据** | 并发查询两个后端，OpenSearch 那一路隐式验证用户身份。 |
| **非搜索请求** | 直接反向代理到 OpenSearch，由 OpenSearch 验证。 |

## Search API 支持说明

oqbridge 会将所有非搜索请求原样转发到 OpenSearch。对于搜索拦截/分层（tiering），当前支持：

- `/{index}/_search`
- `/{index1,index2}/_search`（逗号分隔的显式索引）
- `/{index-pattern*}/_search`（通配符模式，冷数据查询时自动解析）
- `/{index}/_msearch`
- `/_msearch`（要求每个 header 行都包含 `"index"`）

`/_search`（path 中不包含 index）会按原样转发到 OpenSearch。

通配符模式（如 `logs-*/_search`）完全支持时间范围路由。热数据查询时，通配符原样传递给 OpenSearch（OpenSearch 原生支持通配符）。冷数据查询时，oqbridge 会解析通配符，匹配 Quickwit 中已有的索引后查询。

### 跨冷热合并的限制

当查询跨越热+冷两个层级（fan-out + merge）时，目前仅支持基于 score 的排序：

- 默认排序（不指定 `sort`）
- 显式 `_score` 排序

对使用非 `_score` 的显式排序、`search_after` 或 PIT 的查询，oqbridge 会返回 `400`（仅针对需要跨冷热合并的场景），因为正确的全局排序需要完整的 sort-key 合并语义。

### 服务账号配置

- `opensearch.username` / `opensearch.password` — 用于 `oqbridge-migrate` 后台操作（scroll、delete）的**服务账号**。代理不会用这些凭证处理用户请求，而是直接转发客户端原始 header。
- `quickwit.username` / `quickwit.password` — 用于所有 Quickwit 访问（代理和迁移）的**服务账号**。如果 Quickwit 无认证（如网络隔离），留空即可。

### 你不需要做的事

- 不需要在 Quickwit 中创建用户
- 不需要在 OpenSearch 和 Quickwit 之间同步用户数据库
- 不需要修改 OpenSearch Dashboards 的任何配置

## 部署建议

1. **OpenSearch 节点**：部署 `oqbridge-migrate`。本地读取 OpenSearch 数据（高速），压缩后发送到远端 Quickwit。
2. **任意节点**：部署 `oqbridge` 代理，让客户端通过它访问两个后端。

两个二进制共用同一配置文件格式。

### 多实例迁移

可以安全地在同一 OpenSearch 集群上运行多个 `oqbridge-migrate` 实例，协调完全自动：

1. **分布式锁** — 迁移索引前，实例在 `.oqbridge-locks` OpenSearch 索引中获取锁（使用原子性 `op_type=create`）。如果另一个实例已持有锁，则跳过该索引。
2. **共享 checkpoint/watermark** — 迁移进度存储在 `.oqbridge-state` OpenSearch 索引中，而非本地文件。每个实例都能看到已迁移到哪个时间点，从断点继续。
3. **过期锁自动清理** — 锁包含 TTL（默认：2 小时）。如果某实例崩溃，锁过期后其他实例可以接管，从共享的 checkpoint 恢复。

```text
实例 A: 获取锁 → 迁移 [T0, T1) → 存 watermark T1 → 释放锁
实例 B: 获取锁 → 读 watermark T1 → 迁移 [T1, T2) → 存 watermark T2 → 释放锁
```

无需额外的协调服务（etcd、Consul 等）—— 直接复用 OpenSearch 本身作为协调后端。

## 许可证

[MIT](LICENSE)

## 贡献

欢迎贡献代码！请提交 Issue 或 Pull Request。
