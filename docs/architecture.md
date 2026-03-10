# RuseDB 架构总览

## 1. 目标与定位

RuseDB 是一个面向本地开发与小规模测试环境的嵌入式关系型数据库项目，核心目标是：

- 提供可用且可读的 SQL 语义。
- 在单机环境实现可恢复、可观测、可运维的最小闭环。
- 通过 HTTP API 为多语言应用提供统一接入面。

当前版本：`v1.2.1`

---

## 2. 分层架构

RuseDB 采用“自底向上”的分层设计：

1. `rusedb-core`  
   提供通用类型与错误模型：`Value`、`DataType`、`Schema`、`RuseDbError` 等。

2. `rusedb-storage`  
   提供物理存储与元数据能力：页、堆文件、Catalog、有序索引（B+Tree 风格）。

3. `rusedb-sql`  
   提供 SQL 词法/语法解析与 AST 定义，输出 `Statement`。

4. `rusedb-exec`  
   提供执行引擎 `Engine`：DDL、DML、事务、WAL 恢复、统计信息、EXPLAIN、慢日志。

5. `rusedb-server`  
   提供 CLI/TCP/HTTP 接口，补充运维命令（backup/restore/pitr/migrate）和 RBAC。

6. `rusedb-tests`  
   提供里程碑导向的集成回归测试。

---

## 3. 核心数据流

### 3.1 SQL 执行链路

1. 输入来源：CLI 命令、TCP 文本协议、HTTP `/sql`。
2. `rusedb-sql` 解析 SQL 为 AST（`Statement`）。
3. `rusedb-exec::Engine` 根据语句类型分发执行。
4. 访问 `Catalog + HeapFile + OrderedIndex` 完成读写。
5. 返回 `QueryResult`，由 CLI/HTTP 层渲染为文本或 JSON。

### 3.2 HTTP 鉴权与授权链路

1. 可选 Bearer Token 认证：`Authorization: Bearer <token>`。
2. 若已配置 RBAC 用户，还需：
   - `X-RuseDB-User`
   - `X-RuseDB-Token`
3. SQL 在执行前进行权限需求推导（数据库级/表级 + 动作）。
4. 权限不满足返回 `403`。

---

## 4. 部署形态

### 4.1 进程内 CLI 模式

- `rusedb shell`
- `rusedb sql`

适合本地调试与脚本化操作。

### 4.2 TCP 服务模式

- `rusedb init/start/status/connect/stop`

适合单机状态化服务。

### 4.3 HTTP 网关模式

- `rusedb http <catalog_base> ...`
- 路由：`/health`、`/sql`、`/admin/*`、`/openapi.json`、`/docs`

适合多语言接入。

---

## 5. 文件与元数据组织

以 `catalog_base` 为前缀，形成“文件家族”：

- `<base>.tables/.columns/.indexes/.constraints`：Catalog 元数据。
- `<base>.table-<id>.heap`：表堆文件。
- `<base>.index-<id>.idx`：索引文件。
- `<base>.wal`：WAL。
- `<base>.stats`：统计信息。
- `<base>.slowlog`：慢查询日志。
- `<base>.migrations`：迁移状态。
- `<base>.rbac`：权限与用户信息。
- `<base>.backup.lock`：在线备份锁文件。

多库场景使用 `-db-<name>` 前缀扩展同构文件家族。

---

## 6. 关键运行特性

- 事务隔离级别：`READ COMMITTED`。
- WAL 事件：`BEGIN`、`COMMIT_START`、`COMMIT_DONE`、`CHECKPOINT`。
- 写入路径在无显式事务时自动包裹事务（自动 begin/commit）。
- `EXPLAIN/EXPLAIN ANALYZE` + 统计信息驱动访问路径选择（全表扫 vs 索引）。
- 在线备份通过备份锁与写事务协同，保证快照一致性窗口。
- PITR（阶段一）通过“快照时间点选择”实现恢复。

---

## 7. 可观测与运维

- 慢查询日志：`<base>.slowlog`
- 表统计信息：`<base>.stats`
- HTTP 管理接口：
  - `GET /admin/databases`
  - `GET /admin/stats?db=...`
  - `GET /admin/slowlog?db=...&limit=...`
- 运维命令：
  - `backup/restore/pitr`
  - `migrate up/down`

---

## 8. 当前边界（单机版本）

- 以单机/单进程场景为主，未实现分布式复制与高可用集群。
- SQL 覆盖面聚焦常用关系操作，不等价于 MySQL/PostgreSQL 全语法。
- 对外推荐通过 HTTP API 接入，不直接兼容 MySQL/PostgreSQL 协议驱动。
