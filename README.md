# RuseDB

![RuseDB Logo](./RuseDB_LOGO.png)

`RuseDB` 是一个用 Rust 实现的个人数据库项目，目标是做到本地可用、可持续迭代、便于跨语言接入。

当前版本：`RuseDB V1.2.1`

版本说明（V1.2.1）：
- 当前版本已基本实现数据库最简运行需求（建库建表、增删改查、基础事务恢复、CLI/TCP/HTTP 接入）。
- 后续将重点改进高并发、高安全、高可用能力。

## 当前已实现功能

### 1. SQL 能力
- 多库管理：
  - `CREATE DATABASE <name>`
  - `DROP DATABASE <name>`
  - `USE <name>`
  - `SHOW DATABASES`
  - `SHOW CURRENT DATABASE`
- 表和索引管理：
  - `CREATE TABLE ...`
  - `PRIMARY KEY`（列级/表级）
  - `UNIQUE`（列级/表级，支持多列）
  - `FOREIGN KEY ... REFERENCES ...`（支持 `RESTRICT`）
  - `DROP TABLE ...`
  - `SHOW TABLES`
  - `CREATE INDEX ... ON ...(...)`
- 数据操作：
  - `INSERT`
  - `SELECT`
  - `UPDATE`
  - `DELETE`
- 查询能力：
  - `WHERE`
  - `ORDER BY`
  - `LIMIT`
  - `JOIN ... ON`
  - `GROUP BY`
  - `COUNT/SUM/MIN/MAX`

### 2. 事务与恢复
- 支持事务语句：
  - `BEGIN`
  - `COMMIT`
  - `ROLLBACK`
- 已实现基础 WAL 恢复能力。

### 3. 存储层
- 堆文件持久化。
- Catalog 元数据持久化（表、列、索引）。
- 有序索引能力。

### 4. 使用接口
- 本地交互式 SQL Shell：
  - `rusedb shell [catalog_base]`
- 一次性 SQL 执行：
  - `rusedb sql <catalog_base> "<sql 批次>"`
- 备份与恢复（F1）：
  - `rusedb backup <catalog_base> <backup_dir> [--online | --mode <offline|online>]`
  - `rusedb restore <backup_dir> <catalog_base>`
  - `rusedb pitr <backup_root_dir> <catalog_base> <target_unix_ms>`
- 状态化 TCP 服务模式：
  - `rusedb init/start/status/connect/stop`
- HTTP API 网关（跨语言推荐）：
  - `rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]`

### 5. HTTP API 增强能力
- Bearer Token 认证（可选开启）。
- CORS（支持 `OPTIONS` 预检）。
- OpenAPI 文档：
  - `GET /openapi.json`
  - `GET /docs`（Swagger UI）

---

## 快速开始

### 1) 安装命令（让 `rusedb` 全局可用）

在项目根目录执行：

#### Windows（PowerShell）

```powershell
cargo install --path crates/rusedb-server --force
```

重开终端后验证：

```powershell
rusedb --version
```

#### macOS / Linux（bash 或 zsh）

```bash
cargo install --path crates/rusedb-server --force
source ~/.cargo/env
hash -r
rusedb --version
which rusedb
```

### 2) 本地直接使用（最简）

Windows：

```powershell
rusedb shell .\tmp\rusedb
```

macOS / Linux：

```bash
rusedb shell ./tmp/rusedb
```

交互体验优化（shell / connect）：

- 支持多行 SQL 输入，使用 `;` 作为结束符执行。
- 输入 `.help` 查看交互命令帮助。
- 输入 `.clear` 清空当前未结束的多行 SQL 缓冲。
- 查询结果以对齐表格显示，长字段会自动截断避免错位。

### 3) 一次性执行 SQL

Windows：

```powershell
rusedb sql .\tmp\rusedb "CREATE DATABASE app; USE app; CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR); INSERT INTO users (id, name) VALUES (1, 'alice'); SELECT * FROM users;"
```

macOS / Linux：

```bash
rusedb sql ./tmp/rusedb "CREATE DATABASE app; USE app; CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR); INSERT INTO users (id, name) VALUES (1, 'alice'); SELECT * FROM users;"
```

---

## TCP 服务模式

初始化实例：

```powershell
rusedb init rusedb-instance
```

启动服务：

```powershell
rusedb start rusedb-instance
```

连接并交互执行：

```powershell
rusedb connect rusedb-instance
```

查看状态与停服：

```powershell
rusedb status rusedb-instance
rusedb stop rusedb-instance
```

说明：以上命令在 Windows、macOS、Linux 完全一致。

---

## HTTP API 模式（推荐跨语言接入）

启动 HTTP 服务（示例）：

Windows：

```powershell
rusedb http .\tmp\rusedb 127.0.0.1:18080 --token my-secret --allow-origin http://localhost:3000
```

macOS / Linux：

```bash
rusedb http ./tmp/rusedb 127.0.0.1:18080 --token my-secret --allow-origin http://localhost:3000
```

### 路由
- `GET /health`
- `POST /sql`
- `GET /openapi.json`
- `GET /docs`

### 调用示例

```bash
curl -X POST http://127.0.0.1:18080/sql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret" \
  -d "{\"sql\":\"SHOW DATABASES;\"}"
```

### 请求体

```json
{
  "sql": "SHOW DATABASES; SELECT * FROM users;"
}
```

### 成功响应示例

```json
{
  "ok": true,
  "results": [
    {
      "type": "rows",
      "columns": ["database"],
      "rows": [["default"]]
    }
  ]
}
```

### 失败响应示例

```json
{
  "ok": false,
  "error": "statement `SELECT * FROM missing` failed: table 'missing' not found"
}
```

---

## 备份与 PITR（阶段一）

- `offline` 备份：直接复制 catalog 家族文件，适合停写窗口。
- `online` 备份：会创建 `<catalog_base>.backup.lock`，阻止新写事务进入，并等待当前活跃事务结束后再快照。
- `pitr`（阶段一）：从备份根目录中选择 `created_unix_ms <= target_unix_ms` 的最新快照进行恢复。

示例：

```powershell
rusedb backup .\tmp\rusedb .\tmp\backup-20260310 --online
rusedb pitr .\tmp .\tmp\rusedb 1760000000000
```

---

## 常用命令

```powershell
rusedb help
rusedb parse-sql "SELECT * FROM users"
rusedb create-table <catalog_base> <table_name> <col:type[?]>...
rusedb drop-table <catalog_base> <table_name>
rusedb show-tables <catalog_base>
rusedb describe <catalog_base> <table_name>
rusedb backup <catalog_base> <backup_dir>
rusedb backup <catalog_base> <backup_dir> --online
rusedb restore <backup_dir> <catalog_base>
rusedb pitr <backup_root_dir> <catalog_base> <target_unix_ms>
rusedb migrate up <catalog_base> <migrations_dir>
rusedb migrate down <catalog_base> <migrations_dir> <steps>
rusedb user-add <catalog_base> <username> <token>
rusedb user-list <catalog_base>
rusedb grant <catalog_base> <username> <scope> <actions_csv>
```

---

## 构建与测试

Windows（PowerShell）：

```powershell
cargo check --workspace
cargo test --workspace
```

macOS / Linux（bash/zsh）：

```bash
cargo check --workspace
cargo test --workspace
```

---

## macOS / Linux 补充说明

### 1) 直接运行 release 二进制

```bash
cargo build --release -p rusedb
./target/release/rusedb --version
```

### 2) 如果终端提示找不到 `rusedb`

```bash
echo $PATH
ls ~/.cargo/bin/rusedb
source ~/.cargo/env
```

### 3) Linux 后台运行 HTTP 服务（简单方式）

```bash
nohup rusedb http ./tmp/rusedb 127.0.0.1:18080 --token my-secret > rusedb-http.log 2>&1 &
```

查看进程：

```bash
ps -ef | grep rusedb
```

---

## 说明

- 当前 `rusedb` 使用自定义 SQL 解析/执行与协议，不直接兼容 MySQL/PostgreSQL 驱动。
- 若需给 Java/Python/Node/Go 等多语言使用，建议统一通过 HTTP API 接入。
- SQL 支持矩阵见：`docs/sql-support-matrix.md`
- 错误码文档见：`docs/error-codes.md`
- 运维手册见：`docs/ops-runbook.md`
- 架构总览：`docs/architecture.md`
- 存储引擎：`docs/storage_engine.md`
- 事务机制：`docs/transaction.md`
- 查询引擎：`docs/query_engine.md`
- 使用说明书：`docs/usage_manual.md`
- 基线记录见：`benchmarks/baseline-v1.2.0.md`

## 下一步更新计划：DDL 演进能力（P1）

### B1. ALTER TABLE

- [ ] `ALTER TABLE ... ADD COLUMN`
- [ ] `ALTER TABLE ... DROP COLUMN`（受限规则先行）
- [ ] `ALTER TABLE ... ALTER COLUMN`（后续可扩）

### B2. RENAME

- [ ] `RENAME TABLE`
- [ ] `RENAME COLUMN`
- [ ] 关联元数据同步（索引、约束引用）

### B3. 安全与兼容

- [ ] DDL 事务边界与失败回滚策略
- [ ] 向后兼容测试（旧数据目录升级）

### B 验收标准

- [ ] 基本 schema 演进可在不重建库的前提下完成
- [ ] DDL 异常不破坏 catalog 一致性


