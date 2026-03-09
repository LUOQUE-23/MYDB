# RuseDB

![RuseDB Logo](./RuseDB_LOGO.png)

`RuseDB` 是一个用 Rust 实现的个人数据库项目，目标是做到本地可用、可持续迭代、便于跨语言接入。

当前版本：`RuseDB V1.0.1`

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
  - `ALTER TABLE ... ADD COLUMN ...`
  - `ALTER TABLE ... DROP COLUMN ...`（受限规则）
  - `ALTER TABLE ... ALTER COLUMN ...`（类型/空约束受限变更）
  - `RENAME TABLE ... TO ...`
  - `RENAME COLUMN ... TO ...`
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

## 常用命令

```powershell
rusedb help
rusedb parse-sql "SELECT * FROM users"
rusedb create-table <catalog_base> <table_name> <col:type[?]>...
rusedb drop-table <catalog_base> <table_name>
rusedb show-tables <catalog_base>
rusedb describe <catalog_base> <table_name>
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

## 下一阶段更新：查询能力增强（P1）

### C1. 谓词与表达式

- [ ] `IN`
- [ ] `LIKE`
- [ ] `BETWEEN`
- [ ] `IS NULL / IS NOT NULL`

### C2. JOIN 与聚合

- [ ] `LEFT JOIN`
- [ ] `HAVING`
- [ ] 聚合与分组边界场景测试

### C3. 子查询（分阶段）

- [ ] 第一阶段：`IN (SELECT ...)`
- [ ] 第二阶段：标量子查询

### C 验收标准

- [ ] 常见业务 SQL 不需要大量在应用层改写
- [ ] SQL 解析报错位置准确、信息可读
