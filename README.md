
# RuseDB
<img width="1024" height="1024" alt="RuseDB_LOGO" src="https://github.com/user-attachments/assets/f0c08fee-8163-4e11-a643-a4bdef202fcf" />

<p align="center">
  <strong>一个用 Rust 实现的轻量关系型数据库内核</strong><br/>
  面向本地开发、小型系统与教学场景，聚焦“可运行、可恢复、可迭代”。
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.85%2B-black?logo=rust" />
  <img alt="Version" src="https://img.shields.io/badge/version-v1.2.1-blue" />
  <img alt="License" src="https://img.shields.io/badge/license-MIT-green" />
</p>

## 目录

- [项目定位](#项目定位)
- [核心能力](#核心能力)
- [快速开始](#快速开始)
- [HTTP API 接入](#http-api-接入)
- [主流语言调用示例](#主流语言调用示例)
- [命令总览](#命令总览)
- [文档导航](#文档导航)
- [项目结构](#项目结构)
- [当前边界](#当前边界)
- [开发与测试](#开发与测试)
- [版本路线](#版本路线)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

## 项目定位

`RuseDB` 是一个从零实现的 Rust 数据库项目，当前版本为 **V1.2.1**。

当前阶段目标：

- 已基本实现数据库最简运行需求：建库建表、增删改查、基础事务恢复、CLI/TCP/HTTP 接入。
- 为下一阶段的高并发、高安全、高可用演进打下可持续内核基础。

## 核心能力

### 1. SQL 能力（已实现）

- 多库管理：
  - `CREATE DATABASE` / `DROP DATABASE` / `USE`
  - `SHOW DATABASES` / `SHOW CURRENT DATABASE`
- 表与索引：
  - `CREATE TABLE` / `DROP TABLE` / `CREATE INDEX`
  - `ALTER TABLE ADD/DROP/ALTER COLUMN`
  - `RENAME TABLE` / `RENAME COLUMN`
- 约束：
  - `PRIMARY KEY`（列级/表级）
  - `UNIQUE`（列级/表级，多列）
  - `FOREIGN KEY ... REFERENCES ...`（`RESTRICT`）
- DML：
  - `INSERT` / `SELECT` / `UPDATE` / `DELETE`
- 查询：
  - `WHERE` / `ORDER BY` / `LIMIT`
  - `JOIN` / `LEFT JOIN`
  - `GROUP BY` / `HAVING`
  - 聚合函数 `COUNT/SUM/MIN/MAX`
  - `IN` / `LIKE` / `BETWEEN` / `IS NULL`
  - `IN (SELECT ...)` 与标量子查询

### 2. 事务与恢复

- 事务语句：`BEGIN` / `COMMIT` / `ROLLBACK`
- 隔离级别：`READ COMMITTED`
- 基础 WAL 恢复与 checkpoint
- 锁等待超时机制

### 3. 存储与执行

- Heap 文件持久化
- Catalog 元数据持久化（表、列、索引、约束）
- 有序索引能力
- `EXPLAIN` / `EXPLAIN ANALYZE`
- 慢查询日志与基础统计信息

### 4. 接入方式

- 本地 Shell：`rusedb shell [catalog_base]`
- 一次性 SQL：`rusedb sql <catalog_base> "<sql 批次>"`
- TCP 服务：`init/start/status/connect/stop`
- HTTP 网关：`rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]`
- OpenAPI：`GET /openapi.json`，文档页：`GET /docs`

### 5. 运维能力（阶段化）

- 备份：`backup`（offline / online）
- 恢复：`restore`
- 时间点恢复（PITR 阶段一）：`pitr`
- 迁移：`migrate up/down`
- RBAC：`user-add` / `user-list` / `grant`

## 快速开始

### 1) 环境准备

- Rust 1.85+
- Windows PowerShell / macOS / Linux 终端

### 2) 克隆与安装

```bash
git clone https://github.com/LUOQUE-23/RuseDB.git
cd RuseDB
cargo install --path crates/rusedb-server --force
```

验证：

```bash
rusedb --version
rusedb help
```

### 3) 启动本地 Shell

```bash
rusedb shell ./tmp/rusedb
```

示例 SQL：

```sql
CREATE DATABASE app;
USE app;
CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR);
INSERT INTO users (id, name) VALUES (1, 'alice');
SELECT * FROM users;
```

Shell 体验增强：

- 支持多行 SQL 输入，以 `;` 结束执行
- `.help` 查看交互命令
- `.clear` 清空当前未结束 SQL
- 表格输出自动对齐，长字段截断避免错位

## HTTP API 接入

### 启动 HTTP 服务

```bash
rusedb http ./tmp/rusedb 127.0.0.1:18080 --token my-secret --allow-origin http://localhost:3000
```

### 核心路由

- `GET /health`
- `POST /sql`
- `GET /admin/databases`
- `GET /admin/stats?db=default`
- `GET /admin/slowlog?db=default&limit=200`
- `GET /openapi.json`
- `GET /docs`

### 调用示例

```bash
curl -X POST http://127.0.0.1:18080/sql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret" \
  -d '{"sql":"SHOW DATABASES;"}'
```

## 主流语言调用示例

说明：RuseDB 当前不兼容 MySQL/PostgreSQL 线协议，推荐统一走 HTTP API。

### Python

```python
import requests

resp = requests.post(
    "http://127.0.0.1:18080/sql",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer my-secret",
    },
    json={"sql": "USE app; SELECT * FROM users;"},
    timeout=10,
)
print(resp.json())
```

### Node.js

```javascript
const res = await fetch("http://127.0.0.1:18080/sql", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "Authorization": "Bearer my-secret",
  },
  body: JSON.stringify({ sql: "SHOW DATABASES;" }),
});
console.log(await res.json());
```

### Go

```go
package main

import (
  "bytes"
  "fmt"
  "io"
  "net/http"
)

func main() {
  body := []byte(`{"sql":"SHOW DATABASES;"}`)
  req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/sql", bytes.NewBuffer(body))
  req.Header.Set("Content-Type", "application/json")
  req.Header.Set("Authorization", "Bearer my-secret")

  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    panic(err)
  }
  defer resp.Body.Close()

  data, _ := io.ReadAll(resp.Body)
  fmt.Println(string(data))
}
```

## 命令总览

```text
rusedb --version
rusedb help
rusedb init [instance_dir]
rusedb start [instance_dir]
rusedb status [instance_dir]
rusedb stop [instance_dir]
rusedb connect [instance_dir]
rusedb shell [catalog_base]
rusedb create-table <catalog_base> <table_name> <col:type[?]>...
rusedb drop-table <catalog_base> <table_name>
rusedb show-tables <catalog_base>
rusedb describe <catalog_base> <table_name>
rusedb parse-sql <sql>
rusedb sql <catalog_base> <sql-or-batch>
rusedb backup <catalog_base> <backup_dir> [--online | --mode <offline|online>]
rusedb restore <backup_dir> <catalog_base>
rusedb pitr <backup_root_dir> <catalog_base> <target_unix_ms>
rusedb migrate up <catalog_base> <migrations_dir>
rusedb migrate down <catalog_base> <migrations_dir> <steps>
rusedb user-add <catalog_base> <username> <token>
rusedb user-list <catalog_base>
rusedb grant <catalog_base> <username> <scope> <actions_csv>
rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]
```

## 文档导航

- 架构总览：[docs/architecture.md](docs/architecture.md)
- 存储引擎：[docs/storage_engine.md](docs/storage_engine.md)
- 事务机制：[docs/transaction.md](docs/transaction.md)
- 查询引擎：[docs/query_engine.md](docs/query_engine.md)
- 使用手册：[docs/usage_manual.md](docs/usage_manual.md)

## 项目结构

```text
.
├─ crates/
│  ├─ rusedb-core/      # 通用类型/错误模型
│  ├─ rusedb-sql/       # SQL 词法/语法解析与 AST
│  ├─ rusedb-storage/   # 页/堆文件/Catalog/索引
│  ├─ rusedb-exec/      # 执行引擎/事务/WAL
│  ├─ rusedb-server/    # CLI/TCP/HTTP 服务入口
│  └─ rusedb-tests/     # 集成与回归测试
├─ docs/
└─ README.md
```

## 当前边界

- 以单机部署为主，未实现分布式复制与自动故障切换。
- 并发能力处于基础阶段，适合中低并发业务。
- SQL 兼容为实用子集，不等价于 MySQL/PostgreSQL 全语法。

## 开发与测试

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## 版本路线

- `v1.x`：打磨轻量内核能力，持续补齐实用 SQL 与运维闭环。
- 下一阶段重点：高并发、高安全、高可用。

## 贡献指南

欢迎提交 Issue / PR。建议流程：

1. Fork 仓库并创建功能分支。
2. 提交前运行 `cargo fmt`、`cargo test`、`cargo clippy`。
3. 在 PR 中附上改动说明、测试结果与影响范围。

## 许可证

本项目基于 **MIT License** 开源发布。详见 [LICENSE](LICENSE)。
