# Changelog

## Unreleased

### Added

- F1 backup enhancements:
  - `rusedb backup <catalog_base> <backup_dir> --online`
  - backup mode parsing (`offline` / `online`)
  - online backup lock coordination (`<catalog_base>.backup.lock`)
- PITR phase 1:
  - `rusedb pitr <backup_root_dir> <catalog_base> <target_unix_ms>`
  - selects latest snapshot where `created_unix_ms <= target_unix_ms`
- New docs:
  - `docs/sql-support-matrix.md`
  - `docs/error-codes.md`

### Changed

- Engine write-entry path now waits when backup lock exists.
- Added regression tests for:
  - backup lock blocking new mutations
  - unique conflict inside transaction rollback path

## v1.2.1 (2026-03-10)

本版本定位为基础内核稳定发布版本。

### 发布说明

- 当前版本已基本实现数据库最简运行需求：
  - 基础 SQL（建库建表、增删改查、常见查询）
  - 基础事务与 WAL 恢复
  - CLI/TCP/HTTP 接入能力
- 后续版本将重点演进：
  - 高并发能力
  - 高安全能力
  - 高可用能力

## v1.2.0 (2026-03-10)

本版本聚焦 Milestone C/D/E 以及 Milestone F 的部分能力，重点提升 SQL 表达力、事务恢复、可观测性与运维接入能力。

### 已完成

- Milestone C（查询能力增强）
  - `IN` / `LIKE` / `BETWEEN` / `IS NULL` / `IS NOT NULL`
  - `LEFT JOIN` / `HAVING`
  - `IN (SELECT ...)` 与标量子查询
- Milestone D（事务并发与恢复）
  - `READ COMMITTED` 隔离级别
  - 锁等待超时与并发写入保护
  - WAL 版本化、崩溃恢复增强、checkpoint
- Milestone E（优化器与可观测）
  - 统计信息采集与刷新
  - `EXPLAIN` / `EXPLAIN ANALYZE`
  - 慢查询日志与基础 profiling 指标
- Milestone F2（权限系统）
  - 用户管理命令：`user-add` / `user-list`
  - 基础 RBAC（库级、表级）：`grant`
  - HTTP SQL 与管理接口的 RBAC 鉴权（`X-RuseDB-User` / `X-RuseDB-Token`）
- Milestone F3（接入体验，部分）
  - HTTP 管理接口：`/admin/databases`、`/admin/stats`、`/admin/slowlog`
  - 迁移工具：`migrate up` / `migrate down`

### 下个版本预告（v1.3.0）

- Milestone F1：在线/离线备份能力完善
- Milestone F1：PITR（时间点恢复）阶段化实现
- Milestone F3：错误码文档补齐
- 横向工程项补齐：
  - SQL 支持矩阵与文档同步
  - 回归测试补齐（单测/集成/回归）
  - Benchmark 基线记录
  - 版本发布说明流程规范化

## v1.0.1 (2026-03-09)

本次版本为 Milestone B（DDL 演进能力）发布，更新内容如下。

### 新增

- 支持 `ALTER TABLE ... ADD COLUMN`
- 支持 `ALTER TABLE ... DROP COLUMN`（受限规则）
- 支持 `ALTER TABLE ... ALTER COLUMN`
  - `TYPE` / `SET DATA TYPE`
  - `SET NOT NULL`
  - `DROP NOT NULL`
- 支持 `RENAME TABLE`
- 支持 `RENAME COLUMN`

### 元数据与一致性

- 重命名表/列时，自动同步关联元数据：
  - 索引列引用
  - 约束列引用
  - 外键引用目标（跨表）
- DDL 失败时保持 catalog 一致性，支持事务边界内回滚
- 兼容旧数据目录：可在重启后对旧目录执行 Milestone B 的 DDL 迁移

### 测试与验证

- 补充 parser、catalog、sql engine 的 Milestone B 覆盖测试
- 通过：
  - `cargo fmt --all`
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets -- -D warnings`
