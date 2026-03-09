# Changelog

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
