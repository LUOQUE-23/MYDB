# RuseDB Roadmap (V1.0.1)

This roadmap is derived from `prompt.docx` and tracks what is already implemented.

## Milestone 0: Workspace baseline

- [X] Create Rust workspace and split crates (`core/storage/sql/exec/server/tests`)
- [X] Unified core error/result definitions
- [X] CLI starts and supports `rusedb --version`
- [X] `cargo fmt --all`
- [X] `cargo clippy --workspace --all-targets -- -D warnings`
- [X] `cargo test --workspace`

## Milestone 1: Minimal storage layer

- [X] Fixed-size page (`4096 bytes`) with slotted-page layout
- [X] Page header: `page_id`, `lsn`, `free_space_offset`, `slot_count`
- [X] Variable-length records in slot directory
- [X] `DiskManager` random page read/write
- [X] Simplified `BufferPool` wrapper
- [X] `HeapFile` insert/get/delete/update
- [X] Persistence test: restart and read back records
- [X] Slot reuse test after delete

## Milestone 2+

- [X] Schema/Row encoding
- [X] Catalog persistence (`tables`, `columns`, `indexes`)
- [X] `CREATE TABLE` / `DROP TABLE` (via CLI command)
- [X] `SHOW TABLES` / `DESCRIBE` (via CLI command)
- [X] SQL parser (`CREATE/INSERT/SELECT/DELETE/UPDATE` subset + AST + error position)
- [X] Minimal planner/executor (`CREATE/INSERT/SELECT/DELETE/UPDATE` SQL execution path)
- [X] `CREATE INDEX` + index-aware filtering (`=, <, <=, >, >=`)
- [X] Page-based B+Tree-style secondary index (leaf/internal pages, split, range scan, persistence)
- [X] B+Tree merge/rebalance and full maintenance strategy
- [X] WAL + transaction (`BEGIN/COMMIT/ROLLBACK`, commit-recovery)
- [X] Join/Aggregation/Sort (`JOIN ... ON`, `GROUP BY`, `COUNT/SUM/MIN/MAX`, `ORDER BY`, `LIMIT`)
- [X] Interactive shell / protocol (minimal REPL via `rusedb shell`)
- [X] Instance bootstrap and remote CLI flow (`rusedb init/start/connect` with host/port/admin password config)
- [X] Service operations UX (`rusedb status` + authenticated `rusedb stop` graceful shutdown)
