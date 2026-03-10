# RuseDB 存储引擎说明

## 1. 存储引擎职责

`rusedb-storage` 负责：

- 页管理与磁盘读写。
- 堆文件（HeapFile）记录存取。
- Catalog 元数据持久化。
- 有序索引（OrderedIndex）维护。

---

## 2. 页结构（Page）

RuseDB 使用固定页大小（`PAGE_SIZE`）组织数据，页内采用“槽目录 + 变长记录”布局。

页头关键字段：

- `page_id`（4 bytes）
- `lsn`（8 bytes）
- `free_space_offset`（2 bytes）
- `slot_count`（2 bytes）

记录写入流程：

1. 优先复用已删除槽位。
2. 空间不足时执行页内 compact。
3. 仍不足则返回 `PageFull`。

记录删除采用逻辑删除（槽位长度置 0），后续可复用。

---

## 3. HeapFile 组织

`HeapFile` 基于 `DiskManager + Page` 实现：

- `insert_record`：遍历已有页寻找空间，否则分配新页。
- `get_record`：按 `(page_id, slot_id)` 随机访问。
- `delete_record`：删除指定 RID。
- `update_record`：当前实现为 delete + insert，可能导致 RID 变化。
- `scan_records`：全表扫描所有有效记录。

---

## 4. Catalog 元数据存储

Catalog 使用多组堆文件保存系统元数据：

- `<base>.tables`
- `<base>.columns`
- `<base>.indexes`
- `<base>.constraints`

内存中维护映射：

- `table_name -> table_id`
- `table_id -> columns/indexes/constraints`

支持能力：

- 建表/删表
- 增删列、重命名表列
- 主键/唯一/外键信息持久化与重启恢复

---

## 5. 有序索引（OrderedIndex）

索引文件后缀：`<base>.index-<index_id>.idx`  
结构：B+Tree 风格，支持叶子链表顺序扫描。

键类型支持：

- `Int`（覆盖 `INT/BIGINT`）
- `Str`（`VARCHAR`）
- `Bool`（`BOOL`）

当前不支持：

- `DOUBLE` 作为索引键

核心操作：

- `insert`
- `remove`
- `search_eq`
- `search_range`

---

## 6. 数据文件命名规范

针对一个数据库前缀 `<base>`：

- 元数据：`<base>.tables/.columns/.indexes/.constraints`
- 表数据：`<base>.table-<id>.heap`
- 索引数据：`<base>.index-<id>.idx`
- WAL：`<base>.wal`
- 统计：`<base>.stats`
- 慢日志：`<base>.slowlog`
- 迁移状态：`<base>.migrations`
- RBAC：`<base>.rbac`
- 在线备份锁：`<base>.backup.lock`

多库模式下使用 `<prefix>-db-<name>` 形成同构文件家族。

---

## 7. 备份与恢复（存储层视角）

### 7.1 备份

- `offline`：直接复制文件家族。
- `online`：创建备份锁并等待活跃事务结束，再复制文件家族。

备份目录包含 `backup.manifest`，记录：

- `version`
- `mode`
- `source_prefix`
- `source_base`
- `created_unix_ms`
- `files`

### 7.2 恢复

`restore` 根据 manifest 进行前缀映射并覆盖目标文件家族。

### 7.3 PITR（阶段一）

`pitr` 在备份根目录中选择 `created_unix_ms <= target_unix_ms` 的最新快照，再执行恢复。

---

## 8. 一致性与可靠性说明

- 存储层保证页结构校验，遇到布局异常返回 `Corruption`。
- WAL 回放由执行层控制，存储层提供文件与页级读写基础能力。
- 备份锁与事务等待协同，避免在线备份窗口中出现新的写入破坏快照一致性。

