# RuseDB 事务机制说明

## 1. 事务目标

RuseDB 在单机场景下提供：

- 显式事务：`BEGIN / COMMIT / ROLLBACK`
- 自动事务：对单条写语句自动包裹事务
- 崩溃恢复：基于 WAL 的提交恢复
- 并发语义：`READ COMMITTED`

---

## 2. 事务模型

执行引擎内部维护 `EngineState`：

- `tx`: 当前活跃事务上下文（可选）
- `tx_starting`: 事务启动中的过渡状态
- `current_db`: 当前数据库
- `next_tx_id`: 事务 ID

事务上下文 `TransactionContext`：

- `tx_id`
- `live_base`（正式库文件前缀）
- `working_base`（事务工作副本前缀）
- `owner_thread`

---

## 3. 隔离级别与并发控制

隔离级别：`READ COMMITTED`。

并发策略（简化实现）：

- 同时仅允许一个写事务进入（全局事务锁 + 条件变量）。
- 非事务拥有者线程可读取 live 数据，不可读到未提交写入。
- 写入竞争超时返回锁等待错误（默认 3 秒，可配置）。

---

## 4. 自动事务行为

若用户未显式 `BEGIN`，对以下写语句自动执行：

1. `begin_transaction_internal`
2. 执行语句
3. 成功则 `commit_transaction_internal`
4. 失败则 `rollback_transaction_internal`

这保证“单条写语句”具备原子性。

---

## 5. WAL 设计与恢复

### 5.1 WAL 文件

- 路径：`<base>.wal`
- 版本头：`WAL 1`
- 事件：
  - `BEGIN <tx_id>`
  - `COMMIT_START <tx_id>`
  - `COMMIT_DONE <tx_id>`
  - `CHECKPOINT <tx_id>`

### 5.2 提交流程（核心）

1. 记录 `COMMIT_START`
2. 将事务工作文件应用到 live
3. 记录 `COMMIT_DONE`
4. 清理事务工作文件
5. 写入 `CHECKPOINT`

### 5.3 崩溃恢复

引擎启动后（首次访问）会遍历数据库列表执行 WAL 恢复：

- 若检测到 `COMMIT_START` 但缺失 `COMMIT_DONE`，会继续提交应用。
- 完成后刷新 checkpoint 并清理 WAL 状态。

---

## 6. 事务与多库

- 支持 `CREATE/DROP/USE DATABASE`。
- 事务进行中限制数据库切换与当前库删除，避免上下文漂移导致一致性问题。

---

## 7. 事务与在线备份协同

在线备份使用 `<base>.backup.lock`：

- 写语句在执行前检查并等待备份锁释放。
- 在线备份创建锁后，会等待活跃事务结束再做快照复制。

这样可保证备份窗口内快照的一致性。

---

## 8. 错误与可观测

事务相关常见错误：

- `lock wait timeout ...`
- `backup lock wait timeout ...`
- `no active transaction`
- `transaction already active`

错误输出带稳定错误码前缀（如 `RDB-PARSE-001`），便于上层系统归类处理。

