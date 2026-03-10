# RuseDB 查询引擎说明

## 1. 总体流程

SQL 在 RuseDB 中的处理链路：

1. `lexer`：词法分析，生成 `Token`。
2. `parser`：语法分析，生成 AST `Statement`。
3. `Engine::execute_statement`：语句分发执行。
4. 存储访问：Catalog / Heap / Index。
5. 输出：`QueryResult`（rows/affected/message）。

---

## 2. SQL 解析层（rusedb-sql）

### 2.1 词法分析

支持关键 token：

- 标识符、数字、字符串
- 比较运算符（`= != < <= > >=`）
- 结构符号（`(),.*;`）
- 关键字：DDL、DML、事务、谓词、聚合、连接等

### 2.2 语法分析与 AST

核心语句：

- 数据库：`CREATE/DROP/USE/SHOW`
- 表与索引：`CREATE TABLE/INDEX`、`ALTER`、`RENAME`、`DROP TABLE`
- DML：`INSERT/SELECT/UPDATE/DELETE`
- 事务：`BEGIN/COMMIT/ROLLBACK`
- 计划：`EXPLAIN [ANALYZE]`
- 统计：`ANALYZE TABLE`

表达式支持：

- 逻辑与比较
- `IN` / `IN (SELECT ...)`
- 标量子查询
- `LIKE` / `BETWEEN`
- `IS NULL / IS NOT NULL`

---

## 3. 执行引擎分发（rusedb-exec）

按 `Statement` 分为三类：

1. 控制类：事务、数据库切换、show 命令。
2. DDL 类：建表、改表、重命名、建索引。
3. DML 类：增删改查。

写语句在无显式事务时会走自动事务流程。

---

## 4. SELECT 执行策略

RuseDB 对 `SELECT` 有两条主路径：

### 4.1 快速路径（单表简化场景）

当不涉及 join/group/having/复杂聚合时：

- 解析 projection/order/limit
- 尝试 `choose_index_candidates` 选择候选 RID
- 执行谓词过滤与结果构造

### 4.2 通用路径（复杂场景）

当存在 `JOIN/GROUP BY/HAVING/聚合/子查询` 时：

- 加载上下文行（可跨表）
- 执行连接（含 `LEFT JOIN`）
- 过滤 `WHERE`
- 分组与聚合
- 执行 `HAVING`
- 排序与裁剪 `LIMIT`

---

## 5. 索引与谓词下推

- 索引基于单列有序索引。
- 常见比较谓词可转为索引候选扫描。
- 若无法利用索引，回退全表扫描。
- `EXPLAIN` 会输出选择路径与相关统计指标。

---

## 6. 约束校验与写入执行

写入（INSERT/UPDATE/DELETE）过程中会进行：

- 主键/唯一约束冲突检测
- 外键引用与 `RESTRICT` 约束检查
- 索引维护（增删改映射）

对于 `ALTER` 操作，必要时会重写表数据并重建索引。

---

## 7. 可观测能力

- `EXPLAIN`：展示计划与路径信息。
- `EXPLAIN ANALYZE`：附带耗时/扫描行/输出行。
- 慢查询日志：记录慢 SQL 与结果指标。
- `ANALYZE TABLE`：刷新统计信息供优化路径选择。

---

## 8. 错误处理

错误统一走 `RuseDbError`，并输出稳定错误码前缀：

- 语法/参数错误：`RDB-PARSE-001`
- 对象已存在：`RDB-EXISTS-001`
- 对象不存在：`RDB-NOTFOUND-001`
- 类型/约束/页级错误：对应 `RDB-*` 代码

HTTP 场景下映射到 `400/401/403/404/405/500`。

---

## 9. 扩展方向

- 连接别名与更丰富 SQL 语义。
- 代价模型增强（多索引/联合条件更精细估算）。
- 并行执行与更细粒度算子管线化。
- 错误码结构化回传（HTTP `code` 字段标准化）。

