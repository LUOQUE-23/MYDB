# RuseDB SQL Support Matrix (v1.2.1)

## DDL

| Category | Statement | Status |
|---|---|---|
| Database | `CREATE DATABASE` | Supported |
| Database | `DROP DATABASE` | Supported |
| Database | `USE` | Supported |
| Database | `SHOW DATABASES` | Supported |
| Database | `SHOW CURRENT DATABASE` | Supported |
| Table | `CREATE TABLE` | Supported |
| Table | `DROP TABLE` | Supported |
| Table | `ALTER TABLE ... ADD COLUMN` | Supported |
| Table | `ALTER TABLE ... DROP COLUMN` | Supported (restricted) |
| Table | `ALTER TABLE ... ALTER COLUMN` | Supported (restricted) |
| Table | `RENAME TABLE` | Supported |
| Table | `RENAME COLUMN` | Supported |
| Index | `CREATE INDEX` | Supported |

## Constraints

| Constraint | Status | Notes |
|---|---|---|
| `PRIMARY KEY` | Supported | Column-level / table-level |
| `UNIQUE` | Supported | Column-level / table-level / multi-column |
| `FOREIGN KEY ... REFERENCES ...` | Supported | `RESTRICT` semantics |

## DML

| Statement | Status |
|---|---|
| `INSERT` | Supported |
| `SELECT` | Supported |
| `UPDATE` | Supported |
| `DELETE` | Supported |

## Query Features

| Feature | Status |
|---|---|
| `WHERE` | Supported |
| `ORDER BY` | Supported |
| `LIMIT` | Supported |
| `JOIN` | Supported |
| `LEFT JOIN` | Supported |
| `GROUP BY` | Supported |
| `HAVING` | Supported |
| `IN` | Supported |
| `IN (SELECT ...)` | Supported |
| Scalar subquery | Supported |
| `LIKE` | Supported |
| `BETWEEN` | Supported |
| `IS NULL / IS NOT NULL` | Supported |
| Aggregates `COUNT/SUM/MIN/MAX` | Supported |
| `EXPLAIN` | Supported |
| `EXPLAIN ANALYZE` | Supported |

## Transaction & Recovery

| Capability | Status |
|---|---|
| `BEGIN / COMMIT / ROLLBACK` | Supported |
| Isolation level `READ COMMITTED` | Supported |
| Lock wait timeout | Supported |
| WAL recovery + checkpoint | Supported |

## Operations

| Capability | Status | Notes |
|---|---|---|
| Backup/restore | Supported | `backup` / `restore` |
| Online backup | Supported | `backup --online` with backup lock coordination |
| PITR (phase 1) | Supported | `pitr` selects latest snapshot before target timestamp |
| Migration tool | Supported | `migrate up/down` |
| RBAC | Supported | Database-level and table-level grants |

