# RuseDB Error Codes (v1.2.1)

This document defines stable error code categories for API/CLI troubleshooting.

## Core Categories

| Code | Category | Typical Source |
|---|---|---|
| `RDB-IO-001` | I/O failure | file read/write/open failures |
| `RDB-CORRUPTION-001` | Data corruption | invalid page/wal/stats format |
| `RDB-SCHEMA-001` | Invalid schema | incompatible column/index/constraint metadata |
| `RDB-PARSE-001` | Parse / syntax failure | invalid SQL or invalid command args |
| `RDB-TYPE-001` | Type mismatch | value cannot fit target column type |
| `RDB-NULL-001` | NULL constraint violation | writing `NULL` to `NOT NULL` column |
| `RDB-EXISTS-001` | Already exists | duplicate table/index/database/user |
| `RDB-NOTFOUND-001` | Not found | missing table/index/database/backup |
| `RDB-RECORD-001` | Record too large | encoded row exceeds page constraints |
| `RDB-PAGE-001` | Page full / range / RID | storage page boundary errors |

## SQL/Constraint Examples

| Scenario | Suggested Code |
|---|---|
| duplicate primary key | `RDB-CONSTRAINT-PK-001` |
| duplicate unique key | `RDB-CONSTRAINT-UQ-001` |
| foreign key restrict violation | `RDB-CONSTRAINT-FK-001` |
| lock wait timeout | `RDB-TX-LOCK-001` |
| backup lock wait timeout | `RDB-OPS-BACKUP-001` |

## HTTP Status Mapping

| HTTP Status | Meaning |
|---|---|
| `400` | bad request / parse / validation failure |
| `401` | authentication failure |
| `403` | authorization denied (RBAC) |
| `404` | route not found |
| `405` | method not allowed |
| `500` | internal runtime failure |

## Notes

- Current implementation primarily returns human-readable errors.
- These codes are the normalization contract for client integration and observability.
- Next phase can include embedding `code` fields in every HTTP error response.

