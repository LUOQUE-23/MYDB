# RuseDB Single-Node Ops Runbook (v1.2.1)

## Target

This runbook targets daily development and small-scale testing on a single machine.

## 1. Start Service

```powershell
rusedb http .\tmp\rusedb 127.0.0.1:18080 --token my-secret --allow-origin http://localhost:3000
```

## 2. Health & Admin Checks

```bash
curl http://127.0.0.1:18080/health
curl -H "Authorization: Bearer my-secret" http://127.0.0.1:18080/admin/databases
curl -H "Authorization: Bearer my-secret" "http://127.0.0.1:18080/admin/stats?db=default"
curl -H "Authorization: Bearer my-secret" "http://127.0.0.1:18080/admin/slowlog?db=default&limit=200"
```

## 3. Backup Strategy

- Daily online snapshot:

```powershell
rusedb backup .\tmp\rusedb .\backups\daily-20260310 --online
```

- Restore full snapshot:

```powershell
rusedb restore .\backups\daily-20260310 .\tmp\rusedb-restore
```

- PITR phase 1 (snapshot selection):

```powershell
rusedb pitr .\backups .\tmp\rusedb-pitr 1760000000000
```

## 4. Schema Change Control

```powershell
rusedb migrate up .\tmp\rusedb .\migrations
rusedb migrate down .\tmp\rusedb .\migrations 1
```

## 5. Security Baseline

- Enable HTTP bearer token (`--token`).
- Configure RBAC users and grants:

```powershell
rusedb user-add .\tmp\rusedb app_user app_token
rusedb grant .\tmp\rusedb app_user table:default.users select,insert,update
```

## 6. Failure Handling

- WAL recovery runs automatically on next engine startup.
- Use `/admin/slowlog` and `/admin/stats` to inspect hot spots and table stats.
- Keep recent backup snapshots and verify restore regularly.

