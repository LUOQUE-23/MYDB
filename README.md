# mydb

一个用 Rust 实现的个人数据库项目，目标是“可在本机或局域网可用、可管理、可持续迭代”。

当前版本已经支持：

- 存储层（页、堆文件、持久化）
- Catalog（表/列/索引元数据持久化）
- SQL 执行（`CREATE TABLE`、`CREATE INDEX`、`INSERT`、`SELECT`、`UPDATE`、`DELETE`）
- 条件过滤与范围检索（含索引加速）
- 排序与限制（`ORDER BY`、`LIMIT`）
- 事务语句（`BEGIN`、`COMMIT`、`ROLLBACK`）与 WAL 恢复基础能力
- Join 与聚合（`JOIN ... ON`、`GROUP BY`、`COUNT/SUM/MIN/MAX`）
- 命令行服务化体验（`init/start/status/connect/stop`）

## 1. 快速开始（新电脑用户）

下面是最推荐的使用流程（Windows）。

### 1.1 下载并准备可执行文件

1. 打开 GitHub Releases，下载 Windows 版本压缩包（例如 `mydb-windows-x86_64.zip`）。
2. 解压后确保你能看到 `mydb.exe`。
3. 可选：把 `mydb.exe` 所在目录加入系统 `PATH`，便于全局调用。

说明：
- 如果你暂时还没有发布 Release，也可以用“从源码构建”的方式获取 `mydb.exe`（见文末第 5 节）。

### 1.2 初始化实例

首次使用执行：

```powershell
mydb init mydb-instance
```

程序会交互提示你输入：

- Host（默认 `127.0.0.1`）
- Port（默认 `15432`）
- Catalog base path（数据库文件路径）
- Admin username（默认 `admin`）
- Admin password

初始化完成后会生成：

- `mydb-instance/mydb.conf`（实例配置）
- 数据目录（在你设置的 `catalog_base` 路径下）

### 1.3 启动服务

```powershell
mydb start mydb-instance
```

服务启动后会监听你配置的地址和端口。这个命令会前台运行。

### 1.4 连接并执行 SQL

新开一个终端：

```powershell
mydb connect mydb-instance
```

输入用户名和密码后进入交互模式，示例：

```sql
CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR);
INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
SELECT * FROM users ORDER BY id ASC;
```

退出连接：

```text
.exit
```

### 1.5 查看服务状态与停服

查看服务是否可达：

```powershell
mydb status mydb-instance
```

优雅停服（需要管理员认证）：

```powershell
mydb stop mydb-instance
```

## 2. 常用命令速查

```powershell
mydb --version
mydb help
mydb init [instance_dir]
mydb start [instance_dir]
mydb status [instance_dir]
mydb connect [instance_dir]
mydb stop [instance_dir]
mydb shell [catalog_base]
mydb sql <catalog_base> "<sql 或多条 SQL>"
```

`mydb sql` 支持批量 SQL（分号分隔），例如：

```powershell
mydb sql .\tmp\mydb "BEGIN; INSERT INTO t (id) VALUES (1); COMMIT; SELECT * FROM t;"
```

## 3. SQL 功能示例

### 3.1 索引

```sql
CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT NOT NULL, amount BIGINT);
CREATE INDEX idx_orders_user_id ON orders (user_id);
SELECT * FROM orders WHERE user_id = 1001;
```

### 3.2 事务

```sql
BEGIN;
UPDATE users SET name = 'alice-new' WHERE id = 1;
COMMIT;
```

### 3.3 Join + 聚合

```sql
SELECT users.id, COUNT(*), SUM(orders.amount), MIN(orders.amount), MAX(orders.amount)
FROM users
JOIN orders ON users.id = orders.user_id
GROUP BY users.id
ORDER BY users.id ASC;
```

## 4. 故障排查

### 4.1 端口被占用

初始化时换一个端口，或修改 `mydb.conf` 的 `port` 后重启服务。

### 4.2 `status` 显示不可达

按顺序检查：

1. 是否已执行 `mydb start <instance_dir>`。
2. `mydb.conf` 里的 `host/port` 与实际连接是否一致。
3. 本机防火墙是否拦截该端口。

### 4.3 忘记密码

当前版本主要面向个人开发和测试场景，建议：

1. 先备份实例目录和数据目录。
2. 重新创建新实例并迁移数据。

## 5. 从源码构建（开发者）

如果你希望自己构建二进制：

```powershell
cargo build --release -p mydb
```

构建产物在：

```text
target/release/mydb.exe
```

可执行完整验证：

```powershell
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## 6. 版本说明

本项目目前偏向“个人可用数据库”与“工程化演示”定位。  
生产级部署前，建议继续补齐更强的安全与运维能力（例如更强密码哈希、权限模型、备份恢复策略、安装器与自动升级）。
