# RuseDB 使用说明书（从下载到接入）

本文面向第一次使用 RuseDB 的开发者，覆盖：

- 如何下载与安装
- 如何初始化与启动
- 如何执行 SQL
- 如何通过 HTTP API 接入
- 如何在主流语言中访问数据库

当前版本：`v1.2.1`

---

## 1. 环境准备

### 1.1 必要环境

- Rust 工具链（建议稳定版）
- 可用终端（PowerShell / bash / zsh）

### 1.2 获取源码

```bash
git clone https://github.com/LUOQUE-23/RuseDB.git
cd RuseDB
```

---

## 2. 安装与版本验证

### 2.1 通过 Cargo 安装命令

```bash
cargo install --path crates/rusedb-server --force
```

安装后验证：

```bash
rusedb --version
rusedb help
```

---

## 3. 初始化与本地使用

## 3.1 初始化实例（交互式）

```bash
rusedb init rusedb-instance
```

会提示输入：

- Host
- Port
- Catalog base path
- Admin username
- Admin password

## 3.2 启动/状态/停止

```bash
rusedb start rusedb-instance
rusedb status rusedb-instance
rusedb stop rusedb-instance
```

## 3.3 TCP 连接执行

```bash
rusedb connect rusedb-instance
```

---

## 4. 直接执行 SQL（无需 TCP 服务）

### 4.1 交互式 SQL Shell

```bash
rusedb shell ./tmp/rusedb
```

### 4.2 一次性 SQL 批执行

```bash
rusedb sql ./tmp/rusedb "CREATE DATABASE app; USE app; CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR); INSERT INTO users (id, name) VALUES (1, 'alice'); SELECT * FROM users;"
```

---

## 5. HTTP API 启动与鉴权

### 5.1 启动 HTTP 服务

```bash
rusedb http ./tmp/rusedb 127.0.0.1:18080 --token my-secret --allow-origin http://localhost:3000
```

### 5.2 基础路由

- `GET /health`
- `POST /sql`
- `GET /admin/databases`
- `GET /admin/stats?db=default`
- `GET /admin/slowlog?db=default&limit=200`
- `GET /openapi.json`
- `GET /docs`

### 5.3 鉴权规则

1. Bearer Token（可选开启）  
   请求头：`Authorization: Bearer my-secret`

2. RBAC 用户（当已创建 RBAC 用户时必填）  
   请求头：
   - `X-RuseDB-User: <username>`
   - `X-RuseDB-Token: <token>`

---

## 6. RBAC 快速配置

```bash
rusedb user-add ./tmp/rusedb app_user app_token
rusedb grant ./tmp/rusedb app_user table:default.users select,insert,update,delete
rusedb user-list ./tmp/rusedb
```

常用 scope 示例：

- `db:default`
- `table:default.users`
- `table:default.*`
- `db:*`

---

## 7. 备份、恢复、PITR

### 7.1 备份

```bash
rusedb backup ./tmp/rusedb ./backups/snap-001
rusedb backup ./tmp/rusedb ./backups/snap-002 --online
```

### 7.2 恢复

```bash
rusedb restore ./backups/snap-001 ./tmp/rusedb-restore
```

### 7.3 PITR（阶段一）

```bash
rusedb pitr ./backups ./tmp/rusedb-pitr 1760000000000
```

含义：从 `./backups` 中选取时间戳不晚于目标时间点的最新快照并恢复。

---

## 8. 迁移管理

```bash
rusedb migrate up ./tmp/rusedb ./migrations
rusedb migrate down ./tmp/rusedb ./migrations 1
```

文件命名规范：

- `001_init.up.sql`
- `001_init.down.sql`

---

## 9. HTTP 调用示例（curl）

```bash
curl -X POST http://127.0.0.1:18080/sql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret" \
  -H "X-RuseDB-User: app_user" \
  -H "X-RuseDB-Token: app_token" \
  -d "{\"sql\":\"SHOW DATABASES;\"}"
```

---

## 10. 主流语言接入示例

说明：RuseDB 不提供 MySQL/PostgreSQL 线协议，推荐统一走 HTTP API。

### 10.1 Python（requests）

```python
import requests

url = "http://127.0.0.1:18080/sql"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer my-secret",
    "X-RuseDB-User": "app_user",
    "X-RuseDB-Token": "app_token",
}
payload = {"sql": "USE app; SELECT * FROM users;"}

resp = requests.post(url, headers=headers, json=payload, timeout=10)
resp.raise_for_status()
print(resp.json())
```

### 10.2 Node.js（fetch）

```javascript
const url = "http://127.0.0.1:18080/sql";

const headers = {
  "Content-Type": "application/json",
  "Authorization": "Bearer my-secret",
  "X-RuseDB-User": "app_user",
  "X-RuseDB-Token": "app_token",
};

const body = JSON.stringify({
  sql: "USE app; SELECT id, name FROM users ORDER BY id DESC LIMIT 10;",
});

fetch(url, { method: "POST", headers, body })
  .then((r) => r.json())
  .then(console.log)
  .catch(console.error);
```

### 10.3 Java（JDK 11 HttpClient）

```java
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RuseDbDemo {
    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String json = "{\"sql\":\"SHOW DATABASES;\"}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:18080/sql"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer my-secret")
                .header("X-RuseDB-User", "app_user")
                .header("X-RuseDB-Token", "app_token")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response.statusCode());
        System.out.println(response.body());
    }
}
```

### 10.4 Go（net/http）

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

func main() {
	body := []byte(`{"sql":"SHOW DATABASES;"}`)
	req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/sql", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer my-secret")
	req.Header.Set("X-RuseDB-User", "app_user")
	req.Header.Set("X-RuseDB-Token", "app_token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	fmt.Println(resp.StatusCode)
	fmt.Println(string(data))
}
```

---

## 11. 常见问题

### 11.1 为什么不能直接用 MySQL/JDBC 驱动连接？

RuseDB 当前使用自定义 SQL 解析与接口协议，不实现 MySQL/PostgreSQL 线协议。

### 11.2 建议怎么接入生产代码？

建议在应用内封装统一的 HTTP 数据访问层，处理重试、超时、错误码映射和认证头注入。

### 11.3 如何查慢 SQL？

使用：

- `GET /admin/slowlog`
- `GET /admin/stats`
- `EXPLAIN ANALYZE <query>`
