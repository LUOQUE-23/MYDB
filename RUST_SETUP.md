# Rust 环境配置（Windows / macOS / Linux）

本文件用于在新电脑上快速配置 Rust 开发环境，以便编译并运行 RuseDB（命令：`rusedb`）。

---

## Windows（PowerShell）

1. 安装 Rust（官方 rustup）

```powershell
winget install Rustlang.Rustup
```

2. 关闭并重新打开终端，然后检查版本

```powershell
rustc --version
cargo --version
```

3.（可选）确认 `cargo` 在 PATH 中

```powershell
where cargo
```

---

## macOS（bash / zsh）

1. 安装 Rust（官方 rustup）

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. 使环境生效

```bash
source ~/.cargo/env
```

3. 检查版本

```bash
rustc --version
cargo --version
```

4.（可选）确认 `cargo` 路径

```bash
which cargo
```

---

## Linux（bash / zsh）

1. 安装 Rust（官方 rustup）

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. 使环境生效

```bash
source ~/.cargo/env
```

3. 检查版本

```bash
rustc --version
cargo --version
```

4.（可选）确认 `cargo` 路径

```bash
which cargo
```

---

## 常见问题

### 1) 终端提示 `cargo` / `rustc` 找不到

macOS / Linux 重新加载环境：

```bash
source ~/.cargo/env
```

Windows 重新打开 PowerShell 或重新登录系统即可。

### 2) 需要升级 Rust

```bash
rustup update
```

---

## 验证构建（在本项目根目录）

```bash
cargo check --workspace
cargo test --workspace
```
