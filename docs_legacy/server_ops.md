# 服务端操作手册 (Server Operations Manual)

本手册详细介绍了如何管理和服务端的日常运维操作，主要包含进程管理、环境变量配置以及故障排查。

---

## 1. 进程管理 (PM2)

我们使用 [PM2](https://pm2.keymetrics.io/) 来管理 Web Dashboard 和 Backend API 进程。

### 常用命令
| 功能 | 命令 | 说明 |
| :--- | :--- | :--- |
| **查看状态** | `pm2 list` | 查看所有进程的运行状态、ID 和 CPU/内存占用 |
| **停止服务** | `pm2 stop <ID/Name>` | 停止指定服务（例如 `pm2 stop cz_dashboard`） |
| **重启服务** | `pm2 restart <ID/Name>` | 重启指定服务以应用配置更改 |
| **停止所有** | `pm2 stop all` | 停止所有受管进程 |
| **查看日志** | `pm2 logs <ID/Name>` | 查看实时输出日志（排查启动失败时非常有用） |
| **查看详情** | `pm2 show <ID/Name>` | 查看服务的详细环境路径和配置信息 |

### 进程列表
- `cz_dashboard`: Web 前端服务 (Next.js)
- `cz_backend`: Python API 后端服务 (FastAPI)

---

## 2. 环境变量配置 (.env)

项目使用单一的 `.env` 模板文件，在部署时会自动通过 `deploy_to_local.ps1` 脚本生成。

### 如何手动更改配置：
1. 打开 `C:\Apps\CZ_Digital_Central_Server\.env`。
2. 修改 SQL 服务器地址或数据库名：
   ```env
   DB_SERVER=你的服务器名\SQLEXPRESS
   DB_NAME=mddap_v2
   ```
3. **重要**：修改完成后，必须重启服务才能生效：
   ```powershell
   pm2 restart all
   ```

---

## 3. 诊断与排障工具

如果页面无法显示数据或服务无法启动，请运行以下程序：

### SQL 连接诊断
检查 .env 配置是否正确，以及是否能连通数据库。
```powershell
# 路径: diagnostics/check_sql_connection.ps1
cd C:\Apps\CZ_Digital_Central_Server
.\diagnostics\check_sql_connection.ps1
```

### 环境健康检查
检查 Node.js, Python, PM2 等基础环境是否正常。
```powershell
# 路径: check_server_env.ps1
cd C:\Apps\CZ_Digital_Central_Server
.\check_server_env.ps1
```

### 日志收集
一键收集所有服务的最近日志。
```powershell
# 路径: diagnostics/collect_all_logs.ps1
cd C:\Apps\CZ_Digital_Central_Server
.\diagnostics\collect_all_logs.ps1
```

---

## 4. 常见问题 (FAQ)

- **问：Dashboard 显示 502 Bad Gateway？**
  - 答：通常是 `cz_dashboard` 进程挂了，请尝试 `pm2 restart cz_dashboard`。
- **问：SQL 连接报错 "Invalid object name '...'"？**
  - 答：请检查 `.env` 中的 `DB_NAME` 是否连接到了正确的数据库。
- **问：后端 API (8000端口) 无法启动？**
  - 答：请确认 Python 虚拟环境 `.venv` 是否完整，并运行 `pm2 logs cz_backend` 查看报错详情。
