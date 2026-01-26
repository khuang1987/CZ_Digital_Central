# 项目迁移指南 (Migration Guide)

本文档详细说明将 MDDAP 数据平台迁移至新设备或新环境时需要注意的事项和操作步骤。

## 1. 环境依赖 (Prerequisites)

在运行新环境前，请确保安装以下基础软件：

### 1.1 系统软件
*   **操作系统**: Windows 10/11 (项目深度依赖 Windows COM 组件和 PowerShell)
*   **Python**: 推荐 Python 3.10+ (需添加到系统 PATH)
*   **Microsoft Excel**: 必须安装 (用于 SAP GUI Scripting 和 Excel 自动化)
*   **ODBC Driver**: [Microsoft ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
*   **Microsoft Edge**: 用于爬虫自动化 (内置于 Windows)

### 1.2 Python 环境
建议在新环境下重新创建虚拟环境：

```powershell
# 删除旧的 .venv (如果拷贝过来的话)
# 创建新环境
python -m venv .venv

# 激活环境
.venv\Scripts\activate

# 安装依赖
pip install -r documentation\requirements.txt
# 或者
pip install pandas pyodbc openpyxl pyyaml playwright mkdocs
# 安装 Playwright 浏览器内核
playwright install msedge
```

## 2. 关键路径与配置 (Critical Paths)

本项目大量使用绝对路径或 OneDrive 路径，这是迁移中最容易出错的地方。

### 2.1 OneDrive 路径
*   **现状**: 代码中通过 `PROJECT_ROOT` 相对路径解决大部分内部引用，但部分数据源（如 SharePoint 同步文件夹）可能引用了 `C:\Users\huangk14\OneDrive - Medtronic PLC\...`。
*   **风险**: 新用户的用户名(Username)不同，或 OneDrive 同步文件夹名称不同。
*   **解决方案**:
    1.  检查 `data_pipelines/sources/*/config/*.yaml` 配置文件。
    2.  搜索代码中的硬编码路径（特别是 `cmes_downloader.py` 中的 `external path`）。
    3.  确保新机器上的 OneDrive 同步目录结构与原机器一致，或者修改配置文件中的路径。

### 2.2 数据库连接
*   **现状**: 默认连接字符串指向 `localhost\SQLEXPRESS`。
*   **迁移**:
    *   确保新机器安装了 SQL Server Express。
    *   或者修改环境变量 `MDDAP_SQL_SERVER` 指向正确的数据库实例。

## 3. 认证与权限 (Authentication)

### 3.1 账号登录 (Smart Auth)
*   **机制**: 爬虫脚本使用 Playwright 的 User Profile (`User Data - Playwright`) 来保存登录状态。
*   **迁移影响**: 迁移后，旧的 Cookie/Token 失效或不存在。
*   **操作**:
    *   首次运行脚本（如 `refresh_parallel.bat`）时，Smart Auth check 会检测到未登录。
    *   它会自动弹出浏览器窗口。
    *   **手动操作**: 在弹出的窗口中登录 Microsoft 账号（可能需要手机验证码）。
    *   登录成功后，脚本会自动继续，后续运行将自动静默执行。

### 3.2 SAP 权限
*   如果涉及 SAP 脚本 (GUI Scripting)，确保新机器上的 SAP 客户端已安装，并且当前用户有权限执行脚本（通常需要在 SAP 设置中启用 Scripting 支持）。

## 4. 常见问题 (Troubleshooting)

*   **ImportError: DLL load failed**: 通常是因为缺少 C++ Redistributable 或 ODBC 驱动。安装 SQL Server ODBC Driver 17 即可解决。
*   **Playwright Browser Closed**: 确保运行了 `playwright install msedge`。
*   **Path Not Found**: 检查 OneDrive 同步状态，确保文件已下载到本地（不仅仅是云端占位符）。

---
**建议**: 在新机器上初次运行时，使用 `python scripts/orchestration/run_data_collection.py all --no-headless` 显式开启有头模式进行一次完整测试。
