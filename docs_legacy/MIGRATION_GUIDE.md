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
74: **建议**: 在新机器上初次运行时，使用 `python scripts/orchestration/run_data_collection.py all --no-headless` 显式开启有头模式进行一次完整测试。

## 5. 离线/内网环境迁移指南 (Offline/Intranet Migration)

如果目标机器无法连接外网（无 Internet，但可能有公司内网），请严格按照以下步骤操作。

### 5.1 准备工作 (在有网的机器上)

#### 📋 离线安装包下载清单 (Installer Checklist)
请务必提前下载以下软件的**离线安装包**（通常是 .exe 或 .msi）：

1.  **Python 3.10+**:  
    *下载 Windows installer (64-bit)。推荐 3.10 或 3.11 版本。*
2.  **SQL Server Express (2019 或 2022)**:  
    *请下载 "Media" 版本以便获得完整的离线安装文件，而不是几MB的在线安装程序。*
3.  **SSMS (SQL Server Management Studio)**:  
    *用于管理数据库和恢复备份文件。*
4.  **ODBC Driver 17 for SQL Server**:  
    *文件名通常为 `msodbcsql.msi`。这是 Python 连接数据库的必需驱动。*
5.  **Visual Studio Code (可选)**:  
    *推荐下载 System Installer (64-bit)，方便在内网查看代码和配置文件。*

#### 📦 项目资源打包
1.  **打包 Python 依赖**:
    在当前机器上运行以下命令，将所有依赖下载到 `offline_packages` 文件夹：
    ```powershell
    mkdir offline_packages
    pip download -d offline_packages -r requirements.txt
    ```

2.  **打包 Playwright 浏览器**:
    Playwright 默认即时下载浏览器，离线环境需手动拷贝。
    *   找到浏览器缓存目录: `%USERPROFILE%\AppData\Local\ms-playwright`
    *   将整个 `ms-playwright` 文件夹打包复制。

3.  **打包数据库**:
    *   备份数据库: `Run "BACKUP DATABASE [mddap_v2] TO DISK = 'C:\backup\mddap_v2.bak'"`
    *   拷贝 `.bak` 文件。

### 5.2 部署 (在内网/目标机器上)

1.  **安装 Python**: 使用离线安装包安装 Python 3.10+。
2.  **安装依赖**:
    将项目文件夹和 `offline_packages` 拷贝过去，运行：
    ```powershell
    cd 项目根目录
    python -m venv .venv
    .venv\Scripts\activate
    pip install --no-index --find-links=offline_packages -r requirements.txt
    ```
3.  **恢复 Playwright 浏览器**:
    *   创建目录 `%USERPROFILE%\AppData\Local\ms-playwright`
    *   将打包的浏览器文件解压进去。
4.  **SQL Server**:
    *   安装 SQL Server (离线包)。
    *   安装 ODBC Driver 17 (离线包 `msodbcsql.msi`)。
    *   恢复数据库 `.bak` 文件。

### 5.4 高级：远程数据库访问 (Remote Database Access)

如果您希望**内网电脑运行代码**，但**数据保存在原来的电脑 (Server)** 上，请按以下步骤操作。

#### A. 服务端设置 (在原电脑/Server上)

1.  **开启 TCP/IP**:
    *   打开 "SQL Server Configuration Manager"。
    *   展开 "SQL Server Network Configuration" -> "Protocols for SQLEXPRESS"。
    *   将 **TCP/IP** 状态改为 `Enabled`。
    *   (重要) 右键 TCP/IP -> 属性 -> IP Addresses 标签页 -> 滑到底部 "IPAll" -> 确保 **TCP Port** 填有 `1433` (或其他固定端口)。
    *   重启 "SQL Server (SQLEXPRESS)" 服务。

2.  **开启混合验证模式**:
    *   打开 SSMS，右键服务器实例 -> Properties -> Security。
    *   选择 **"SQL Server and Windows Authentication mode"**。
    *   点击 OK 并重启 SQL Server 服务。

3.  **创建数据库账户**:
    *   在 SSMS 中，展开 Security -> Logins。
    *   右键 -> New Login...
    *   Login name: `mddap_user` (举例)
    *   Select "SQL Server authentication"，设置密码。
    *   User Mapping 标签页: 勾选 `mddap_v2` 数据库，并勾选 `db_owner` 或 `db_datareader` + `db_datawriter`。

4.  **配置防火墙**:
    *   在 Windows 防火墙中，新建 **Inbound Rule** (入站规则)。
    *   类型: Port -> TCP -> Specific local ports: `1433` (与第1步设置一致)。
    *   Action: Allow the connection。
    *   命名为 "SQL Server Remote Access"。

#### B. 客户端设置 (在内网/目标电脑上)

1.  **修改 .env 文件**:
    打开项目根目录下的 `.env` 文件，修改以下配置：
    ```ini
    # 设置为原电脑的 IP 地址 (例如 192.168.1.5)
    MDDAP_SQL_SERVER=192.168.1.5,1433
    
    # 填写刚才创建的账户和密码
    MDDAP_SQL_USER=mddap_user
    MDDAP_SQL_PASSWORD=your_password
    ```

2.  **验证连接**:
    运行任意一个简单脚本 (如 `run_data_collection.py`)，查看日志是否报错。如果提示连接超时，请检查两台电脑是否 ping 得到。

第一步：去客户机（被控端）开通 SSH 服务 请在客户机上用 管理员 PowerShell 运行这三行命令（已放在文档里，复制过去直接跑）：
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
Start-Service sshd
Set-Service -Name sshd -StartupType 'Automatic'