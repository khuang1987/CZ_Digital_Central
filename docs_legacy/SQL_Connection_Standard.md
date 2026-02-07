# SQL Server 连接标准指南 (Standard Connection Guide)

本文档定义了项目中 SQL Server 的标准连接配置和故障排除方法，以避免重复查找连接参数。

## 1. 环境变量配置 (.env)

项目使用双重配置模式，支持本地开发和看板服务。

### A. 后端数据处理配置 (ODC/Python 脚本)
使用 Windows 身份验证或全路径连接。
```env
MDDAP_SQL_SERVER=localhost\SQLEXPRESS
MDDAP_SQL_DATABASE=mddap_v2
```

### B. 看板服务配置 (Next.js API)
使用 SQL Server 身份验证 (推荐，更稳定)。
```env
DB_SERVER=127.0.0.1
DB_PORT=1433
DB_NAME=mddap_v2
DB_USER=mddap_user
DB_PASSWORD=1111
DB_TRUST_SERVER_CERTIFICATE=true
```

---

## 2. 命令行连接 (sqlcmd)

在开发过程中，使用以下命令快速验证连接：

### 使用 Windows 身份验证 (本地登录用户)
```powershell
sqlcmd -S ".\SQLEXPRESS" -E -d "mddap_v2"
```

### 使用 SQL 身份验证 (API 用户)
```powershell
sqlcmd -S "127.0.0.1" -U "mddap_user" -P "1111" -d "mddap_v2"
```

> [!IMPORTANT]
> - 如果连接失败，请检查 SQL Server 配置管理器中的 **TCP/IP** 是否已启用。
> - 确保 **SQL Server Browser** 服务正在运行。

---

## 3. 常见数据库名称

- **CZ_Digital_Central**: 核心业务数据库（当前活跃）。
- **mddap_v2**: 结构化数据 V2 数据库。
- **WorkshopDB**: 培训/工作坊数据库。

---

## 4. 连接代码示例 (Node.js)

```typescript
// 参考 src/lib/db.ts
import sql from 'mssql';

const config = {
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    options: {
        encrypt: false,
        trustServerCertificate: true
    }
};
```
