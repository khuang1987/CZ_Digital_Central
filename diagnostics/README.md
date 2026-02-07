# Diagnostics Toolkit

## 📋 Overview

This folder contains diagnostic tools to help troubleshoot deployment issues, especially SQL Server connectivity problems with PM2.

## 🔧 Available Tools

### 1. `check_sql_connection.ps1`
**Purpose**: Comprehensive SQL connection diagnostics

**What it checks**:
- ✓ .env file existence and DB variables
- ✓ ecosystem.config.js configuration
- ✓ Current PowerShell environment variables
- ✓ SQL Server TCP connectivity
- ✓ PM2 process status and environment
- ✓ Recent PM2 error logs

**How to use**:
```powershell
.\diagnostics\check_sql_connection.ps1
```

**Output**: Creates `diagnostics_output/sql_diagnostics_TIMESTAMP.txt`

---

### 2. `compare_pm2_env.ps1`
**Purpose**: Compare .env file with PM2 runtime environment

**What it checks**:
- ✓ Loads all variables from .env
- ✓ Gets PM2 process environment
- ✓ Compares each DB_* and MDDAP_* variable
- ✓ Reports mismatches and missing variables

**How to use**:
```powershell
.\diagnostics\compare_pm2_env.ps1
```

**Output**: Creates `diagnostics_output/pm2_env_comparison_TIMESTAMP.txt`

**Common Issues Detected**:
- Variables in .env but missing in PM2
- Values different between .env and PM2
- Empty values in PM2 when .env has values

---

### 3. `collect_all_logs.ps1`
**Purpose**: Collect all diagnostic information into a single package

**What it collects**:
- ✓ .env file
- ✓ ecosystem.config.js
- ✓ PM2 status and process list
- ✓ PM2 application logs
- ✓ System environment variables
- ✓ SQL Server instances
- ✓ Network configuration
- ✓ System information

**How to use**:
```powershell
.\diagnostics\collect_all_logs.ps1
```

**Output**: Creates `diagnostics_output/diagnostic_package_TIMESTAMP/` folder

**Use case**: When you need to share diagnostic information for remote troubleshooting.

---

## 🚀 Quick Start

### If SQL connection is failing:

1. **Run the connection check**:
   ```powershell
   .\diagnostics\check_sql_connection.ps1
   ```

2. **Check for environment mismatches**:
   ```powershell
   .\diagnostics\compare_pm2_env.ps1
   ```

3. **If issues found, restart PM2 with fresh environment**:
   ```powershell
   pm2 delete all
   pm2 start ecosystem.config.js --update-env
   ```

4. **If still failing, collect full diagnostics**:
   ```powershell
   .\diagnostics\collect_all_logs.ps1
   ```

---

## 📊 Common Issues and Solutions

### Issue 1: PM2 environment doesn't match .env
**Symptom**: `compare_pm2_env.ps1` shows mismatches

**Solution**:
```powershell
# Restart PM2 services to reload environment
pm2 delete all
pm2 start ecosystem.config.js --update-env
pm2 save
```

### Issue 2: SQL Server not listening on port 1433
**Symptom**: TCP connection test fails in `check_sql_connection.ps1`

**Solution**:
1. Check if SQL Server is running: `Get-Service MSSQL*`
2. Enable TCP/IP in SQL Server Configuration Manager
3. Restart SQL Server service

### Issue 3: Empty DB_USER/DB_PASSWORD in PM2
**Symptom**: PM2 environment shows empty values

**Solution**:
1. Verify .env file has correct values
2. Check ecosystem.config.js was generated correctly
3. Restart deployment: `.\deploy_to_local.bat`

---

## 📁 Output Location

All diagnostic outputs are saved to:
```
diagnostics_output/
├── sql_diagnostics_TIMESTAMP.txt
├── pm2_env_comparison_TIMESTAMP.txt
└── diagnostic_package_TIMESTAMP/
    ├── README.txt
    ├── .env
    ├── ecosystem.config.js
    ├── pm2_logs/
    └── ... (other files)
```

---

## 💡 Tips

- Run diagnostics **after deployment** but **before** the application starts working
- If sharing diagnostics, zip the `diagnostic_package_TIMESTAMP` folder
- Check timestamps to ensure you're looking at the latest output
- Compare diagnostics between working (dev) and non-working (server) environments

---

## 🆘 Need Help?

If diagnostics show issues you can't resolve:
1. Run `collect_all_logs.ps1`
2. Zip the output folder
3. Share with your development team
4. Include specific error messages from PM2 logs
