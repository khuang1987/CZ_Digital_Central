# Collect All Diagnostic Logs
# Gathers all relevant logs and configuration files for troubleshooting

$ErrorActionPreference = "Continue"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Diagnostic Log Collector" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Create diagnostics output directory
$diagDir = Join-Path (Get-Location) "diagnostics_output"
if (-not (Test-Path $diagDir)) {
    New-Item -ItemType Directory -Path $diagDir -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$packageDir = Join-Path $diagDir "diagnostic_package_$timestamp"
New-Item -ItemType Directory -Path $packageDir -Force | Out-Null

Write-Host "`nCollecting diagnostic information..." -ForegroundColor Yellow

# 1. Copy .env file
Write-Host "  [1/8] Copying .env file..."
$envPath = Join-Path (Get-Location) ".env"
if (Test-Path $envPath) {
    Copy-Item $envPath -Destination (Join-Path $packageDir ".env") -Force
    Write-Host "    ✓ .env copied" -ForegroundColor Green
}
else {
    "✗ .env file not found" | Out-File (Join-Path $packageDir "MISSING_.env.txt")
    Write-Host "    ✗ .env not found" -ForegroundColor Red
}

# 2. Copy ecosystem.config.js
Write-Host "  [2/8] Copying ecosystem.config.js..."
$ecosystemPath = Join-Path (Get-Location) "ecosystem.config.js"
if (Test-Path $ecosystemPath) {
    Copy-Item $ecosystemPath -Destination (Join-Path $packageDir "ecosystem.config.js") -Force
    Write-Host "    ✓ ecosystem.config.js copied" -ForegroundColor Green
}
else {
    "✗ ecosystem.config.js not found" | Out-File (Join-Path $packageDir "MISSING_ecosystem.txt")
    Write-Host "    ✗ ecosystem.config.js not found" -ForegroundColor Red
}

# 3. PM2 status
Write-Host "  [3/8] Collecting PM2 status..."
try {
    pm2 status | Out-File (Join-Path $packageDir "pm2_status.txt") -Encoding UTF8
    pm2 jlist | Out-File (Join-Path $packageDir "pm2_jlist.json") -Encoding UTF8
    Write-Host "    ✓ PM2 status collected" -ForegroundColor Green
}
catch {
    "✗ Failed to get PM2 status: $($_.Exception.Message)" | Out-File (Join-Path $packageDir "pm2_error.txt")
    Write-Host "    ✗ PM2 status failed" -ForegroundColor Red
}

# 4. PM2 logs
Write-Host "  [4/8] Collecting PM2 logs..."
$pm2LogDir = "$env:USERPROFILE\.pm2\logs"
if (Test-Path $pm2LogDir) {
    $logPackageDir = Join-Path $packageDir "pm2_logs"
    New-Item -ItemType Directory -Path $logPackageDir -Force | Out-Null
    
    Get-ChildItem $pm2LogDir -Filter "cz-*" | ForEach-Object {
        Copy-Item $_.FullName -Destination $logPackageDir -Force
    }
    Write-Host "    ✓ PM2 logs collected" -ForegroundColor Green
}
else {
    "✗ PM2 log directory not found" | Out-File (Join-Path $packageDir "pm2_logs_missing.txt")
    Write-Host "    ✗ PM2 logs not found" -ForegroundColor Red
}

# 5. System environment variables
Write-Host "  [5/8] Collecting system environment..."
Get-ChildItem Env: | Where-Object { $_.Name -like "DB_*" -or $_.Name -like "MDDAP_*" -or $_.Name -like "PATH" -or $_.Name -like "NODE_*" } | 
Format-Table -AutoSize | Out-File (Join-Path $packageDir "system_env.txt") -Encoding UTF8 -Width 200
Write-Host "    ✓ System environment collected" -ForegroundColor Green

# 6. SQL Server instances
Write-Host "  [6/8] Detecting SQL Server instances..."
try {
    $sqlInstances = Get-Service | Where-Object { $_.Name -like "MSSQL*" }
    $sqlInstances | Format-Table -AutoSize | Out-File (Join-Path $packageDir "sql_instances.txt") -Encoding UTF8
    Write-Host "    ✓ SQL instances detected: $($sqlInstances.Count)" -ForegroundColor Green
}
catch {
    "✗ Failed to detect SQL instances" | Out-File (Join-Path $packageDir "sql_detection_error.txt")
    Write-Host "    ✗ SQL detection failed" -ForegroundColor Yellow
}

# 7. Network configuration
Write-Host "  [7/8] Collecting network configuration..."
ipconfig /all | Out-File (Join-Path $packageDir "network_config.txt") -Encoding UTF8
netstat -an | Select-String "1433" | Out-File (Join-Path $packageDir "sql_port_1433.txt") -Encoding UTF8
Write-Host "    ✓ Network config collected" -ForegroundColor Green

# 8. System information
Write-Host "  [8/8] Collecting system information..."
@"
Computer Name: $env:COMPUTERNAME
User Name: $env:USERNAME
OS: $(Get-WmiObject -Class Win32_OperatingSystem | Select-Object -ExpandProperty Caption)
PowerShell Version: $($PSVersionTable.PSVersion)
Node Version: $(node --version 2>&1)
NPM Version: $(npm --version 2>&1)
PM2 Version: $(pm2 --version 2>&1)
Python Version: $(python --version 2>&1)
Current Directory: $(Get-Location)
Timestamp: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
"@ | Out-File (Join-Path $packageDir "system_info.txt") -Encoding UTF8
Write-Host "    ✓ System info collected" -ForegroundColor Green

# Create summary
Write-Host "`nCreating summary report..."
@"
========================================
  Diagnostic Package Summary
========================================

Package Created: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Location: $packageDir

Files Collected:
- .env (environment configuration)
- ecosystem.config.js (PM2 configuration)
- pm2_status.txt (PM2 process status)
- pm2_jlist.json (PM2 process details)
- pm2_logs/ (PM2 application logs)
- system_env.txt (system environment variables)
- sql_instances.txt (SQL Server instances)
- network_config.txt (network configuration)
- sql_port_1433.txt (SQL port status)
- system_info.txt (system information)

Next Steps:
1. Review the files in this package
2. Check pm2_logs/ for error messages
3. Compare .env with ecosystem.config.js
4. Verify SQL Server is running (sql_instances.txt)
5. Check if port 1433 is listening (sql_port_1433.txt)

If deploying to another machine, zip this folder and share it for analysis.

========================================
"@ | Out-File (Join-Path $packageDir "README.txt") -Encoding UTF8

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "✓ Diagnostic package created!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "`nLocation: $packageDir" -ForegroundColor Yellow
Write-Host "`nYou can now:"
Write-Host "  1. Review the files in the package"
Write-Host "  2. Zip the folder to share for analysis"
Write-Host "  3. Check README.txt for next steps"

Write-Host "`nPress Enter to exit..."
Read-Host
