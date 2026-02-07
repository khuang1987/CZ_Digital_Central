# SQL Connection & Service Diagnostics Script
# This script checks SQL Server connectivity and PM2 service status (Dashboard & Backend)

$ErrorActionPreference = "Continue"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CZ Digital Central Service Diagnostics" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Create diagnostics output directory
$diagDir = Join-Path (Get-Location) "diagnostics_output"
if (-not (Test-Path $diagDir)) {
    New-Item -ItemType Directory -Path $diagDir -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $diagDir "service_diagnostics_$timestamp.txt"

function Write-Output-Both {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
    $Message | Out-File -FilePath $outputFile -Append -Encoding UTF8
}

# 1. Environment Variables Check
Write-Output-Both "`n=== 1. Environment Variables Check ===" "Yellow"
Write-Output-Both "Checking .env file..."

# Try multiple locations for .env
$envPath = ""
$possibleEnvPaths = @(
    Join-Path (Get-Location) ".env",
    Join-Path (Split-Path (Get-Location) -Parent) ".env"
)
foreach ($p in $possibleEnvPaths) {
    if (Test-Path $p) { $envPath = $p; break }
}

if ($envPath) {
    Write-Output-Both "✓ .env file found at: $envPath" "Green"
    Write-Output-Both "`nDB-related variables in .env:"
    $envLines = Get-Content $envPath
    foreach ($line in $envLines) {
        if ($line -match "DB_" -and $line -notmatch "^#") {
            Write-Output-Both "  $line"
        }
    }
}
else {
    Write-Output-Both "✗ .env file NOT found!" "Red"
}

# 2. PM2 Environment Variables
Write-Output-Both "`n=== 2. PM2 Environment Variables ===" "Yellow"
Write-Output-Both "Checking ecosystem.config.js..."

$ecosystemPath = ""
$possibleEcoPaths = @(
    Join-Path (Get-Location) "ecosystem.config.js",
    Join-Path (Split-Path (Get-Location) -Parent) "ecosystem.config.js"
)
foreach ($p in $possibleEcoPaths) {
    if (Test-Path $p) { $ecosystemPath = $p; break }
}

if ($ecosystemPath) {
    Write-Output-Both "✓ ecosystem.config.js found at: $ecosystemPath" "Green"
    Write-Output-Both "`nDB-related variables in ecosystem.config.js:"
    $ecoLines = Get-Content $ecosystemPath
    foreach ($line in $ecoLines) {
        if ($line -match "DB_") {
            Write-Output-Both "  $($line.Trim())"
        }
    }
}
else {
    Write-Output-Both "✗ ecosystem.config.js NOT found!" "Red"
}

# 3. Virtual Environment Check
Write-Output-Both "`n=== 3. Virtual Environment Check ===" "Yellow"
$venvPath = Join-Path (if ($ecosystemPath) { Split-Path $ecosystemPath } else { Get-Location }) ".venv"
$pythonPath = Join-Path $venvPath "Scripts\python.exe"
if (Test-Path $pythonPath) {
    Write-Output-Both "✓ Python interpreter found at: $pythonPath" "Green"
    try {
        $version = & $pythonPath --version 2>&1
        Write-Output-Both "  Python version: $version"
    }
    catch {}
}
else {
    Write-Output-Both "✗ Python interpreter NOT found in .venv! Backend WILL fail." "Red"
}

# 4. SQL Server Connectivity Test
Write-Output-Both "`n=== 4. SQL Server Connectivity Test ===" "Yellow"

# Try to load SQL credentials from .env
$dbServer = ""
$dbPort = ""
$dbUser = ""
$dbPassword = ""
$dbName = ""

if ($envPath -and (Test-Path $envPath)) {
    $envLines = Get-Content $envPath
    foreach ($line in $envLines) {
        if ($line -match "=" -and $line -notmatch "^#") {
            $parts = $line.Split('=', 2)
            if ($parts.Count -eq 2) {
                $name = $parts[0].Trim()
                $val = $parts[1].Trim().Trim('"').Trim("'")
                switch ($name) {
                    "DB_SERVER" { $dbServer = $val }
                    "DB_PORT" { $dbPort = $val }
                    "DB_USER" { $dbUser = $val }
                    "DB_PASSWORD" { $dbPassword = $val }
                    "DB_NAME" { $dbName = $val }
                    "DB_DATABASE" { if ($null -eq $dbName -or $dbName -eq "") { $dbName = $val } }
                }
            }
        }
    }
}

$passDisplay = if ($dbPassword) { "***" } else { "(empty)" }

Write-Output-Both "`nSQL Connection Parameters:"
Write-Output-Both "  Server: $dbServer"
Write-Output-Both "  Port: $dbPort"
Write-Output-Both "  Database/Name: $dbName"
Write-Output-Both "  User: $dbUser"
Write-Output-Both "  Password: $passDisplay"

# Test TCP connection
if ($dbServer -and $dbPort) {
    Write-Output-Both "`nTesting TCP connection to ${dbServer}:${dbPort}..."
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.Connect($dbServer, $dbPort)
        $tcpClient.Close()
        Write-Output-Both "✓ TCP connection successful!" "Green"
    }
    catch {
        Write-Output-Both "✗ TCP connection failed: $($_.Exception.Message)" "Red"
    }
}

# 5. PM2 Process Status
Write-Output-Both "`n=== 5. PM2 Process Status ===" "Yellow"
try {
    $pm2Raw = pm2 jlist 2>&1 | Out-String
    if ($pm2Raw -match '\[.*\]') {
        $jsonStr = $Matches[0]
        $pm2Status = $jsonStr | ConvertFrom-Json
        
        foreach ($procName in @("cz_dashboard", "cz_backend")) {
            $proc = $pm2Status | Where-Object { $_.name -eq $procName }
            if ($proc) {
                Write-Output-Both "`n$procName Process Info:"
                Write-Output-Both "  Status: $($proc.pm2_env.status)" " $(if ($proc.pm2_env.status -eq 'online') { 'Green' } else { 'Red' })"
                Write-Output-Both "  PID: $($proc.pid)"
                Write-Output-Both "  Restarts: $($proc.pm2_env.restart_time)"
                Write-Output-Both "  Uptime: $([Math]::Round($proc.pm2_env.pm_uptime / 1000 / 60, 2)) minutes"
            }
            else {
                Write-Output-Both "✗ $procName process not found in PM2!" "Red"
            }
        }
    }
    else {
        throw "Could not find valid JSON array in PM2 output"
    }
}
catch {
    Write-Output-Both "✗ Failed to get PM2 status: $($_.Exception.Message)" "Red"
}

# 6. Service Error Logs
Write-Output-Both "`n=== 6. Service Error Logs ===" "Yellow"
$logsToCheck = @(
    @{ Name = "Dashboard"; Path = "$env:USERPROFILE\.pm2\logs\cz-dashboard-error.log" },
    @{ Name = "Backend"; Path = "$env:USERPROFILE\.pm2\logs\cz-backend-error.log" }
)

foreach ($log in $logsToCheck) {
    Write-Output-Both "`nLast 20 lines of $($log.Name) error log:"
    if (Test-Path $log.Path) {
        $lines = Get-Content $log.Path -Tail 20
        foreach ($line in $lines) { Write-Output-Both "  $line" }
    }
    else {
        Write-Output-Both "  (No error log found at $($log.Path))"
    }
}

Write-Output-Both "`n========================================" "Cyan"
Write-Output-Both "Diagnostics complete!" "Green"
Write-Output-Both "Full report saved to: $outputFile" "Green"
Write-Output-Both "========================================" "Cyan"

Write-Host "`nPress Enter to exit..."
Read-Host
