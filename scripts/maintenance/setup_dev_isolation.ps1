# MDDAP Development Environment Isolation Script
# Purpose: Move .venv and node_modules to C:\Apps\CZ_Digital_Central to avoid OneDrive sync issues.

$ErrorActionPreference = "Stop"

# 1. Paths Configuration
$ProjectRoot = "c:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central"
$DashboardDir = Join-Path $ProjectRoot "apps\web_dashboard"

$AppsBase = "C:\Apps\CZ_Digital_Central"
$VenvStore = Join-Path $AppsBase ".venv"
$NodeModulesStore = Join-Path $AppsBase "node_modules_dashboard"
$NextStore = Join-Path $AppsBase "next_build_cache"

Write-Host "--- Initializing Dev Environment Isolation (C:\Apps) ---" -ForegroundColor Cyan

# 2. Create Storage directory
if (-not (Test-Path $AppsBase)) {
    New-Item -ItemType Directory -Path $AppsBase -Force | Out-Null
    Write-Host "Success: Created storage directory $AppsBase"
}

# 3. Handle Python Virtual Environment (.venv)
Write-Host "`n[1/2] Processing Python Virtual Environment (.venv)..." -ForegroundColor Yellow
$VenvPath = Join-Path $ProjectRoot ".venv"
if (Test-Path $VenvPath) {
    if ((Get-Item $VenvPath).Attributes -match "ReparsePoint") {
        Write-Host "Success: .venv is already isolated (linked)." -ForegroundColor Green
    } 
    else {
        Write-Host "Moving .venv files to $VenvStore ..."
        if (Test-Path $VenvStore) { Remove-Item -Recurse -Force $VenvStore }
        Move-Item -Path $VenvPath -Destination $VenvStore -Force
        
        Write-Host "Creating Junction link..."
        cmd /c "mklink /j ""$VenvPath"" ""$VenvStore"""
        Write-Host "Success: .venv isolation completed." -ForegroundColor Green
    }
} 
else {
    Write-Host "Note: No .venv folder found in root, skipping."
}

# 4. Handle Dashboard Dependencies (node_modules)
Write-Host "`n[2/2] Processing Dashboard Dependencies (node_modules)..." -ForegroundColor Yellow
$NodeModulesPath = Join-Path $DashboardDir "node_modules"
if (Test-Path $NodeModulesPath) {
    if ((Get-Item $NodeModulesPath).Attributes -match "ReparsePoint") {
        Write-Host "Success: node_modules is already isolated (linked)." -ForegroundColor Green
    } 
    else {
        Write-Host "Moving node_modules files to $NodeModulesStore ..."
        if (Test-Path $NodeModulesStore) { Remove-Item -Recurse -Force $NodeModulesStore }
        Move-Item -Path $NodeModulesPath -Destination $NodeModulesStore -Force
        
        Write-Host "Creating Junction link..."
        cmd /c "mklink /j ""$NodeModulesPath"" ""$NodeModulesStore"""
        Write-Host "Success: node_modules isolation completed." -ForegroundColor Green
    }
} 
else {
    Write-Host "Note: No node_modules found in apps\web_dashboard, skipping."
}

# 5. Handle Dashboard Build Cache (.next)
Write-Host "`n[3/3] Processing Dashboard Build Cache (.next)..." -ForegroundColor Yellow
$NextPath = Join-Path $DashboardDir ".next"
if (Test-Path $NextPath) {
    if ((Get-Item $NextPath).Attributes -match "ReparsePoint") {
        Write-Host "Success: .next is already isolated (linked)." -ForegroundColor Green
    } 
    else {
        Write-Host "Moving .next files to $NextStore ..."
        if (Test-Path $NextStore) { Remove-Item -Recurse -Force $NextStore }
        Move-Item -Path $NextPath -Destination $NextStore -Force
        
        Write-Host "Creating Junction link..."
        cmd /c "mklink /j ""$NextPath"" ""$NextStore"""
        Write-Host "Success: .next isolation completed." -ForegroundColor Green
    }
} 
else {
    Write-Host "Note: No .next folder found in apps\web_dashboard, skipping."
}

Write-Host "`n--- Setup Completed Successfully! ---" -ForegroundColor Cyan
Write-Host "Your dependencies are now physically stored in C:\Apps."
Write-Host "OneDrive will no longer sync these large folders." -ForegroundColor Gray
