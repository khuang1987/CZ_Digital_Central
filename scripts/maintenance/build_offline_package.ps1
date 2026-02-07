param (
    [ValidateSet("Full", "Frontend", "Logic")]
    [string]$Mode = "Full",
    
    [string]$OutputDir = "",

    [string]$PackageName = ""
)

$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\Apps\CZ_Digital_Central"
$pythonVersion = try { python --version 2>$null } catch { "Unknown" }

if ($OutputDir -eq "") {
    $BaseTargetDir = $ProjectRoot
}
else {
    $BaseTargetDir = $OutputDir
    if (-not (Test-Path $BaseTargetDir)) {
        New-Item -ItemType Directory -Path $BaseTargetDir -Force | Out-Null
    }
}

if ($PackageName -ne "") {
    $BuildDir = Join-Path $BaseTargetDir $PackageName
}
else {
    $BuildDir = Join-Path $BaseTargetDir "deploy_$( $Mode )_$(Get-Date -Format 'yyyyMMdd_HHmm')"
}

$WebDashboardDir = Join-Path $ProjectRoot "apps\web_dashboard"

Write-Host "--- Starting Offline Packaging Process (Mode: $Mode) ---" -ForegroundColor Cyan

# 1. Directory Handling
if ($Mode -eq "Full" -and $PackageName -eq "") {
    # Only clean if it's a new timestamped full build
    if (Test-Path $BuildDir) { Remove-Item -Recurse -Force $BuildDir }
}

if (-not (Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir | Out-Null
}
$DashboardTarget = New-Item -ItemType Directory -Path (Join-Path $BuildDir "apps/web_dashboard") -Force
$OfflinePkgsDir = New-Item -ItemType Directory -Path (Join-Path $BuildDir "offline_pkgs") -Force

# 2. Build Frontend (Production)
if ($Mode -ne "Logic") {
    Write-Host "`n[1/4] Building Frontend Assets (Standalone Mode)..." -ForegroundColor Yellow
    Set-Location $WebDashboardDir
    npm run build

    Write-Host "Copying production assets..."
    # Next.js standalone folder contains the minimal node_modules and server logic
    $standalonePath = Join-Path $WebDashboardDir ".next/standalone"
    if (Test-Path $standalonePath) {
        # Copy everything within standalone to the target apps/web_dashboard
        # This includes the standalone node_modules!
        Copy-Item -Path "$standalonePath\*" -Destination $DashboardTarget.FullName -Recurse -Force
        
        # Next.js standalone needs .next/static and public to be manually copied
        $staticSource = Join-Path $WebDashboardDir ".next/static"
        $staticTarget = New-Item -ItemType Directory -Path (Join-Path $DashboardTarget.FullName ".next/static") -Force
        Copy-Item -Path "$staticSource\*" -Destination $staticTarget.FullName -Recurse -Force
        
        $publicSource = Join-Path $WebDashboardDir "public"
        $publicTarget = New-Item -ItemType Directory -Path (Join-Path $DashboardTarget.FullName "public") -Force
        Copy-Item -Path "$publicSource\*" -Destination $publicTarget.FullName -Recurse -Force
        
        Write-Host "Success: Standalone build assets copied." -ForegroundColor Green
    }
    else {
        Write-Host "Error: Standalone build output not found at $standalonePath" -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "`n[1/4] Skipping Frontend Build (Logic Mode)" -ForegroundColor Gray
}

# 3. Pull Python Dependencies (Wheels)
if ($Mode -eq "Full") {
    Write-Host "`n[2/4] Downloading Python wheels for offline installation..." -ForegroundColor Yellow
    Set-Location $ProjectRoot
    python -m pip download --only-binary=:all: --platform win_amd64 --python-version 312 -d "$($OfflinePkgsDir.FullName)" -r requirements.txt
    
    $pkgCount = (Get-ChildItem -Path "$($OfflinePkgsDir.FullName)").Count
    if ($pkgCount -eq 0) {
        Write-Host "Error: No Python wheels were downloaded. Check your internet connection." -ForegroundColor Red
        exit 1
    }
    Write-Host "Success: Downloaded $pkgCount wheels to offline_pkgs." -ForegroundColor Green
}
else {
    Write-Host "`n[2/4] Skipping Python Wheels download (Patch Mode)" -ForegroundColor Gray
}

# 4. Copy Backend Logic & Scripts
Write-Host "`n[3/4] Copying backend logic and shared infrastructure..." -ForegroundColor Yellow
$backendDirs = @("data_pipelines", "scripts", "shared_infrastructure", "config", "apps/backend_api")
foreach ($dir in $backendDirs) {
    if (Test-Path (Join-Path $ProjectRoot $dir)) {
        Copy-Item -Path (Join-Path $ProjectRoot $dir) -Destination (Join-Path $BuildDir $dir) -Recurse -Force
    }
}

# 5. Copy Base Files & Maintenance Tools
Write-Host "`n[3/4] Copying maintenance tools and installers..." -ForegroundColor Yellow
$baseFiles = @("requirements.txt", "README.md")
foreach ($file in $baseFiles) {
    if (Test-Path $file) {
        Copy-Item -Path $file -Destination $BuildDir -Force
    }
}

# Copy unified .env template (placeholders will be resolved on deployment)
if (Test-Path ".env") {
    Copy-Item -Path ".env" -Destination $BuildDir -Force
    Write-Host "Copied unified .env template" -ForegroundColor Green
}
else {
    Write-Host "[WARN] .env template not found!" -ForegroundColor Yellow
}

# Copy the health check script
Copy-Item -Path "scripts\maintenance\check_server_env.ps1" -Destination $BuildDir -Force

# Copy diagnostics toolkit
if (Test-Path "diagnostics") {
    Copy-Item -Path "diagnostics" -Destination $BuildDir -Recurse -Force
    Write-Host "Included diagnostics toolkit" -ForegroundColor Green
}

# Note: Large software installers are excluded per user request.
# However, we include the small PM2 offline tarball for automation.
$targetInstallers = New-Item -ItemType Directory -Path (Join-Path $BuildDir "installers") -Force
$pm2Source = "c:\Apps\CZ_Digital_Central\server_deploy_package\installers\pm2_complete.zip"
if (Test-Path $pm2Source) {
    Copy-Item -Path $pm2Source -Destination $targetInstallers.FullName -Force
    Write-Host "Included complete PM2 offline bundle (with all dependencies)" -ForegroundColor Green
}
else {
    Write-Host "[WARN] PM2 offline bundle not found at $pm2Source" -ForegroundColor Yellow
}

# 6. Create Server Install & Deployment Scripts
Write-Host "`n[4/4] Creating server-side helpers..." -ForegroundColor Yellow

# Helper 1: New Server Setup
$installScript = @"
# Server Installation Helper
`$ErrorActionPreference = "Continue"

`$OperationalDir = "C:\Apps\CZ_Digital_Central_Server"
`$SourceDir = Get-Location

Write-Host "--- CZ Digital Central Server Deployment Setup ---" -ForegroundColor Cyan

# 1. Mandatory Health Check
Write-Host "Step 1: Running Environment Health Check..." -ForegroundColor Yellow
& ".\check_server_env.ps1"

Write-Host "`nIf all [FAIL] items are resolved, press Enter to continue with setup, or Ctrl+C to stop."
Read-Host

# 2. Setup Python Env
if (-not (Test-Path ".venv")) {
    Write-Host "`nStep 2: Creating Python Virtual Environment..." -ForegroundColor Yellow
    python -m venv .venv
}
Write-Host "Installing dependencies from offline_pkgs..."
try {
    & ".\.venv\Scripts\pip" install --no-index --find-links=offline_pkgs -r requirements.txt
    Write-Host "Success: All dependencies installed from offline_pkgs." -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Failed to install dependencies from offline_pkgs." -ForegroundColor Red
    Write-Host "Please ensure your server's Python version matches the developer's ($pythonVersion)." -ForegroundColor Red
    Write-Host "Try running: .\.venv\Scripts\pip install --no-index --find-links=offline_pkgs pandas" -ForegroundColor Yellow
}

# 3. Setup PM2 (Global)
Write-Host "`nStep 3: Process Management Setup (PM2)..." -ForegroundColor Yellow
`$pm2Installer = Join-Path `$SourceDir "installers/pm2.tgz"
if (Test-Path `$pm2Installer) {
    Write-Host "Installing PM2 globally from offline tarball..."
    npm install -g `$pm2Installer
}
else {
    Write-Host "[WARN] PM2 installer not found in installers/. Please install manually if needed."
}

# 4. Starting Dashboard
Write-Host "`nStep 4: Starting Dashboard Service..." -ForegroundColor Yellow
Set-Location "`$OperationalDir\apps\web_dashboard"
# We recommend node server.js for standalone build, managed by PM2
try {
    pm2 start server.js --name 'cz_dashboard'
    Write-Host "Success: Dashboard service started via PM2." -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Failed to start via PM2. Try manual start: node server.js" -ForegroundColor Red
}

Write-Host "`n--- Deployment Setup Completed ---" -ForegroundColor Green
"@

# Helper 2: Unified "One-Click" Deployment Script
$deployToLocalScript = @'
# Unified Deployment & Environment Setup
$ErrorActionPreference = "Stop" # Full stop on errors for visibility
trap { 
    Write-Host "`n[FATAL ERROR] $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "`nClick this window and press Enter to exit..."
    Read-Host
    exit
}

$OperationalDir = "C:\Apps\CZ_Digital_Central_Server"
$SourceDir = Get-Location

Clear-Host
Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "   CZ Digital Central 'One-Click' Deployment" -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "Current Package: $SourceDir"
Write-Host "Operation Target: $OperationalDir"

# 1. Mandatory File Transfer
Write-Host "`n[1/5] Transferring files to operational directory..." -ForegroundColor Yellow
if (-not (Test-Path "$OperationalDir")) {
    New-Item -ItemType Directory -Path "$OperationalDir" -Force | Out-Null
}

Write-Host "Stopping services to allow file copy..."
try { pm2 stop cz_dashboard --silent } catch {}

$exclude = @(".venv", ".next", "node_modules", "installers", "offline_pkgs", "server_setup.ps1", "deploy_to_local.ps1", "deploy_to_local.bat")
Get-ChildItem -Path "$SourceDir" | Where-Object { $_.Name -notin $exclude } | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination "$OperationalDir" -Recurse -Force
}

# 2. Smart Environment Configuration (Unified .env Template)
Write-Host "`n[2/5] Processing environment configuration..." -ForegroundColor Yellow

# Function to resolve placeholders in .env template
function Resolve-EnvTemplate {
    param([string]$TemplateContent)
    
    # Auto-detect OneDrive path
    $onedrivePath = ""
    $possiblePaths = @(
        "$env:USERPROFILE\OneDrive - Medtronic PLC",
        "$env:OneDrive",
        "$env:OneDriveCommercial",
        "$env:USERPROFILE\OneDrive"
    )
    
    foreach ($path in $possiblePaths) {
        if (Test-Path $path) {
            $onedrivePath = $path
            Write-Host "  Detected OneDrive: $onedrivePath" -ForegroundColor Green
            break
        }
    }
    
    if (-not $onedrivePath) {
        Write-Host "  [WARN] OneDrive path not detected, using default" -ForegroundColor Yellow
        $onedrivePath = "$env:USERPROFILE\OneDrive - Medtronic PLC"
    }
    
    # Replace placeholders
    $resolved = $TemplateContent -replace '\{\{ONEDRIVE_PATH\}\}', $onedrivePath
    $resolved = $resolved -replace '\{\{USERNAME\}\}', $env:USERNAME
    $resolved = $resolved -replace '\{\{COMPUTERNAME\}\}', $env:COMPUTERNAME
    
    return $resolved
}

# Load and process .env template
$envTemplatePath = Join-Path "$SourceDir" ".env"
if (Test-Path $envTemplatePath) {
    Write-Host "  Loading .env template..." -ForegroundColor Cyan
    $envTemplate = Get-Content $envTemplatePath -Raw -Encoding UTF8
    $resolvedEnv = Resolve-EnvTemplate $envTemplate
    $resolvedEnv | Out-File (Join-Path "$OperationalDir" ".env") -Encoding UTF8 -Force
    Write-Host "  Environment configured successfully" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] .env template not found!" -ForegroundColor Red
    throw ".env template is missing from deployment package"
}

# 3. Validation & Setup
Write-Host "`n[3/5] Validating System Environment..." -ForegroundColor Yellow
Set-Location "$OperationalDir"
if (Test-Path ".\check_server_env.ps1") { & ".\check_server_env.ps1" }

if (-not (Test-Path ".venv")) {
    Write-Host "Re-initializing Python .venv..." -ForegroundColor Yellow
    python -m venv .venv
    if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
        throw "Failed to create Python virtual environment!"
    }
}

Write-Host "Syncing Python dependencies (Offline)..."
$pipInstall = & ".\.venv\Scripts\pip" install --no-index --find-links="$SourceDir\offline_pkgs" -r requirements.txt 2>&1
$installExitCode = $LASTEXITCODE

if ($installExitCode -ne 0) {
    Write-Host "" -ForegroundColor Red
    Write-Host "================================================================" -ForegroundColor Red
    Write-Host "  ERROR: Python Dependency Installation Failed" -ForegroundColor Red
    Write-Host "================================================================" -ForegroundColor Red
    Write-Host "Exit Code: $installExitCode" -ForegroundColor Yellow
    Write-Host "Output:" -ForegroundColor Yellow
    $pipInstall | ForEach-Object { Write-Host "  $_" }
    Write-Host ""
    Write-Host "Please ensure offline_pkgs folder contains all required packages." -ForegroundColor White
    Write-Host "Press any key to exit..."
    Read-Host
    throw "Dependency installation failed with exit code $installExitCode"
}

# Verify critical packages
Write-Host "Verifying critical packages..."
Write-Host "  Checking .venv location: $(Get-Location)\.venv" -ForegroundColor Gray
Write-Host "  Listing installed packages..." -ForegroundColor Gray

$installedPkgs = & ".\.venv\Scripts\pip" list 2>&1 | Out-String
$pkgLines = ($installedPkgs -split "`n")
$pkgCount = $pkgLines.Count
Write-Host "  Installed packages count: $pkgCount" -ForegroundColor Gray

$criticalPackages = @("uvicorn", "fastapi", "python-dotenv")
$missingPackages = @()

foreach ($pkg in $criticalPackages) {
    Write-Host "  Checking $pkg..." -ForegroundColor Gray
    $check = & ".\.venv\Scripts\pip" show $pkg 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    ❌ $pkg not found (exit code: $LASTEXITCODE)" -ForegroundColor Yellow
        $missingPackages += $pkg
    } else {
        Write-Host "    ✅ $pkg found" -ForegroundColor Green
    }
}

if ($missingPackages.Count -gt 0) {
    Write-Host "" -ForegroundColor Red
    Write-Host "================================================================" -ForegroundColor Red
    Write-Host "  ERROR: Critical Packages Missing" -ForegroundColor Red
    Write-Host "================================================================" -ForegroundColor Red
    Write-Host "Missing packages: $($missingPackages -join ', ')" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Diagnostic: Let's check what was actually installed..." -ForegroundColor Cyan
    Write-Host "Running: .\.venv\Scripts\pip list | Select-String 'uvicorn|fastapi|dotenv'" -ForegroundColor Gray
    $relevantPkgs = & ".\.venv\Scripts\pip" list 2>&1 | Select-String "uvicorn|fastapi|dotenv"
    if ($relevantPkgs) {
        Write-Host "Found related packages:" -ForegroundColor Yellow
        $relevantPkgs | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
    } else {
        Write-Host "No related packages found in .venv!" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "The backend WILL NOT start without these packages." -ForegroundColor White
    Write-Host "Press any key to exit..."
    Read-Host
    throw "Critical packages not found: $($missingPackages -join ', ')"
}

Write-Host "✅ All dependencies verified successfully!" -ForegroundColor Green

# 4. Launching Services
Write-Host "`n[4/5] Launching Services..." -ForegroundColor Yellow

# Check if PM2 is available
$pm2Exists = Get-Command pm2 -ErrorAction SilentlyContinue -CommandType Application, ExternalScript

if (-not $pm2Exists) {
    Write-Host "PM2 not found. Attempting offline installation..." -ForegroundColor Cyan
    $pm2Bundle = Join-Path "$SourceDir\installers" "pm2_complete.zip"
    
    if (Test-Path $pm2Bundle) {
        Write-Host "Extracting PM2 bundle with all dependencies..." -ForegroundColor Yellow
        $tempExtract = Join-Path $env:TEMP "pm2_install_temp"
        if (Test-Path $tempExtract) { Remove-Item $tempExtract -Recurse -Force }
        Expand-Archive -Path $pm2Bundle -DestinationPath $tempExtract -Force
        
        # Get npm global prefix
        $npmPrefix = (npm config get prefix)
        $globalNodeModules = Join-Path $npmPrefix "node_modules"
        
        Write-Host "Installing PM2 to global node_modules: $globalNodeModules" -ForegroundColor Yellow
        
        # Copy PM2 and all dependencies
        if (-not (Test-Path $globalNodeModules)) {
            New-Item -ItemType Directory -Path $globalNodeModules -Force | Out-Null
        }
        
        Copy-Item -Path "$tempExtract\node_modules\*" -Destination $globalNodeModules -Recurse -Force
        
        # Create PM2 bin symlinks
        $npmBin = Join-Path $npmPrefix ""
        $pm2Bin = Join-Path $globalNodeModules "pm2\bin\pm2"
        
        # Refresh PATH
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        
        # Verify installation
        $pm2Exists = Get-Command pm2 -ErrorAction SilentlyContinue -CommandType Application, ExternalScript
        if ($pm2Exists) {
            Write-Host "✓ PM2 installed successfully from offline bundle!" -ForegroundColor Green
        } else {
            Write-Host "[WARN] PM2 files copied but command not in PATH. Trying direct execution..." -ForegroundColor Yellow
            # Create a wrapper script
            $pm2Wrapper = Join-Path $npmPrefix "pm2.cmd"
            "@echo off`nnode `"$pm2Bin`" %*" | Out-File -FilePath $pm2Wrapper -Encoding ASCII
            Write-Host "Created PM2 wrapper at: $pm2Wrapper" -ForegroundColor Green
        }
        
        # Cleanup
        Remove-Item $tempExtract -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "" -ForegroundColor Red
        Write-Host "================================================================" -ForegroundColor Red
        Write-Host "  ERROR: PM2 Offline Bundle Not Found" -ForegroundColor Red
        Write-Host "================================================================" -ForegroundColor Red
        Write-Host ""
        Write-Host "Expected location: `$pm2Bundle" -ForegroundColor Yellow
        Write-Host "Please ensure the deployment package includes pm2_complete.zip" -ForegroundColor White
        Write-Host ""
        Write-Host "Press any key to exit..."
        Read-Host
        throw "PM2 offline bundle not found."
    }
} else {
    Write-Host "✓ PM2 is already available in the system." -ForegroundColor Green
}

# Clean approach: always try to delete then always start
# This guarantees fresh env loading and avoids buggy JSON parsing
Write-Host "Refreshing process instances..."
try { 
    pm2 delete cz_dashboard --silent 
    pm2 delete cz_backend --silent
} catch {}

# CRITICAL: Manually load .env into session environment for PM2 inheritance
$envFilePath = Join-Path "$OperationalDir" ".env"
$envVars = @{}
if (Test-Path $envFilePath) {
    Write-Host "Loading configuration into service environment (with quote trimming)..."
    Get-Content $envFilePath | Where-Object { $_ -match '=' -and $_ -notmatch '^#' } | ForEach-Object {
        $name, $value = $_.Split('=', 2)
        # Robust trimming of quotes (") and whitespace
        $cleanValue = $value.Trim().Trim('"').Trim("'")
        $envVars[$name.Trim()] = $cleanValue
        [System.Environment]::SetEnvironmentVariable($name.Trim(), $cleanValue, [System.EnvironmentVariableTarget]::Process)
    }
}

# Create PM2 ecosystem file with environment variables
$ecosystemConfig = @"
module.exports = {
  apps: [
    {
      name: 'cz_backend',
      script: '.venv/Scripts/python.exe',
      args: '-m uvicorn apps.backend_api.main:app --host 0.0.0.0 --port 8000',
      cwd: '$($OperationalDir -replace '\\', '/')',
      env: {
"@

foreach ($key in $envVars.Keys) {
    # Escape backslashes for JavaScript string (double them)
    $value = $envVars[$key] -replace '\\', '\\\\'
    $ecosystemConfig += "`n        '$key': '$value',"
}

$ecosystemConfig += @"

      }
    },
    {
      name: 'cz_dashboard',
      script: 'server.js',
      cwd: '$($OperationalDir -replace '\\', '/')/apps/web_dashboard',
      env: {
"@

foreach ($key in $envVars.Keys) {
    # Escape backslashes for JavaScript string (double them)
    $value = $envVars[$key] -replace '\\', '\\\\'
    $ecosystemConfig += "`n        '$key': '$value',"
}

$ecosystemConfig += @"

      }
    }
  ]
};
"@

$ecosystemPath = Join-Path "$OperationalDir" "ecosystem.config.js"
$ecosystemConfig | Out-File -FilePath $ecosystemPath -Encoding UTF8 -Force

Write-Host "Starting services from ecosystem file..."
Set-Location "$OperationalDir"
pm2 start ecosystem.config.js --update-env

Write-Host "Deployment complete." -ForegroundColor Green

# 5. Summary & Final Reveal
Write-Host "`n[5/5] Final Summary..." -ForegroundColor Yellow
$ipObj = Get-NetIPAddress -AddressFamily IPv4 | Where-Object { 
    $_.InterfaceAlias -notlike "*Loopback*" -and $_.IPv4Address -notlike "169.254.*" -and $_.InterfaceAlias -notlike "*vEthernet*" -and $_.InterfaceAlias -notlike "*VMware*"
} | Select-Object -First 1
$LocalIP = if ($ipObj) { $ipObj.IPAddress } else { "127.0.0.1" }
$DashboardUrl = "http://{0}:3000" -f $LocalIP

Write-Host "========================================" -ForegroundColor Green
Write-Host "🎉 DEPLOYMENT SUCCESSFUL!" -ForegroundColor Green
Write-Host "Access URL (Real IP): $DashboardUrl" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Green

Write-Host "`nOpening Dashboard..."
try { Start-Process $DashboardUrl } catch {}

Write-Host "`nPress any key to close this window..."
Read-Host
'@

$deployBat = @"
@echo off
setlocal
title CZ Digital Central - One-Click Deployer
cd /d "%~dp0"

:: Check for Administrator privileges
net session >nul 2>&1
if %errorLevel% == 0 (
    goto :admin
) else (
    echo.
    echo [INFO] Administrative privileges required for PM2 installation and system path updates.
    echo [INFO] Requesting elevation (UAC prompt will appear)...
    powershell -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

:admin
echo.
echo ====================================================
echo    CZ Digital Central Deployment Wrapper (ADMIN)
echo ====================================================
echo.
echo Running PowerShell deployment script...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "deploy_to_local.ps1"
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Deployment script failed with exit code %ERRORLEVEL%
    pause
)
pause
"@

$installScript | Out-File -FilePath (Join-Path $BuildDir "server_setup.ps1") -Encoding utf8
$deployToLocalScript | Out-File -FilePath (Join-Path $BuildDir "deploy_to_local.ps1") -Encoding utf8
$deployBat | Out-File -FilePath (Join-Path $BuildDir "deploy_to_local.bat") -Encoding oem

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Offline Deployment Package Created!" -ForegroundColor Green
Write-Host "Location: $BuildDir" -ForegroundColor Green
Write-Host "Next Step: Zip this folder and copy it to your server." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Green
