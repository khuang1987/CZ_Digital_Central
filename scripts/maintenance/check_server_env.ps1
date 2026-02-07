# MDDAP Server Health Check Script
# Purpose: Verify the server environment before deployment.

$ErrorActionPreference = "Continue"

Write-Host "--- CZ Digital Central Server Environment Health Check ---" -ForegroundColor Cyan
Write-Host "Checking requirements for internal network deployment...`n"

$results = @()

# 1. Node.js Check
$nodeVersion = try { node -v 2>$null } catch { $null }
if ($nodeVersion) {
    $major = [int]($nodeVersion -replace 'v', '' -replace '\..*', '')
    if ($major -ge 18) {
        $results += [PSCustomObject]@{ Component = "Node.js"; Status = "[OK]"; Detail = $nodeVersion }
    }
    else {
        $results += [PSCustomObject]@{ Component = "Node.js"; Status = "[WARN]"; Detail = "$nodeVersion (v18+ recommended)" }
    }
}
else {
    $results += [PSCustomObject]@{ Component = "Node.js"; Status = "[FAIL]"; Detail = "Not Found. Please install from installers/ folder." }
}

# 2. Python Check
$pythonVersion = try { python --version 2>$null } catch { $null }
if ($pythonVersion) {
    $results += [PSCustomObject]@{ Component = "Python"; Status = "[OK]"; Detail = $pythonVersion }
}
else {
    $results += [PSCustomObject]@{ Component = "Python"; Status = "[FAIL]"; Detail = "Not Found. Please install from installers/ folder." }
}

# 3. PM2 Check
$pm2Version = try { pm2 -v 2>$null } catch { $null }
if ($pm2Version) {
    $results += [PSCustomObject]@{ Component = "PM2"; Status = "[OK]"; Detail = "Version $pm2Version Found" }
}
else {
    $pm2Installer = Join-Path $PSScriptRoot "installers/pm2.tgz"
    if (Test-Path $pm2Installer) {
        $results += [PSCustomObject]@{ Component = "PM2"; Status = "[WARN]"; Detail = "Not installed, but pm2.tgz found in installers/" }
    }
    else {
        $results += [PSCustomObject]@{ Component = "PM2"; Status = "[WARN]"; Detail = "Not Found globally. (Global install recommended)" }
    }
}

# 4. ODBC Driver Check
$odbcDrivers = Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" }
if ($odbcDrivers) {
    $results += [PSCustomObject]@{ Component = "ODBC Driver"; Status = "[OK]"; Detail = ($odbcDrivers.Name -join ", ") }
}
else {
    $results += [PSCustomObject]@{ Component = "ODBC Driver"; Status = "[FAIL]"; Detail = "No SQL Server ODBC Driver found. Install msodbcsql.msi." }
}

# 5. Disk Space Check
$drive = Get-PSDrive C
$freeGB = [Math]::Round($drive.Free / 1GB, 2)
if ($freeGB -gt 5) {
    $results += [PSCustomObject]@{ Component = "Disk Space"; Status = "[OK]"; Detail = "$freeGB GB Free on C:" }
}
else {
    $results += [PSCustomObject]@{ Component = "Disk Space"; Status = "[WARN]"; Detail = "Only $freeGB GB Free on C:" }
}

# Display Results
$results | Format-Table -AutoSize

Write-Host "`n--- Summary ---" -ForegroundColor Cyan
$failedCount = ($results | Where-Object { $_.Status -eq "[FAIL]" }).Count
if ($failedCount -eq 0) {
    Write-Host "Environment looks good! You can proceed with deployment." -ForegroundColor Green
}
else {
    Write-Host "Found $failedCount critical issue(s). Please review and install missing pre-requisites." -ForegroundColor Red
}

Write-Host "`nInstallers for missing components are located in the 'installers/' folder." -ForegroundColor Gray
