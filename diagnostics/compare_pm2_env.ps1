# PM2 Environment Comparison Script
# Compares .env file with PM2 runtime environment

$ErrorActionPreference = "Continue"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  PM2 Environment Comparison" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Create diagnostics output directory
$diagDir = Join-Path (Get-Location) "diagnostics_output"
if (-not (Test-Path $diagDir)) {
    New-Item -ItemType Directory -Path $diagDir -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path $diagDir "pm2_env_comparison_$timestamp.txt"

function Write-Output-Both {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
    $Message | Out-File -FilePath $outputFile -Append -Encoding UTF8
}

# Load .env file
$envPath = Join-Path (Get-Location) ".env"
$envVars = @{}

if (Test-Path $envPath) {
    Write-Output-Both "Loading .env file..." "Yellow"
    Get-Content $envPath | Where-Object { $_ -match "=" -and $_ -notmatch "^#" } | ForEach-Object {
        $name, $value = $_.Split('=', 2)
        $cleanValue = $value.Trim().Trim('"').Trim("'")
        $envVars[$name.Trim()] = $cleanValue
    }
    Write-Output-Both "✓ Loaded $($envVars.Count) variables from .env" "Green"
}
else {
    Write-Output-Both "✗ .env file not found!" "Red"
    exit 1
}

# Get PM2 environment
Write-Output-Both "`nGetting PM2 environment..." "Yellow"
try {
    $pm2Status = pm2 jlist 2>&1 | ConvertFrom-Json
    $dashboard = $pm2Status | Where-Object { $_.name -eq "cz_dashboard" }
    
    if (-not $dashboard) {
        Write-Output-Both "✗ Dashboard process not found in PM2!" "Red"
        exit 1
    }
    
    Write-Output-Both "✓ Found dashboard process (PID: $($dashboard.pid))" "Green"
}
catch {
    Write-Output-Both "✗ Failed to get PM2 status: $($_.Exception.Message)" "Red"
    exit 1
}

# Compare environments
Write-Output-Both "`n=== Environment Variable Comparison ===" "Cyan"
Write-Output-Both "`nFormat: [Variable Name]"
Write-Output-Both "  .env value: ..."
Write-Output-Both "  PM2 value: ..."
Write-Output-Both "  Status: ✓ Match / ✗ Mismatch / ⚠ Missing"

$mismatches = 0
$missing = 0

foreach ($key in $envVars.Keys | Sort-Object) {
    if ($key -like "DB_*" -or $key -like "MDDAP_*") {
        $envValue = $envVars[$key]
        $pm2Value = $dashboard.pm2_env.env.$key
        
        Write-Output-Both "`n[$key]"
        Write-Output-Both "  .env: $envValue"
        Write-Output-Both "  PM2:  $pm2Value"
        
        if ($null -eq $pm2Value) {
            Write-Output-Both "  Status: ⚠ MISSING IN PM2" "Yellow"
            $missing++
        }
        elseif ($envValue -ne $pm2Value) {
            Write-Output-Both "  Status: ✗ MISMATCH" "Red"
            $mismatches++
        }
        else {
            Write-Output-Both "  Status: ✓ Match" "Green"
        }
    }
}

Write-Output-Both "`n=== Summary ===" "Cyan"
Write-Output-Both "Total variables checked: $($envVars.Keys | Where-Object { $_ -like "DB_*" -or $_ -like "MDDAP_*" } | Measure-Object | Select-Object -ExpandProperty Count)"
Write-Output-Both "Mismatches: $mismatches" $(if ($mismatches -gt 0) { "Red" } else { "Green" })
Write-Output-Both "Missing in PM2: $missing" $(if ($missing -gt 0) { "Yellow" } else { "Green" })

if ($mismatches -gt 0 -or $missing -gt 0) {
    Write-Output-Both "`n⚠ ISSUE DETECTED!" "Red"
    Write-Output-Both "PM2 environment does not match .env file." "Red"
    Write-Output-Both "Recommended action: Restart PM2 services with updated environment." "Yellow"
}
else {
    Write-Output-Both "`n✓ All variables match!" "Green"
}

Write-Output-Both "`nFull report saved to: $outputFile" "Cyan"

Write-Host "`nPress Enter to exit..."
Read-Host
