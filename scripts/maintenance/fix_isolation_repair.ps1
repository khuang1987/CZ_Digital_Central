# MDDAP Environment Isolation Repair Script
# Purpose: Fix broken Junction links caused by locked folders (active terminal).

$ErrorActionPreference = "Stop"

# 1. Configuration
$ProjectRoot = "c:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central"
$DashboardDir = Join-Path $ProjectRoot "apps\web_dashboard"
$AppsBase = "C:\Apps\CZ_Digital_Central"

Write-Host "--- Repairing Environment Isolation ---" -ForegroundColor Cyan
Write-Host "IMPORTANT: Ensure all terminals with '(.venv)' and VS Code are CLOSED." -ForegroundColor Red

# Function to safely isolate a folder
function Repair-Folder {
    param($SourcePath, $DestStore, $FolderName)
    
    Write-Host "`nProcessing $FolderName..." -ForegroundColor Yellow
    
    if (Test-Path $SourcePath) {
        $item = Get-Item $SourcePath
        
        # If it's a broken link, remove it
        if ($item.Attributes -match "ReparsePoint") {
            Write-Host "Detected broken link at $SourcePath. Removing it..."
            # For junctions, we must use cmd /c rmdir to be safe
            cmd /c "rmdir ""$SourcePath"""
        }
    }

    # Now verify if the source PHYSICAL folder exists (it should have failed to move)
    if (Test-Path $SourcePath) {
        Write-Host "Source folder exists. Attempting robust move to $DestStore ..."
        
        if (-not (Test-Path (Split-Path $DestStore))) { 
            New-Item -ItemType Directory -Path (Split-Path $DestStore) -Force | Out-Null 
        }

        # Use Robocopy for better move handling (resilient to attributes)
        robocopy "$SourcePath" "$DestStore" /move /e /r:1 /w:1
        
        if ($LASTEXITCODE -lt 8) {
            Write-Host "Creating Junction link..."
            cmd /c "mklink /j ""$SourcePath"" ""$DestStore"""
            Write-Host "Success: $FolderName isolation repaired." -ForegroundColor Green
        }
        else {
            Write-Host "Error: Could not move $FolderName. Is it still in use?" -ForegroundColor Red
        }
    }
    else {
        # If it doesn't exist in project, maybe it moved partially?
        if (Test-Path $DestStore) {
            Write-Host "Folder already at $DestStore. Re-creating link..."
            cmd /c "mklink /j ""$SourcePath"" ""$DestStore"""
            Write-Host "Success: Link re-created." -ForegroundColor Green
        }
        else {
            Write-Host "Warning: Could not find $FolderName in Source or Destination." -ForegroundColor Yellow
        }
    }
}

# Repair .venv
Repair-Folder -SourcePath (Join-Path $ProjectRoot ".venv") -DestStore (Join-Path $AppsBase ".venv") -FolderName ".venv"

# Repair node_modules
Repair-Folder -SourcePath (Join-Path $DashboardDir "node_modules") -DestStore (Join-Path $AppsBase "node_modules_dashboard") -FolderName "node_modules"

Write-Host "`n--- Repair Attempt Finished ---" -ForegroundColor Cyan
