# Final Dev Isolation Script
# This script ensures the ENTIRE repository is outside OneDrive.

$ErrorActionPreference = "Stop"

$OneDrivePath = "C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central"
$NewRepoPath = "C:\Apps\CZ_Digital_Central"

Write-Host "--- Finalizing Repository Isolation ---" -ForegroundColor Cyan

# 1. Check if we are already in the new path
$currentLocation = Get-Location
if ($currentLocation.Path -startsWith $NewRepoPath) {
    Write-Host "Success: You are currently working in the isolated path ($NewRepoPath)." -ForegroundColor Green
} else {
    Write-Host "Warning: You are currently in $currentLocation. Please switch to $NewRepoPath." -ForegroundColor Yellow
}

# 2. Cleanup Junctions (if they point back to OneDrive)
Write-Host "`nChecking for legacy junctions..."
$junctions = @("apps\web_dashboard\node_modules", "apps\web_dashboard\.next", ".venv")
foreach ($j in $junctions) {
    $fullPath = Join-Path $NewRepoPath $j
    if (Test-Path $fullPath) {
        $item = Get-Item $fullPath
        if ($item.Attributes -match "ReparsePoint") {
            Write-Host "Found junction at $j. Removing link to re-internalize files..."
            Remove-Item $fullPath -Force
            # Note: The actual files should already be in C:\Apps Store if the previous script ran.
            # We want them to be NATIVE folders now, not junctions.
        }
    }
}

Write-Host "`nSyncing latest changes to GitHub..."
git add .
git commit -m "chore: prepare for full repository isolation"
git push

Write-Host "`n[RECOMMENDATION] 1. Close VS Code." -ForegroundColor Cyan
Write-Host "[RECOMMENDATION] 2. Delete the folder in OneDrive: $OneDrivePath" -ForegroundColor Cyan
Write-Host "[RECOMMENDATION] 3. Open C:\Apps\CZ_Digital_Central in VS Code." -ForegroundColor Cyan
Write-Host "[RECOMMENDATION] 4. Run 'npm install' and 'pip install -r requirements.txt' to ensure local copies are fresh." -ForegroundColor Cyan

Write-Host "`n--- Isolation Ready ---" -ForegroundColor Green
