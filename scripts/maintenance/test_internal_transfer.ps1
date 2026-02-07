# Internal File Transfer Test Script
# Purpose: Test if the local machine can write to the server via network share.

$ErrorActionPreference = "Stop"

# --- CONFIGURATION (Change these) ---
$ServerName = "10.138.54.161" 
$TargetShare = "C$" # Or a specific shared folder like "Deployment"
$DestinationPath = "\\$ServerName\$TargetShare\Temp\mddap_test"
# ------------------------------------

Write-Host "--- Testing Internal File Transfer to $DestinationPath ---" -ForegroundColor Cyan

# 1. Create a small test file
$TestFile = "transfer_test.txt"
"Connection Test from $($env:COMPUTERNAME) at $((Get-Date).ToString())" | Out-File -FilePath $TestFile

# 2. Check if destination is reachable
if (Test-Path $DestinationPath) {
    Write-Host "Success: Destination path is reachable." -ForegroundColor Green
}
else {
    Write-Host "Note: Attempting to create destination folder..."
    try {
        New-Item -ItemType Directory -Path $DestinationPath -Force | Out-Null
        Write-Host "Success: Created destination folder." -ForegroundColor Green
    }
    catch {
        Write-Host "Error: Cannot reach or create destination. Please check network permissions/shares." -ForegroundColor Red
        return
    }
}

# 3. Perform the copy
Write-Host "Copying test file..."
try {
    Copy-Item -Path $TestFile -Destination $DestinationPath -Force
    Write-Host "Done! File successfully copied to the server." -ForegroundColor Green
}
catch {
    Write-Host "Error: Copy failed. $($_.Exception.Message)" -ForegroundColor Red
}

# 4. Cleanup
Remove-Item $TestFile
