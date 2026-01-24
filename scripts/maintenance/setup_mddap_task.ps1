$ErrorActionPreference = "Stop"

$TaskName = "MDDAP_ETL_Daily_Refresh"
$ScriptPath = Resolve-Path "$PSScriptRoot\..\orchestration\refresh_parallel.bat"
$ScriptPath = $ScriptPath.Path

Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  MDDAP Scheduled Task Configurator " -ForegroundColor Cyan
Write-Host "========================================================" 
Write-Host "Target Script: $ScriptPath"
Write-Host "Task Name:     $TaskName"

# 1. Define Action
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$ScriptPath`""

# 2. Define Trigger
# Daily at 08:20
$Trigger = New-ScheduledTaskTrigger -Daily -At "08:20"

# 3. Define Repetition
# Repeat every 120 minutes (2 hours), for 1 day duration
$Trigger.Repetition = $(New-ScheduledTaskTrigger -Once -At "00:00" -RepetitionInterval (New-TimeSpan -Minutes 120) -RepetitionDuration (New-TimeSpan -Days 1)).Repetition

# 4. Define Settings
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2)

# 5. Register/Update Task
try {
    Write-Host "Registering scheduled task..." -ForegroundColor Yellow
    
    # Register (Force update)
    # Note: Requires Administrator privileges
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -User "System" -Force
    
    Write-Host "SUCCESS: Scheduled task updated successfully!" -ForegroundColor Green
    Write-Host "Status: Daily at 08:20, repeats every 2 hours." -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Failed to register task!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "Hint: Please run this script as Administrator." -ForegroundColor Yellow
    exit 1
}
