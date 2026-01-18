# Windows Task Scheduler Setup Script
# Creates a daily scheduled task for ETL data refresh

param(
    [string]$TaskName = "MDDAP_ETL_Daily_Refresh",
    [string]$TriggerTime = "08:30",
    [switch]$Remove
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# scripts/deploy -> scripts -> project_root
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
$BatchScript = Join-Path $ProjectRoot "scripts\orchestration\refresh_parallel.bat"

Write-Host "============================================================"
Write-Host "MDDAP ETL Scheduled Task Setup"
Write-Host "============================================================"

if ($Remove) {
    Write-Host "Removing scheduled task: $TaskName"
    try {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
        Write-Host "Task removed successfully" -ForegroundColor Green
    }
    catch {
        Write-Host "Failed to remove or task not found: $_" -ForegroundColor Yellow
    }
    exit 0
}

if (-not (Test-Path $BatchScript)) {
    Write-Host "Error: Batch script not found: $BatchScript" -ForegroundColor Red
    exit 1
}

Write-Host "Project Root: $ProjectRoot"
Write-Host "Batch Script: $BatchScript"
Write-Host "Task Name: $TaskName"
Write-Host "Trigger Time: Daily at $TriggerTime"
Write-Host ""

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Found existing task, removing first..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$trigger = New-ScheduledTaskTrigger -Daily -At $TriggerTime
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatchScript`"" -WorkingDirectory $ProjectRoot
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RunOnlyIfNetworkAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

try {
    Register-ScheduledTask -TaskName $TaskName -Trigger $trigger -Action $action -Settings $settings -Principal $principal -Description "MDDAP ETL Daily Refresh" -ErrorAction Stop

    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "Scheduled task created successfully!" -ForegroundColor Green
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:"
    Write-Host "  - Name: $TaskName"
    Write-Host "  - Trigger: Daily at $TriggerTime"
    Write-Host "  - Script: $BatchScript"
    Write-Host "  - Log: $ProjectRoot\shared_infrastructure\logs\core_refresh_*.log"
    Write-Host ""
    Write-Host "Management Commands:"
    Write-Host "  - View: Get-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  - Run: Start-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  - Remove: .\setup_scheduled_task.ps1 -Remove"
    Write-Host ""
}
catch {
    Write-Host "Failed to create scheduled task: $_" -ForegroundColor Red
    exit 1
}
