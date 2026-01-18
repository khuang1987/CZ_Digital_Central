param(
    [switch]$Execute
)

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

function Ensure-Dir([string]$Path) {
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Move-One([string]$Source, [string]$DestDir) {
    Ensure-Dir $DestDir
    $dest = Join-Path $DestDir (Split-Path $Source -Leaf)

    if (Test-Path $dest) {
        Write-Host "SKIP (exists): $Source -> $dest" -ForegroundColor Yellow
        return
    }

    if ($Execute) {
        Move-Item -LiteralPath $Source -Destination $destDir
        Write-Host "MOVED: $Source -> $dest" -ForegroundColor Green
    } else {
        Write-Host "DRYRUN: $Source -> $dest" -ForegroundColor Cyan
    }
}

# Keep list (do NOT move)
$keep = @(
    'refresh_core_data.bat',
    'refresh_all_data.bat',
    'scheduled_refresh.bat',
    'setup_scheduled_task.ps1',
    'export_core_to_a1.py',
    '_refresh_mes_metrics_materialized.py',
    '_execute_create_view_v2.py',
    'README_refresh_data.md',
    'README_scripts_organization.md',
    'organize_scripts.ps1'
)

$checksDir = Join-Path $ScriptDir 'checks'
$maintDir = Join-Path $ScriptDir 'maintenance'
$debugDir = Join-Path $ScriptDir 'debug'
$archiveDir = Join-Path $ScriptDir 'archive'
$archiveBat = Join-Path $archiveDir 'bat_deprecated'
$archiveSql = Join-Path $archiveDir 'sql_deprecated'
$archiveEmpty = Join-Path $archiveDir 'empty_files'
$archiveLegacy = Join-Path $archiveDir 'legacy'
$archiveDebug = Join-Path $archiveDir 'debug_and_verify'

$sqlDir = Join-Path $ScriptDir 'sql'
$mesSqlDir = Join-Path $sqlDir 'mes_metrics'

Ensure-Dir $checksDir
Ensure-Dir $maintDir
Ensure-Dir $debugDir
Ensure-Dir $archiveDir
Ensure-Dir $archiveBat
Ensure-Dir $archiveSql
Ensure-Dir $archiveEmpty
Ensure-Dir $archiveLegacy
Ensure-Dir $archiveDebug

Ensure-Dir $sqlDir
Ensure-Dir $mesSqlDir

$files = Get-ChildItem -LiteralPath $ScriptDir -File

foreach ($f in $files) {
    if ($keep -contains $f.Name) {
        continue
    }

    # mes_metrics sql scripts
    if ($f.Extension -ieq '.sql') {
        if (
            $f.Name -ieq '_init_mes_metrics_materialized.sql' -or
            $f.Name -ieq '_compute_mes_metrics_materialized.sql' -or
            $f.Name -ieq '_compute_mes_metrics_incremental.sql'
        ) {
            Move-One $f.FullName $mesSqlDir
            continue
        }
    }

    # useful connectivity check
    if ($f.Name -ieq '_test_sqlserver_connection.py') {
        Move-One $f.FullName $checksDir
        continue
    }

    # checks (read-only)
    if ($f.Name -ieq 'check_db_schema.py') {
        Move-One $f.FullName $checksDir
        continue
    }

    # debug scripts
    if ($f.Name -like 'debug_*.py') {
        Move-One $f.FullName $debugDir
        continue
    }

    # bat: deprecated refresh scripts
    if ($f.Extension -ieq '.bat') {
        Move-One $f.FullName $archiveBat
        continue
    }

    # sql: legacy view creation sql
    if ($f.Extension -ieq '.sql') {
        if ($f.Name -like '_create_mes_view_sqlserver*') {
            Move-One $f.FullName $archiveSql
            continue
        }
        # Other sql: keep in root unless you decide otherwise
        Move-One $f.FullName $archiveSql
        continue
    }

    # empty files
    if ($f.Length -eq 0) {
        Move-One $f.FullName $archiveEmpty
        continue
    }

    # checks (read-only)
    if ($f.Name -like '_check_*.py' -or $f.Name -ieq 'check_tables.py') {
        Move-One $f.FullName $checksDir
        continue
    }

    # maintenance (write/cleanup/migration-ish)
    if (
        $f.Name -like '_clear_*.py' -or
        $f.Name -like 'purge_*.py' -or
        $f.Name -like 'refresh_*.py' -or
        $f.Name -like '_delete_*.py' -or
        $f.Name -like '_remove_*.py' -or
        $f.Name -like '_update_*.py' -or
        $f.Name -like 'test_*.py'
    ) {
        Move-One $f.FullName $maintDir
        continue
    }

    # legacy/debug/verify/temporary
    if (
        $f.Name -like '_temp_*.py' -or
        $f.Name -like '_debug_*.py' -or
        $f.Name -like '_verify_*.py' -or
        $f.Name -like '_trace_*.py' -or
        $f.Name -like '_inspect_*.py' -or
        $f.Name -like '_list_*.py' -or
        $f.Name -like '_diagnose_*.py' -or
        $f.Name -like '_final_*'
    ) {
        Move-One $f.FullName $archiveDebug
        continue
    }

    # known legacy export
    if ($f.Name -ieq '_export_mes_to_parquet.py') {
        Move-One $f.FullName $archiveLegacy
        continue
    }

    # old view executor
    if ($f.Name -ieq '_execute_create_view.py') {
        Move-One $f.FullName $archiveLegacy
        continue
    }

    # default: archive legacy
    Move-One $f.FullName $archiveLegacy
}

Write-Host ''
if ($Execute) {
    Write-Host 'Done. Files were moved.' -ForegroundColor Green
} else {
    Write-Host 'Dry run complete. Re-run with -Execute to actually move files.' -ForegroundColor Cyan
}
