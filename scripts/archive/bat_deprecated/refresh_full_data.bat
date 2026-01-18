@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

cd /d "%~dp0.."

echo.
echo ================================================================
echo      MDDAP Data Platform - Full Data Refresh (SQLite)
echo ================================================================
echo   Output: data_pipelines\database\mddap_v2.db
echo ================================================================
echo.

echo --- System Check ---
echo ----------------------------------------------------------------
if exist ".venv\Scripts\activate.bat" (
    echo [OK] Virtual env detected
    set ENV_STATUS=Production
) else (
    echo [!] No virtual env, using system Python
    set ENV_STATUS=Development
)

echo [OK] Mode: %ENV_STATUS%
echo [OK] Directory: %CD%
echo.

echo.
echo SQLite Data Sources (12 total)
echo ----------------------------------------------------------------
echo   Dimension: Calendar, Holidays, Operation Mapping
echo   SAP: Routing, Labor Hours, Planned Hours
echo   SFC: Batch Output, WIP, Repair, NC
echo   MES: Batch Output
echo.

echo.
echo ================================================================
echo   Starting SQLite Data Refresh
echo ================================================================
echo.

set START_TIME=%TIME%
echo Start Time: %START_TIME%
echo.

if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
    echo [OK] Virtual env activated
    echo.
)

set ERROR_COUNT=0
set SUCCESS_COUNT=0
set TOTAL_COUNT=15

echo ================================================================
echo [Dimension] 1/15 - Calendar
echo ================================================================
python data_pipelines\sources\dimension\etl\etl_calendar.py
if errorlevel 1 (
    echo [X] Calendar refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] Calendar refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [Dimension] 2/12 - Holidays
echo ================================================================
python data_pipelines\sources\dimension\etl\etl_holidays.py
if errorlevel 1 (
    echo [X] Holidays refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] Holidays refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [Dimension] 3/12 - Operation Mapping
echo ================================================================
python data_pipelines\sources\dimension\etl\etl_operation_mapping.py
if errorlevel 1 (
    echo [X] Operation Mapping refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] Operation Mapping refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SAP] 4/12 - SAP Routing
echo ================================================================
python data_pipelines\sources\sap\etl\etl_sap_routing_raw.py
if errorlevel 1 (
    echo [X] SAP Routing refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SAP Routing refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SAP] 5/12 - SAP Labor Hours (Incremental)
echo ================================================================
python data_pipelines\sources\sap\etl\etl_sap_labor_hours.py --ypp
if errorlevel 1 (
    echo [X] SAP Labor Hours refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SAP Labor Hours refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SAP] 6/12 - Planned Labor Hours
echo ================================================================
python data_pipelines\sources\sap\etl\etl_planned_labor_hours.py
if errorlevel 1 (
    echo [X] Planned Labor Hours refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] Planned Labor Hours refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SFC] 7/12 - SFC Batch Output
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_batch_output_raw.py
if errorlevel 1 (
    echo [X] SFC Batch Output refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SFC Batch Output refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SFC] 8/12 - SFC WIP (All Files)
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_wip_czm.py --mode all
if errorlevel 1 (
    echo [X] SFC WIP refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SFC WIP refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SFC] 9/12 - SFC Repair (All Files)
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_repair.py --mode all
if errorlevel 1 (
    echo [X] SFC Repair refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SFC Repair refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SFC] 10/15 - SFC NC (All Files)
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_nc.py --mode all
if errorlevel 1 (
    echo [X] SFC NC refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SFC NC refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SFC] 11/15 - SFC Product Inspection
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_inspection_raw.py
if errorlevel 1 (
    echo [X] SFC Product Inspection refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] SFC Product Inspection refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [MES] 12/15 - MES Batch Output
echo ================================================================
python data_pipelines\sources\mes\etl\etl_mes_batch_output_raw.py
if errorlevel 1 (
    echo [X] MES Batch Output refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] MES Batch Output refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [SAP] 13/15 - Update Planned Workday Flags
echo ================================================================
python data_pipelines\sources\sap\etl\etl_update_planned_workday.py
if errorlevel 1 (
    echo [X] Planned Workday Flags refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] Planned Workday Flags refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [Planner] 14/15 - Planner Tasks & Labels
echo ================================================================
python data_pipelines\sources\planner\etl\etl_planner_tasks_raw.py
if errorlevel 1 (
    echo [X] Planner Tasks refresh failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] Planner Tasks refresh success
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo [Monitoring] 15/15 - KPI Data Aggregation & Trigger Engine
echo ================================================================
python data_pipelines\monitoring\etl\etl_kpi_aggregation.py
if errorlevel 1 (
    echo [X] KPI Aggregation failed
    set /a ERROR_COUNT+=1
) else (
    echo [OK] KPI Aggregation success
    set /a SUCCESS_COUNT+=1
)
echo.

set END_TIME=%TIME%

echo.
echo ================================================================
echo                      Refresh Summary
echo ================================================================
echo.
echo   Start Time: %START_TIME%
echo   End Time: %END_TIME%
echo.
echo   Success: %SUCCESS_COUNT% / %TOTAL_COUNT%
if %ERROR_COUNT% GTR 0 (
    echo   Failed: %ERROR_COUNT%
)
echo.

if %ERROR_COUNT% EQU 0 (
    echo ================================================================
    echo              [OK] All SQLite data refreshed!
    echo ================================================================
    echo.
    echo   Database: data_pipelines\database\mddap_v2.db
    echo.
) else (
    echo ================================================================
    echo          [!] Some data sources failed
    echo ================================================================
    echo.
)

echo ================================================================
echo Press any key to exit...
pause >nul
