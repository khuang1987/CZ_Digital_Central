@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

:: ============================================================
:: Full Refresh (SQL Server Only)
:: - Includes low-frequency dimensions (calendar) and planning (planner)
:: - Suitable for manual run (weekly/monthly) or ad-hoc
:: ============================================================

:: 设置日志文件
set "LOG_DIR=%~dp0..\shared_infrastructure\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "DATE_STR=%%i"

if not defined LOG_PREFIX set "LOG_PREFIX=all_refresh"
set "LOG_FILE=%LOG_DIR%\%LOG_PREFIX%_%DATE_STR%.log"

:: 确保日志文件是 UTF-8 BOM（避免中文乱码）
if not exist "%LOG_FILE%" (
    powershell -NoProfile -Command "'' | Out-File -FilePath '%LOG_FILE%' -Encoding utf8" >nul
)

:: 记录开始时间
echo ============================================================ >> "%LOG_FILE%"
echo [%date% %time%] Full refresh started >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo ============================================================
echo [%date% %time%] Full refresh started
echo 日志: "%LOG_FILE%"
echo ============================================================

:: 切换到项目目录
cd /d "%~dp0.."
set "PROJECT_ROOT=%cd%"
echo [%date% %time%] 项目目录: %PROJECT_ROOT% >> "%LOG_FILE%"

:: 设置 PYTHONPATH（确保 shared_infrastructure 等绝对导入可用）
set "PYTHONPATH=%PROJECT_ROOT%"
echo [%date% %time%] PYTHONPATH: %PYTHONPATH% >> "%LOG_FILE%"
echo [%date% %time%] PYTHONUTF8: %PYTHONUTF8% >> "%LOG_FILE%"
echo [%date% %time%] PYTHONIOENCODING: %PYTHONIOENCODING% >> "%LOG_FILE%"

:: 激活虚拟环境
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
    echo [%date% %time%] 虚拟环境已激活 >> "%LOG_FILE%"
) else (
    echo [%date% %time%] 警告: 虚拟环境不存在，使用系统Python >> "%LOG_FILE%"
)

:: 固定使用虚拟环境的 python.exe（任务计划更稳定）
set "PYTHON_EXE=python"
if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=.venv\Scripts\python.exe"
)
echo [%date% %time%] Python: %PYTHON_EXE% >> "%LOG_FILE%"

:: 初始化计数器
set SUCCESS_COUNT=0
set FAIL_COUNT=0
set TOTAL_COUNT=16

:: ============================================================
:: 1/16 - SAP工艺路线原始数据 (SQL Server ODS)
:: ============================================================
echo [%date% %time%] 开始刷新 SAP 工艺路线数据... >> "%LOG_FILE%"
echo [1/16] SAP 工艺路线原始数据
"%PYTHON_EXE%" data_pipelines\sources\sap\etl\etl_sap_routing_raw.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SAP 工艺路线数据刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SAP 工艺路线数据刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 2/16 - SFC批次报工原始数据 (SQL Server ODS)
:: ============================================================
echo [%date% %time%] 开始刷新 SFC 批量报告数据... >> "%LOG_FILE%"
echo [2/16] SFC 批次报工原始数据
"%PYTHON_EXE%" data_pipelines\sources\sfc\etl\etl_sfc_batch_output_raw.py --max-new-files 22 >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SFC 批量报告刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SFC 批量报告刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 3/16 - SFC产品检验原始数据 (SQL Server ODS)
:: ============================================================
echo [%date% %time%] 开始刷新 SFC 产品检验原始数据... >> "%LOG_FILE%"
echo [3/16] SFC 产品检验原始数据
"%PYTHON_EXE%" data_pipelines\sources\sfc\etl\etl_sfc_inspection_raw.py --max-new-files 22 >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SFC 产品检验刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SFC 产品检验刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 4/16 - MES批次报工原始数据 (SQL Server ODS)
:: ============================================================
echo [%date% %time%] 开始刷新 MES 批量报告数据... >> "%LOG_FILE%"
echo [4/16] MES 批次报工原始数据
"%PYTHON_EXE%" data_pipelines\sources\mes\etl\etl_mes_batch_output_raw.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] MES 批量报告刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] MES 批量报告刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 5/16 - Planner任务数据 (SQL Server)
:: ============================================================
echo [%date% %time%] 开始刷新 Planner 任务数据... >> "%LOG_FILE%"
echo [5/16] Planner 任务数据
"%PYTHON_EXE%" data_pipelines\sources\planner\etl\etl_planner_tasks_raw.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] Planner 任务数据刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] Planner 任务数据刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 6/16 - Calendar维度 (SQL Server)
:: ============================================================
echo [%date% %time%] 开始刷新 Calendar 维度... >> "%LOG_FILE%"
echo [6/16] Calendar 维度
"%PYTHON_EXE%" data_pipelines\sources\dimension\etl\etl_calendar.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] Calendar 维度刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] Calendar 维度刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 7/16 - 工序标准分类（维表）
:: ============================================================
echo [%date% %time%] 开始导入 工序标准分类（仅工序名称工作表）... >> "%LOG_FILE%"
echo [7/16] 工序标准分类.xlsx
"%PYTHON_EXE%" data_pipelines\sources\dimension\etl\etl_operation_mapping.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] 工序标准分类导入成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] 工序标准分类导入失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 8/16 - SFC WIP (CZM)
:: ============================================================
echo [%date% %time%] 开始刷新 SFC WIP (CZM)... >> "%LOG_FILE%"
echo [8/16] SFC WIP CZM
"%PYTHON_EXE%" data_pipelines\sources\sfc\etl\etl_sfc_wip_czm.py --mode latest >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SFC WIP CZM 刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SFC WIP CZM 刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 9/16 - CMES WIP (CKH/others)
:: ============================================================
echo [%date% %time%] 开始刷新 CMES WIP（最近7天）... >> "%LOG_FILE%"
echo [9/16] CMES WIP
"%PYTHON_EXE%" data_pipelines\sources\mes\etl\etl_mes_wip_cmes.py --days 7 >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] CMES WIP 刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] CMES WIP 刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 10/16 - SFC Repair
:: ============================================================
echo [%date% %time%] 开始刷新 SFC Repair... >> "%LOG_FILE%"
echo [10/16] SFC Repair
"%PYTHON_EXE%" data_pipelines\sources\sfc\etl\etl_sfc_repair.py --mode latest >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SFC Repair 刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SFC Repair 刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 11/16 - SAP Labor Hours
:: ============================================================
echo [%date% %time%] 开始刷新 SAP Labor Hours... >> "%LOG_FILE%"
echo [11/16] SAP Labor Hours
"%PYTHON_EXE%" data_pipelines\sources\sap\etl\etl_sap_labor_hours.py --ypp >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SAP Labor Hours 刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SAP Labor Hours 刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 12/16 - SAP 9997 发料记录（Sheet1, A:T）
:: ============================================================
echo [%date% %time%] 开始刷新 SAP 9997 发料记录... >> "%LOG_FILE%"
echo [12/16] SAP 9997 发料记录
"%PYTHON_EXE%" data_pipelines\sources\sap\etl\etl_sap_gi_9997.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SAP 9997 发料记录刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SAP 9997 发料记录刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: KPI监控与触发结果（trigger result）
:: ============================================================
echo [%date% %time%] (Disabled) Skipping KPI trigger results... >> "%LOG_FILE%"

:: ============================================================
:: 13/16 - 刷新 MES 指标物化快照（BI 秒开）
:: ============================================================
echo [%date% %time%] 开始刷新 MES 指标物化快照... >> "%LOG_FILE%"
echo [13/16] 刷新 MES 指标物化快照
"%PYTHON_EXE%" scripts\_refresh_mes_metrics_materialized.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] MES 指标物化快照刷新成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] MES 指标物化快照刷新失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 14/16 - 导出分区/静态Parquet到A1_ETL_Output
:: ============================================================
echo [%date% %time%] 开始导出分区/静态Parquet到A1_ETL_Output... >> "%LOG_FILE%"
echo [14/16] 导出分区/静态Parquet到A1_ETL_Output
"%PYTHON_EXE%" scripts\export_core_to_a1.py --mode partitioned --reconcile --reconcile-last-n 2 --meta-store sql >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] 导出分区/静态Parquet成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] 导出分区/静态Parquet失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 15/16 - SQL Server 写入完整性检查（关键表行数）
:: ============================================================
echo [%date% %time%] 开始 SQL Server 写入完整性检查... >> "%LOG_FILE%"
echo [15/16] SQL Server 写入完整性检查
"%PYTHON_EXE%" scripts\validation\sqlserver_postcheck.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] SQL Server 写入完整性检查完成 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] SQL Server 写入完整性检查失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 16/16 - meta 汇总（Dashboard 大屏用：表统计 + 数据质量）
:: ============================================================
echo [%date% %time%] 开始生成 meta 汇总（表统计 + 数据质量）... >> "%LOG_FILE%"
echo [16/16] meta 汇总（Dashboard）
"%PYTHON_EXE%" data_pipelines\monitoring\etl\etl_meta_table_health.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [%date% %time%] meta 汇总生成成功 >> "%LOG_FILE%"
    echo 结果: 成功
    set /a SUCCESS_COUNT+=1
) else (
    echo [%date% %time%] meta 汇总生成失败 [错误码: !ERRORLEVEL!] >> "%LOG_FILE%"
    echo 结果: 失败 (错误码: !ERRORLEVEL!)
    set /a FAIL_COUNT+=1
)

:: ============================================================
:: 执行结果汇总
:: ============================================================

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo [%date% %time%] Full refresh finished >> "%LOG_FILE%"
echo 成功: !SUCCESS_COUNT!/!TOTAL_COUNT! >> "%LOG_FILE%"
echo 失败: !FAIL_COUNT!/!TOTAL_COUNT! >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo.
echo ============================================================
echo [%date% %time%] Full refresh finished
echo 成功: !SUCCESS_COUNT!/!TOTAL_COUNT!
echo 失败: !FAIL_COUNT!/!TOTAL_COUNT!
echo 日志: "%LOG_FILE%"
echo ============================================================

:: 如果有失败，返回错误码
if !FAIL_COUNT! GTR 0 (
    exit /b 1
) else (
    exit /b 0
)
