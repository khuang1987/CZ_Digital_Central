@echo off
chcp 65001 >nul
title SA指标ETL增量更新 - 无人值守模式

:: ========================================
:: SA指标数据清洗ETL一键启动脚本
:: 功能：增量更新SFC、SAP工艺路线、MES数据
:: 模式：默认增量刷新，无人值守运行
:: ========================================

:: 设置环境变量
set PYTHONIOENCODING=utf-8
set PYTHONPATH=%~dp0

:: 获取当前时间戳用于日志文件名
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YYYY=%dt:~0,4%"
set "MM=%dt:~4,2%"
set "DD=%dt:~6,2%"
set "HH=%dt:~8,2%"
set "MIN=%dt:~10,2%"
set "SS=%dt:~12,2%"
set "TIMESTAMP=%YYYY%%MM%%DD%_%HH%%MIN%%SS%"

:: 设置日志文件路径
set LOG_DIR=%~dp0..\06_日志文件
set LOG_FILE=%LOG_DIR%\etl_incremental_%TIMESTAMP%.log

:: 创建日志目录
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: 开始记录日志
echo ======================================== >> "%LOG_FILE%"
echo SA指标ETL增量更新开始 >> "%LOG_FILE%"
echo 开始时间: %date% %time% >> "%LOG_FILE%"
echo 模式: 增量刷新，无人值守运行 >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"

echo.
echo ========================================
echo SA指标ETL增量更新 - 无人值守模式
echo 开始时间: %date% %time%
echo 日志文件: %LOG_FILE%
echo ========================================
echo.

:: 设置错误计数器
set ERROR_COUNT=0

:: ========================================
:: 步骤1: SAP工艺路线数据更新
:: ========================================
echo [步骤1/3] 开始更新SAP工艺路线数据... >> "%LOG_FILE%"
echo [步骤1/3] 开始更新SAP工艺路线数据...

cd /d "%~dp0"
python etl_dataclean_sap_routing.py --mode incremental --unattended >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] SAP工艺路线数据更新完成 >> "%LOG_FILE%"
    echo [OK] SAP工艺路线数据更新完成
) else (
    echo [ERROR] SAP工艺路线数据更新失败! 错误代码: %ERRORLEVEL% >> "%LOG_FILE%"
    echo [ERROR] SAP工艺路线数据更新失败! 错误代码: %ERRORLEVEL%
    set /a ERROR_COUNT+=1
)
echo.

:: ========================================
:: 步骤2: SFC批次报告数据更新
:: ========================================
echo [步骤2/3] 开始更新SFC批次报告数据... >> "%LOG_FILE%"
echo [步骤2/3] 开始更新SFC批次报告数据...

cd /d "%~dp0"
python etl_dataclean_sfc_batch_report.py --mode incremental --unattended >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] SFC批次报告数据更新完成 >> "%LOG_FILE%"
    echo [OK] SFC批次报告数据更新完成
) else (
    echo [ERROR] SFC批次报告数据更新失败! 错误代码: %ERRORLEVEL% >> "%LOG_FILE%"
    echo [ERROR] SFC批次报告数据更新失败! 错误代码: %ERRORLEVEL%
    set /a ERROR_COUNT+=1
)
echo.

:: ========================================
:: 步骤3: MES批次报告数据更新
:: ========================================
echo [步骤3/3] 开始更新MES批次报告数据... >> "%LOG_FILE%"
echo [步骤3/3] 开始更新MES批次报告数据...

cd /d "%~dp0"
python etl_dataclean_mes_batch_report.py --mode incremental --unattended >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] MES批次报告数据更新完成 >> "%LOG_FILE%"
    echo [OK] MES批次报告数据更新完成
) else (
    echo [ERROR] MES批次报告数据更新失败! 错误代码: %ERRORLEVEL% >> "%LOG_FILE%"
    echo [ERROR] MES批次报告数据更新失败! 错误代码: %ERRORLEVEL%
    set /a ERROR_COUNT+=1
)
echo.

:: ========================================
:: 更新完成总结
:: ========================================
echo ======================================== >> "%LOG_FILE%"
echo ETL更新完成总结 >> "%LOG_FILE%"
echo 完成时间: %date% %time% >> "%LOG_FILE%"
echo 错误数量: %ERROR_COUNT% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"

echo.
echo ========================================
echo ETL更新完成总结
echo 完成时间: %date% %time%
echo 错误数量: %ERROR_COUNT%
echo 日志文件: %LOG_FILE%
echo ========================================

if %ERROR_COUNT% EQU 0 (
    echo [SUCCESS] 所有ETL更新成功完成!
    echo [SUCCESS] 所有ETL更新成功完成! >> "%LOG_FILE%"
) else (
    echo [WARNING] 共发生 %ERROR_COUNT% 个错误，请查看日志文件
    echo [WARNING] 共发生 %ERROR_COUNT% 个错误，请查看日志文件 >> "%LOG_FILE%"
)

:: ========================================
:: 自动打开日志文件（可选）
:: ========================================
echo.
echo 是否查看详细日志? (Y/N)
set /p VIEW_LOG=
if /i "%VIEW_LOG%"=="Y" (
    start notepad "%LOG_FILE%"
)

echo.
echo 按任意键退出...
pause >nul
