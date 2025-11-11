@echo off
chcp 65001 >nul
cd /d %~dp0
echo ========================================
echo SA指标数据清洗ETL - 完整流程
echo ========================================
echo.
echo 步骤1: 处理SFC数据
echo ========================================
python etl_sfc.py
if %errorlevel% neq 0 (
    echo SFC数据处理失败，退出
    pause >nul
    exit /b %errorlevel%
)
echo.
echo 步骤2: 处理MES数据（需要SFC数据）
echo ========================================
python etl_sa.py
if %errorlevel% neq 0 (
    echo MES数据处理失败，退出
    pause >nul
    exit /b %errorlevel%
)
echo.
echo ========================================
echo 所有处理完成！
echo ========================================
pause >nul

