@echo off
chcp 65001 >nul
title 每日4数据源顺序刷新

echo ========================================
echo 每日4数据源顺序刷新任务
echo 处理顺序: 日历 → SAP → SFC → MES
echo 开始时间: %date% %time%
echo ========================================

cd /d "%~dp0"

python smart_refresh_scheduler.py --mode immediate

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ 4数据源顺序刷新完成！
) else (
    echo.
    echo ❌ 4数据源顺序刷新失败！
)

echo 结束时间: %date% %time%
echo ========================================
pause
