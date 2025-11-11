@echo off
chcp 65001 >nul
echo 正在转换标准时间表为Parquet格式...

cd /d "%~dp0"
python convert_standard_time.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo 转换完成！
    pause
) else (
    echo.
    echo 转换失败，请检查错误信息
    pause
)

