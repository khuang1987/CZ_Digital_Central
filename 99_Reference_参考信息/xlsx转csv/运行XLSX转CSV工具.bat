@echo off
chcp 65001 >nul
title XLSX转CSV工具

echo 正在启动XLSX转CSV工具...
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误：未找到Python，请先安装Python 3.7或更高版本
    echo 下载地址：https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 检查依赖包是否安装
echo 检查依赖包...
python -c "import pandas, openpyxl, xlrd" >nul 2>&1
if errorlevel 1 (
    echo 正在安装依赖包...
    pip install pandas openpyxl xlrd
    if errorlevel 1 (
        echo 错误：依赖包安装失败，请检查网络连接或手动安装
        pause
        exit /b 1
    )
)

echo 启动程序...
python xlsx_to_csv_converter.py

if errorlevel 1 (
    echo.
    echo 程序运行出错，请检查错误信息
    pause
) 