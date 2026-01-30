@echo off
chcp 65001 >nul
:: Switch to Project Root (from scripts/orchestration/ -> scripts/ -> root)
cd /d "%~dp0..\.."

:: Env Detection
set "PYTHON_CMD=python"
if exist ".venv_%USERNAME%\Scripts\python.exe" (
    set "PYTHON_CMD=.venv_%USERNAME%\Scripts\python.exe"
    echo Using Client Env: .venv_%USERNAME%
) else if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
    echo Using Shared Env: .venv
)

"%PYTHON_CMD%" "scripts\orchestration\run_etl_parallel.py" %*

pause
