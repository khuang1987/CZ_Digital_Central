@echo off
chcp 65001 >nul
:: Switch to Project Root (from scripts/orchestration/ -> scripts/ -> root)
cd /d "%~dp0..\.."

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" "scripts\orchestration\run_etl_parallel.py"
) else (
    python "scripts\orchestration\run_etl_parallel.py"
)

pause
