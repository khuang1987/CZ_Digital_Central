@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"
REM Use system Python; adjust if using venv
python -X utf8 "%~dp0etl.py"
set ERR=%ERRORLEVEL%
if %ERR% NEQ 0 (
  echo ETL failed with code %ERR%
  exit /b %ERR%
)
echo ETL completed successfully
exit /b 0
