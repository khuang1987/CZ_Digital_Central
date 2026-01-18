@echo off
echo Starting MDDAP Dashboard...
echo.
set "SCRIPT_DIR=%~dp0"
streamlit run "%SCRIPT_DIR%app.py"
pause
