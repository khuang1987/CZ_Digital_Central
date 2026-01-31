@echo off
title MDDAP Services Launcher
color 0A

echo ========================================================
echo   MDDAP Integrated Platform Services
echo ========================================================
echo.

echo [0/2] Detecting Environment...
set "VENV_DIR=.venv"
if exist ".venv_%USERNAME%" (
    set "VENV_DIR=.venv_%USERNAME%"
    echo   - Using User Env: .venv_%USERNAME%
) else (
    if exist ".venv" (
        echo   - Using Shared Env: .venv
    ) else (
        echo   - Warning: No VENV found, trying Global Python...
        set "VENV_DIR="
    )
)

echo [1/2] Starting Documentation Service (Port 8000)...
if defined VENV_DIR (
    start "MDDAP Docs" cmd /c ""%VENV_DIR%\Scripts\mkdocs" serve -a 0.0.0.0:8000"
) else (
    start "MDDAP Docs" cmd /c "mkdocs serve -a 0.0.0.0:8000"
)

echo [2/2] (Skip) Dashboard Service has been migrated to Next.js.
REM set "SCRIPT_DIR=%~dp0dashboard\"
REM cd /d "%SCRIPT_DIR%"
REM if defined VENV_DIR (
REM     start "MDDAP Dashboard" cmd /c "..\%VENV_DIR%\Scripts\streamlit" run app.py"
REM ) else (
REM     start "MDDAP Dashboard" cmd /c "streamlit run app.py"
REM )

echo.
echo ========================================================
echo   All services started!
echo ========================================================
echo.
echo [Local Access]
echo   - Docs:      http://localhost:8000
echo   - Dashboard: http://localhost:3000
echo.

:: Show all valid network IPs
python "%~dp0scripts\show_urls.py"

echo ========================================================
echo.
echo [Troubleshooting Mobile Connection]
echo If your phone cannot connect, please check:
echo 1. Phone and PC must be on the SAME Wi-Fi network.
echo 2. Windows Firewall might be blocking ports 8000/8501.
echo    To fix Firewall, run PowerShell as Administrator:
echo    New-NetFirewallRule -DisplayName "MDDAP Docs" -Direction Inbound -LocalPort 8000 -Protocol TCP -Action Allow
echo    New-NetFirewallRule -DisplayName "MDDAP Dash" -Direction Inbound -LocalPort 8501 -Protocol TCP -Action Allow
echo.
echo ========================================================
echo Close this window to keep services running in background.
echo To stop services, close the popped-up command windows.
pause
