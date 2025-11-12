@echo off
chcp 65001 >nul 2>&1
cls
echo ========================================
echo   MkDocs Documentation Server
echo ========================================
echo.
echo Starting documentation server...
echo.
echo Instructions:
echo   - After server starts, open browser and visit:
echo   - http://127.0.0.1:8000/
echo   - Browser will auto-refresh when files are modified
echo   - Press Ctrl+C to stop the server
echo.
echo ========================================
echo.

REM Check if virtual environment exists and activate it
if exist "..\.venv\Scripts\activate.bat" (
    call ..\.venv\Scripts\activate.bat
)

REM Start MkDocs server
mkdocs serve

if errorlevel 1 (
    echo.
    echo Error: Failed to start server
    echo Please check if MkDocs is installed and configured correctly
    pause
    exit /b 1
)

pause

