@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

:main_menu
cls
echo ========================================
echo   MkDocs Documentation Server
echo ========================================
echo.
echo Select operation:
echo   [1] Start server (Normal mode)
echo   [2] Start server (Debug mode - Quick restart)
echo   [3] Exit
echo.
echo ========================================
echo.
set /p choice="Please enter choice (1-3): "

if "%choice%"=="1" goto start_normal
if "%choice%"=="2" goto start_debug
if "%choice%"=="3" goto exit
echo Invalid choice, please try again
pause
goto main_menu

:start_normal
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
    call "..\.venv\Scripts\activate.bat"
)

REM Start MkDocs server
mkdocs serve

if errorlevel 1 (
    echo.
    echo Error: Failed to start server
    echo Please check if MkDocs is installed and configured correctly
    pause
    goto main_menu
)

echo.
echo Server stopped. Press any key to return to menu...
pause >nul
goto main_menu

:start_debug
cls
echo ========================================
echo   MkDocs Documentation Server - Debug Mode
echo ========================================
echo.
echo Debug mode started
echo Usage instructions:
echo   - Visit: http://127.0.0.1:8000/ after server starts
echo   - Press Ctrl+C to stop server
echo   - After stopping, choose quick restart or exit
echo   - Files will auto-refresh when modified
echo.
echo ========================================
echo.

REM Check if virtual environment exists and activate it
if exist "..\.venv\Scripts\activate.bat" (
    call "..\.venv\Scripts\activate.bat"
)

:debug_loop
echo Starting MkDocs server...
echo.

REM Start MkDocs server
mkdocs serve

REM Server stopped, show restart menu
cls
echo ========================================
echo   Server stopped
echo ========================================
echo.
echo Quick operation options:
echo   [R] Restart server (Recommended)
echo   [Q] Return to main menu
echo   [X] Exit completely
echo.
echo ========================================
echo.

:restart_menu
set /p restart_choice="Please enter choice (R/Q/X): "

if /i "%restart_choice%"=="R" (
    echo.
    echo Restarting server...
    timeout /t 2 /nobreak >nul
    cls
    echo ========================================
    echo   MkDocs Documentation Server - Debug Mode
    echo ========================================
    echo.
    echo Debug mode - Restarting
    echo Server restarting...
    echo.
    echo ========================================
    echo.
    goto debug_loop
)

if /i "%restart_choice%"=="Q" (
    echo.
    echo Returning to main menu...
    timeout /t 1 /nobreak >nul
    goto main_menu
)

if /i "%restart_choice%"=="X" (
    goto exit
)

echo Invalid choice, please enter R/Q/X
goto restart_menu

:exit
cls
echo ========================================
echo   Thank you for using MkDocs Documentation Server
echo ========================================
echo.
echo Program exited. Goodbye!
echo.
timeout /t 2 /nobreak >nul
exit /b 0
