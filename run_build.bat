@echo off
setlocal
title CZ Digital Central Packaging Tool

echo ============================================================
echo      Changzhou Campus Digital Central Packaging Controller
echo ============================================================
echo.
echo [1] Full     - Full build (Python wheels, Frontend, Logic, PM2)
echo [2] Frontend - Frontend build (UI assets + Backend logic)
echo [3] Logic    - Logic build (Python/API code only - FASTEST)
echo.

:: Use choice command with 10s timeout, default to 3 (Logic)
choice /C 123 /T 10 /D 3 /M "Select build mode (Default Logic in 10s): "

if errorlevel 3 goto :LOGIC
if errorlevel 2 goto :FRONTEND
if errorlevel 1 goto :FULL

:FULL
set BUILD_MODE=Full
goto :RUN

:FRONTEND
set BUILD_MODE=Frontend
goto :RUN

:LOGIC
set BUILD_MODE=Logic
goto :RUN

:RUN
echo.
echo Executing %BUILD_MODE% build for stable folder: deploy_server_CCDC...
echo ------------------------------------------------------------

powershell.exe -ExecutionPolicy Bypass -File ".\scripts\maintenance\build_offline_package.ps1" ^
    -Mode %BUILD_MODE% ^
    -OutputDir "c:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\server_packages" ^
    -PackageName "deploy_server_CCDC"

echo.
echo ============================================================
echo DONE: Files synced to OneDrive transfer directory.
pause
