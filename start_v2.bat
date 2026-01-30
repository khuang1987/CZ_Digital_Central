@echo off
title CZ Digital Central Launcher
chcp 65001 >nul

echo ===================================================
echo    Changzhou Campus Digital Central
echo ===================================================
echo.

:: Start Backend API
start "MDDAP Backend" cmd /k "cd apps\backend_api && .venv\Scripts\python main.py"

:: Start Frontend Dashboard
start "MDDAP Frontend" cmd /k "cd apps\web_dashboard && npm run dev"

echo ✨ Servers are starting...
echo ---------------------------------------------------
echo 🔗 Backend API:   http://localhost:8000
echo 🔗 Dashboard:     http://localhost:3000
echo ---------------------------------------------------
echo.
pause
