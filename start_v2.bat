@echo off
title CZ Digital Central Launcher
chcp 65001 >nul

echo ===================================================
echo    Changzhou Campus Digital Central
echo ===================================================
echo.

:: Start Backend API
start "CZ Digital Central Backend" cmd /k "cd apps\backend_api && .venv\Scripts\python main.py"

:: Start Frontend Dashboard
start "CZ Digital Central Frontend" cmd /k "cd apps\web_dashboard && npm run dev"

echo ✨ Servers are starting...
echo ---------------------------------------------------
echo 🔗 Backend API:   http://localhost:8000
echo 🔗 Dashboard:     http://localhost:3000
echo ---------------------------------------------------
echo.
pause
