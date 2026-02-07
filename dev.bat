@echo off
echo ===================================================
echo   CZ Digital Central - Development Launcher
echo ===================================================

:: 1. Start Backend API in a new window
echo [1/2] Starting Backend API (FastAPI)...
start "CZ Backend API" cmd /k "cd apps\backend_api && ..\..\.venv\Scripts\python -m uvicorn main:app --reload --port 8000"

:: 2. Start Web Dashboard in current window
echo [2/2] Starting Web Dashboard (Next.js)...
cd apps\web_dashboard
npm run dev
