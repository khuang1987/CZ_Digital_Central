@echo off
color 0A
echo ========================================================
echo   Changzhou Campus Digital Central Migration Setup Helper
echo ========================================================
echo.
echo [1/3] Installing Web Dashboard Dependencies...
cd apps\web_dashboard
call npm install
cd ..\..

echo.
echo [2/3] Creating Python Virtual Environment (.venv)...
python -m venv .venv
echo.
echo [3/3] Installing Python Requirements...
call .venv\Scripts\activate
pip install -r requirements.txt

echo.
echo ========================================================
echo   Setup Complete!
echo   You can now run 'start_services.bat' to launch the app.
echo ========================================================
pause
