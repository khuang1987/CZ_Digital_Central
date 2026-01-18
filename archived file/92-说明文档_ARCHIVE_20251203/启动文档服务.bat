@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

:main_menu
cls
echo ========================================
echo   MkDocs Documentation Server
echo ========================================
echo.
echo 选择操作:
echo   [1] 启动文档服务 (普通模式)
echo   [2] 启动文档服务 (调试模式 - 支持快速重启)
echo   [3] 退出
echo.
echo ========================================
echo.
set /p choice="请输入选择 (1-3): "

if "%choice%"=="1" goto start_normal
if "%choice%"=="2" goto start_debug
if "%choice%"=="3" goto exit
echo 无效选择，请重新输入
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
    call ..\.venv\Scripts\activate.bat
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
echo 🐛 调试模式已启动
echo 📝 使用说明:
echo   - 服务启动后访问: http://127.0.0.1:8000/
echo   - 按 Ctrl+C 停止服务
echo   - 停止后可选择快速重启或退出
echo   - 修改文件后会自动刷新
echo.
echo ========================================
echo.

REM Check if virtual environment exists and activate it
if exist "..\.venv\Scripts\activate.bat" (
    call ..\.venv\Scripts\activate.bat
)

:debug_loop
echo 🚀 正在启动 MkDocs 服务...
echo.

REM Start MkDocs server
mkdocs serve

REM Server stopped, show restart menu
cls
echo ========================================
echo   服务已停止
echo ========================================
echo.
echo 快速操作选项:
echo   [R] 重新启动服务 (推荐)
echo   [Q] 退出到主菜单
echo   [X] 完全退出程序
echo.
echo ========================================
echo.

:restart_menu
set /p restart_choice="请输入选择 (R/Q/X): "

if /i "%restart_choice%"=="R" (
    echo.
    echo 🔄 正在重新启动服务...
    timeout /t 2 /nobreak >nul
    cls
    echo ========================================
    echo   MkDocs Documentation Server - Debug Mode
    echo ========================================
    echo.
    echo 🐛 调试模式 - 重新启动
    echo 📝 服务重启中...
    echo.
    echo ========================================
    echo.
    goto debug_loop
)

if /i "%restart_choice%"=="Q" (
    echo.
    echo 📋 返回主菜单...
    timeout /t 1 /nobreak >nul
    goto main_menu
)

if /i "%restart_choice%"=="X" (
    goto exit
)

echo 无效选择，请输入 R/Q/X
goto restart_menu

:exit
cls
echo ========================================
echo   感谢使用 MkDocs Documentation Server
echo ========================================
echo.
echo 程序已退出。再见！
echo.
timeout /t 2 /nobreak >nul
exit /b 0

