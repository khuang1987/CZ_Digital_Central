@echo off
cd /d "%~dp0"

echo ========================================================
echo  MDDAP 计划任务一键安装工具
echo ========================================================
echo 正在请求管理员权限以更新计划任务...

:: 使用 PowerShell 将自身作为管理员重新运行 (RunAs)
:: 如果已经是管理员，则直接执行 setup_mddap_task.ps1

powershell -Command "Start-Process powershell -ArgumentList '-NoProfile -ExecutionPolicy Bypass -File ""%~dp0setup_mddap_task.ps1""' -Verb RunAs"

echo.
echo 已尝试启动 PowerShell 安装脚本...
echo 如果没有弹出窗口，请检查是否被拦截，或手动右键本文件 -> 以管理员身份运行。
echo.
pause
