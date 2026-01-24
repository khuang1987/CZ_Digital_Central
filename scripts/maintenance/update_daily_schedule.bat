@echo off
setlocal

:: 设置变量
set TASK_NAME=MDDAP_ETL_Daily_Refresh
set SCRIPT_PATH=%~dp0..\..\scripts\orchestration\refresh_parallel.bat
set START_TIME=08:20
set REPEAT_INTERVAL=120
set DURATION=23:59

:: 获取脚本绝对路径
for %%i in ("%SCRIPT_PATH%") do set ABS_SCRIPT_PATH=%%~fi

echo ========================================================
echo 正在更新计划任务: %TASK_NAME%
echo --------------------------------------------------------
echo 脚本路径: %ABS_SCRIPT_PATH%
echo 开始时间: %START_TIME%
echo 重复间隔: 每 %REPEAT_INTERVAL% 分钟 (2小时)
echo 持续时间: %DURATION% (全天)
echo ========================================================

:: 检查脚本是否存在
if not exist "%ABS_SCRIPT_PATH%" (
    echo [错误] 找不到目标脚本: %ABS_SCRIPT_PATH%
    pause
    exit /b 1
)

:: 创建/更新计划任务
:: /F: 强制覆盖
:: /SC DAILY: 每天运行
:: /ST %START_TIME%: 开始时间
:: /RI %REPEAT_INTERVAL%: 重复间隔(分钟)
:: /DU %DURATION%: 持续时间
:: /TR ...: 运行的操作 (这里使用 cmd /c 来运行 bat 以避免一些环境问题，并引用路径)

schtasks /Create /F ^
    /TN "%TASK_NAME%" ^
    /TR "cmd /c \"'%ABS_SCRIPT_PATH%'\"" ^
    /SC DAILY ^
    /ST %START_TIME% ^
    /RI %REPEAT_INTERVAL% ^
    /DU %DURATION%

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [成功] 计划任务已更新！
    echo 下次运行时间: 今天 %START_TIME% (或明天，如果今天已过)
) else (
    echo.
    echo [失败] 无法创建计划任务。请尝试以【管理员身份】运行此脚本。
)

pause
