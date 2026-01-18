@echo off
setlocal EnableDelayedExpansion

set "LOG_FILE=test.log"
echo start > "%LOG_FILE%"

echo Testing choice...
echo N | choice /C YN /T 2 /D Y /M "Test Choice"
if errorlevel 2 echo Choice N detected
if errorlevel 1 echo Choice Y detected

echo Testing variable with parentheses...
set "CMD=echo (test)"
call :RunStep 1 "Test Step" "!CMD!"

echo Done.
exit /b 0

:RunStep
set "S_CMD=%~3"
echo Running: !S_CMD!
!S_CMD! >> "%LOG_FILE%" 2>&1
exit /b 0
