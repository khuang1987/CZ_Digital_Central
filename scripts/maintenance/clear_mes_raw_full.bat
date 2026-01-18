@echo off
setlocal

REM One-click clear: dbo.raw_mes + dbo.etl_file_state (mes_raw_*)

for %%I in ("%~dp0..\..") do set "ROOT=%%~fI"
set "SCRIPT=%~dp0_clear_sqlserver_mes.py"

set "PY=python"
if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"

echo ===============================================================================
echo Project root: %ROOT%
echo Python: %PY%
echo Script: %SCRIPT%
echo ===============================================================================
echo.
echo This will CLEAR SQL Server tables:
echo   - dbo.raw_mes
echo   - dbo.etl_file_state (etl_name like 'mes_raw_%%')
echo.
echo The Python script will ask you to type YES to continue.
echo.

"%PY%" "%SCRIPT%"
set "EC=%ERRORLEVEL%"

if not "%EC%"=="0" (
echo.
echo ERROR: clear failed with exit code %EC%
echo.
pause
exit /b %EC%
)

echo.
echo SUCCESS: MES raw tables cleared.
echo Next step (run manually):
echo   python "%ROOT%\data_pipelines\sources\mes\etl\etl_mes_batch_output_raw.py"
echo.
pause
exit /b 0
