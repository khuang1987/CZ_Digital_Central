@echo off
chcp 65001 >nul
cd /d %~dp0
echo ========================================
echo SA指标数据清洗ETL - MES数据
echo ========================================
echo.
echo 注意：MES数据处理需要先运行SFC数据处理
echo 如果SFC数据文件不存在，Checkin_SFC字段将为空
echo.
echo 建议使用 run_all_etl.bat 按顺序运行完整流程
echo.
pause
echo.
python etl_sa.py
echo.
echo ========================================
echo 处理完成，按任意键退出...
pause >nul
