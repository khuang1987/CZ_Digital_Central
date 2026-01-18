@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
echo.
echo ========================================
echo   SA数据清洗 - 快速导航菜单
echo ========================================
echo.
echo 请选择要打开的文件夹:
echo.
echo 1. 核心ETL程序    - 主要的数据处理程序
echo 2. 测试验证脚本    - 测试和验证脚本
echo 3. 配置文件        - 配置文件和依赖
echo 4. 报告文档        - 各类报告和文档
echo 5. 数据文件        - 数据文件和结果
echo 6. 日志文件        - 运行日志
echo 7. 工具脚本        - 辅助工具和批处理
echo 8. 临时文件        - 临时文件和缓存
echo.
echo 9. 运行ETL脚本
echo 10. 运行测试脚本
echo 11. 查看文件夹结构说明
echo 0. 退出
echo.
set /p choice="请输入选择 (0-11): "

if "%choice%"=="1" (
    explorer "01_核心ETL程序"
    echo 已打开: 核心ETL程序
)
if "%choice%"=="2" (
    explorer "02_测试验证脚本"
    echo 已打开: 测试验证脚本
)
if "%choice%"=="3" (
    explorer "03_配置文件"
    echo 已打开: 配置文件
)
if "%choice%"=="4" (
    explorer "04_报告文档"
    echo 已打开: 报告文档
)
if "%choice%"=="5" (
    explorer "05_数据文件"
    echo 已打开: 数据文件
)
if "%choice%"=="6" (
    explorer "06_日志文件"
    echo 已打开: 日志文件
)
if "%choice%"=="7" (
    explorer "07_工具脚本"
    echo 已打开: 工具脚本
)
if "%choice%"=="8" (
    explorer "08_临时文件"
    echo 已打开: 临时文件
)
if "%choice%"=="9" (
    echo.
    echo 可用的ETL脚本:
    echo 1. 运行完整ETL流程 (SAP + SFC + MES) - PowerShell版本
    echo 2. 仅运行MES数据处理 - PowerShell版本
    echo 3. 仅运行SAP路由处理 - PowerShell版本
    echo 4. 仅运行SFC批次处理 - PowerShell版本
    echo.
    set /p etl_choice="请选择ETL脚本 (1-4): "
    if "!etl_choice!"=="1" (
        start "" "07_工具脚本\run_all_etl_ps.bat"
        echo 已启动: 完整ETL流程 (PowerShell版本)
    )
    if "!etl_choice!"=="2" (
        start "" "07_工具脚本\run_etl.bat"
        echo 已启动: MES数据处理
    )
    if "!etl_choice!"=="3" (
        start "" "07_工具脚本\run_sap_routing_ps.bat"
        echo 已启动: SAP路由处理 (PowerShell版本)
    )
    if "!etl_choice!"=="4" (
        start "" "07_工具脚本\run_sfc_batch.bat"
        echo 已启动: SFC批次处理
    )
)
if "%choice%"=="10" (
    start "" "07_工具脚本\run_tests.bat"
    echo 已启动: 测试脚本运行器
)
if "%choice%"=="11" (
    start "" "文件夹结构说明.md"
    echo 已打开: 文件夹结构说明文档
)
if "%choice%"=="0" (
    exit /b 0
)

echo.
pause
