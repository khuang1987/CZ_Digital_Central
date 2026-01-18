@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM 切换到项目根目录
pushd "%~dp0.."

REM 确保项目根目录在 PYTHONPATH 中（用于绝对导入 shared_infrastructure.*）
set "PYTHONPATH=%CD%;%PYTHONPATH%"

echo.
echo ================================================================
echo           数据平台 - 核心数据快速刷新 (Core Data)
echo ================================================================
echo.

REM 检测运行环境
echo --- 系统检测 ---
echo ----------------------------------------------------------------
if exist ".venv\Scripts\activate.bat" (
    echo 虚拟环境: 已检测到 .venv
    set ENV_STATUS=生产环境
) else (
    echo 虚拟环境: 未检测到
    set ENV_STATUS=测试环境
)

if exist "data_pipelines\sources\mes\etl\etl_mes_batch_output_raw.py" (
    echo ETL脚本: 已就绪
) else (
    echo ETL脚本: 未找到
    echo.
    echo 错误：ETL脚本不存在，请检查项目结构
    if "%NO_PAUSE%"=="" pause
    exit /b 1
)

echo 运行模式: %ENV_STATUS%
echo 当前目录: %CD%
echo.

REM 显示数据源信息
echo.
echo 数据源概览
echo ----------------------------------------------------------------
echo.
echo   数据源                    状态        说明
echo   ----------------------------------------------------------------
echo   1. SAP 工艺路线数据      就绪        标准时间数据 (V2)
echo   2. SFC 批量报告          就绪        SFC批次数据 (V2)
echo   3. SFC 产品检验          就绪        产品检验数据 (V2)
echo   4. MES 批量报告          就绪        生产批次数据 (V2)
echo   5. Planner 任务数据      就绪        任务与标签 (V2)
echo   6. KPI 监控触发         就绪        自动触发检测 (V2)

echo.
echo   总计: 6 个数据源
echo.

REM 显示状态文件信息
echo.
echo 状态检查
echo ----------------------------------------------------------------
if exist "data_pipelines\database\mddap_v2.db" (
    echo V2 数据库文件已存在
    echo   路径: data_pipelines\database\mddap_v2.db
) else (
    echo V2 数据库文件未找到 (将自动创建)
)
echo.

REM 刷新模式说明
echo --- 刷新模式 ---
echo ----------------------------------------------------------------
echo   固定使用增量刷新模式
echo   如需全量刷新请手动运行各ETL脚本
echo.

:start_refresh
echo.
echo ================================================================
echo   开始执行数据刷新（增量模式）
echo ================================================================
echo.

REM 记录开始时间
set START_TIME=%TIME%
echo 开始时间: %START_TIME%
echo.

REM 激活虚拟环境（如果存在）
if exist ".venv\Scripts\activate.bat" (
    echo 虚拟环境: 正在激活...
    call .venv\Scripts\activate.bat
    echo 虚拟环境已激活
    echo.
)

REM 设置错误计数器
set ERROR_COUNT=0
set SUCCESS_COUNT=0

echo ================================================================
echo 1/6 - 刷新 SAP 工艺路线数据
echo ================================================================
python data_pipelines\sources\sap\etl\etl_sap_routing_raw.py
if errorlevel 1 (
    echo SAP 工艺路线数据刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo SAP 工艺路线数据刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 2/6 - 刷新 SFC 批量报告数据
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_batch_output_raw.py
if errorlevel 1 (
    echo SFC 批量报告刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo SFC 批量报告刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 3/6 - 刷新 SFC 产品检验数据
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_sfc_inspection_raw.py
if errorlevel 1 (
    echo SFC 产品检验刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo SFC 产品检验刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 4/6 - 刷新 MES 批量报告数据
echo ================================================================
python data_pipelines\sources\mes\etl\etl_mes_batch_output_raw.py
if errorlevel 1 (
    echo MES 批量报告刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo MES 批量报告刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 5/6 - 刷新 Planner 任务数据
echo ================================================================
python data_pipelines\sources\planner\etl\etl_planner_tasks_raw.py
if errorlevel 1 (
    echo Planner 任务数据刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo Planner 任务数据刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 6/6 - 执行 KPI 监控和触发检测
echo ================================================================
echo 开始执行 KPI 聚合...
python data_pipelines\monitoring\etl\etl_kpi_aggregation.py
if errorlevel 1 (
    echo KPI 聚合失败
    set /a ERROR_COUNT+=1
) else (
    echo KPI 聚合及触发检测成功
    set /a SUCCESS_COUNT+=1
)
echo.

REM 记录结束时间
set END_TIME=%TIME%

echo.
echo ================================================================
echo                      刷新完成统计
echo ================================================================
echo.
echo --- 执行信息 ---
echo ----------------------------------------------------------------
echo   刷新模式: 增量刷新（固定）
echo   开始时间: %START_TIME%
echo   结束时间: %END_TIME%
echo.
echo --- 执行结果 ---
echo ----------------------------------------------------------------
echo   成功: %SUCCESS_COUNT% 个数据源
if %ERROR_COUNT% GTR 0 (
    echo   失败: %ERROR_COUNT% 个数据源
)
echo   总计: 6 个数据源
echo.

if %ERROR_COUNT% EQU 0 (
    echo ================================================================
    echo              所有数据源刷新成功！
    echo ================================================================
    echo.
    echo --- 数据输出位置 ---
    echo ----------------------------------------------------------------
    echo   MES数据: 30-MES导出数据\publish\
    echo   SFC数据: 20-SFC导出数据\publish\
    echo   SAP数据: 40-SAP导出数据\publish\
    echo.
    echo --- 日志文件 ---
    echo ----------------------------------------------------------------
    echo   shared_infrastructure\logs\etl_mes.log
    echo   shared_infrastructure\logs\etl_sfc.log
    echo   shared_infrastructure\logs\etl_sap.log
    echo.
) else (
    echo ================================================================
    echo          部分数据源刷新失败，请检查日志
    echo ================================================================
    echo.
    echo --- 日志位置 ---
    echo ----------------------------------------------------------------
    echo   shared_infrastructure\logs\
    echo.
    echo --- 故障排除 ---
    echo ----------------------------------------------------------------
    echo   1. 查看日志文件了解详细错误
    echo   2. 检查数据源连接是否正常
    echo   3. 验证配置文件路径是否正确
    echo   4. 尝试运行 refresh_full_data.bat 进行全量刷新
    echo.
)

echo ================================================================
echo 按任意键退出...
if "%NO_PAUSE%"=="" pause >nul

popd

if %ERROR_COUNT% NEQ 0 exit /b 1
exit /b 0
