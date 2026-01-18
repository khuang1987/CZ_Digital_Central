@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM 切换到项目根目录
cd ..

echo.
echo ================================================================
echo           数据平台 - 一键刷新所有数据 (增强版)
echo ================================================================
echo.

REM 检测运行环境
echo --- 系统检测 ---
echo ----------------------------------------------------------------
if exist ".venv\Scripts\activate.bat" (
    echo ✓ 虚拟环境: 已检测到 .venv
    set ENV_STATUS=生产环境
) else (
    echo ⚠ 虚拟环境: 未检测到
    set ENV_STATUS=测试环境
)

if exist "data_pipelines\sources\mes\etl\etl_dataclean_mes_batch_report.py" (
    echo ✓ ETL脚本: 已就绪
) else (
    echo ✗ ETL脚本: 未找到
    echo.
    echo 错误：ETL脚本不存在，请检查项目结构
    pause
    exit /b 1
)

echo ✓ 运行模式: %ENV_STATUS%
echo ✓ 当前目录: %CD%
echo.

REM 显示数据源信息
echo.
echo 数据源概览
echo ----------------------------------------------------------------
echo.
echo   数据源                    状态        说明
echo   ----------------------------------------------------------------
echo   1. SAP 工艺路线数据      就绪        标准时间数据
echo   2. SFC 批量报告          就绪        SFC批次数据
echo   3. SFC 产品检验          就绪        产品检验数据
echo   4. MES 批量报告          就绪        生产批次数据
echo.
echo.
echo   总计: 4 个数据源
echo.

REM 显示状态文件信息
echo.
echo 状态文件检查
echo ----------------------------------------------------------------
set STATE_COUNT=0
if exist "data_pipelines\sources\sfc\state\sfc_product_inspection_state.json" (
    echo ✓ SFC产品检验状态文件存在
    set /a STATE_COUNT+=1
)
REM SFC团队合格率已删除
if exist "data_pipelines\sources\mes\state\mes_batch_report_state.json" (
    echo ✓ MES批量报告状态文件存在
    set /a STATE_COUNT+=1
)
echo.
echo   已找到 %STATE_COUNT% 个状态文件（支持增量刷新）
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
    echo ✓ 虚拟环境已激活
    echo.
)

REM 设置错误计数器
set ERROR_COUNT=0
set SUCCESS_COUNT=0

echo ================================================================
echo 1/4 - 刷新 SAP 工艺路线数据
echo ================================================================
python data_pipelines\sources\sap\etl\etl_dataclean_sap_routing.py
if errorlevel 1 (
    echo ❌ SAP 工艺路线数据刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo ✅ SAP 工艺路线数据刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 2/4 - 刷新 SFC 批量报告数据
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_dataclean_sfc_batch_report.py
if errorlevel 1 (
    echo ❌ SFC 批量报告刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo ✅ SFC 批量报告刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 3/4 - 刷新 SFC 产品检验数据
echo ================================================================
python data_pipelines\sources\sfc\etl\etl_dataclean_sfc_product_inspection.py
if errorlevel 1 (
    echo ❌ SFC 产品检验刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo ✅ SFC 产品检验刷新成功
    set /a SUCCESS_COUNT+=1
)
echo.

echo ================================================================
echo 4/4 - 刷新 MES 批量报告数据
echo ================================================================
python data_pipelines\sources\mes\etl\etl_dataclean_mes_batch_report.py
if errorlevel 1 (
    echo ❌ MES 批量报告刷新失败
    set /a ERROR_COUNT+=1
) else (
    echo ✅ MES 批量报告刷新成功
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
echo   ✓ 成功: %SUCCESS_COUNT% 个数据源
if %ERROR_COUNT% GTR 0 (
    echo   ✗ 失败: %ERROR_COUNT% 个数据源
)
echo   总计: 4 个数据源
echo.

if %ERROR_COUNT% EQU 0 (
    echo ================================================================
    echo              ✅ 所有数据源刷新成功！
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
    echo          ⚠️  部分数据源刷新失败，请检查日志
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
    echo   4. 尝试使用全量刷新模式
    echo.
)

echo ================================================================
echo 按任意键退出...
pause >nul
