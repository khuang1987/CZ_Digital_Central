@echo off
chcp 65001 >nul
echo ========================================
echo   MkDocs 电子说明书 - 安装依赖
echo ========================================
echo.

echo [1/3] 检查 Python 环境...
python --version
if errorlevel 1 (
    echo.
    echo ❌ 错误：未找到 Python！
    echo 请先安装 Python 3.8 或更高版本
    echo 下载地址：https://www.python.org/downloads/
    pause
    exit /b 1
)

echo.
echo [2/3] 升级 pip...
python -m pip install --upgrade pip

echo.
echo [3/3] 安装 MkDocs 及插件...
pip install -r requirements.txt

echo.
echo ========================================
echo   ✅ 安装完成！
echo ========================================
echo.
echo 下一步：
echo   - 运行 "启动文档服务.bat" 预览文档
echo   - 运行 "生成静态网站.bat" 生成离线版本
echo.
pause

