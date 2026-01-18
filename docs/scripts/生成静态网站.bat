@echo off
chcp 65001 >nul
echo ========================================
echo   MkDocs 电子说明书 - 生成静态网站
echo ========================================
echo.

echo 切换到项目根目录...
cd ..
echo 正在生成静态 HTML 文件...
mkdocs build --clean

if errorlevel 1 (
    echo.
    echo ❌ 生成失败！请检查错误信息
    pause
    exit /b 1
)

echo.
echo ========================================
echo   ✅ 生成完成！
echo ========================================
echo.
echo 生成位置：site/ 文件夹
echo.
echo 使用方法：
echo   1. 本地浏览：双击打开 site\index.html
echo   2. 部署服务器：将 site 文件夹复制到 Web 服务器
echo.
pause

