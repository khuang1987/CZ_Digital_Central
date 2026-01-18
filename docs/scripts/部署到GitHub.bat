@echo off
chcp 65001 >nul
echo ========================================
echo   MkDocs 部署到 GitHub Pages
echo ========================================
echo.

REM 切换到项目根目录
echo 切换到项目根目录...
cd ..

REM 检查是否在正确的目录
if not exist "mkdocs.yml" (
    echo 错误：未找到 mkdocs.yml 文件
    echo 请确保在项目根目录中运行此脚本
    pause
    exit /b 1
)

echo 步骤 1：构建静态网站...
echo.
mkdocs build --clean
if errorlevel 1 (
    echo.
    echo ❌ 构建失败！请检查错误信息
    pause
    exit /b 1
)

echo.
echo ✅ 构建成功！
echo.

echo 步骤 2：检查 Git 状态...
echo.
git status
echo.

echo 步骤 3：部署到 GitHub Pages...
echo.
echo 提示：如果遇到网络问题，请：
echo   1. 检查网络连接
echo   2. 检查 GitHub 访问权限
echo   3. 或使用代理设置
echo.
pause

mkdocs gh-deploy
if errorlevel 1 (
    echo.
    echo ❌ 部署失败！
    echo.
    echo 可能的原因：
    echo   1. 网络连接问题
    echo   2. GitHub 权限问题
    echo   3. 仓库未初始化
    echo.
    echo 请参考 GitHub部署指南.md 获取详细帮助
    pause
    exit /b 1
)

echo.
echo ========================================
echo   ✅ 部署成功！
echo ========================================
echo.
echo 您的文档已部署到 GitHub Pages
echo 访问地址：https://khuang1987.github.io/250418_MDDAP_project/
echo.
echo 注意：GitHub Pages 可能需要几分钟才能更新
echo.
pause

