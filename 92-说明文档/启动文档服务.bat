@echo off
chcp 65001 >nul
echo ========================================
echo   MkDocs 电子说明书 - 启动本地服务
echo ========================================
echo.
echo 正在启动文档服务器...
echo.
echo 提示：
echo   - 服务器启动后，请手动打开浏览器访问：
echo   - http://127.0.0.1:8000/
echo   - 修改文档后会自动刷新浏览器
echo   - 按 Ctrl+C 停止服务器
echo.
echo ========================================
echo.

mkdocs serve

pause

