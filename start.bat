@echo off
chcp 65001 >nul

echo 🚀 MySQL慢日志分析工具 - 启动中...
echo.

REM 检查docker目录是否存在
if not exist "docker" (
    echo ❌ 错误: 找不到docker目录
    echo    请确保在项目根目录运行此脚本
    pause
    exit /b 1
)

REM 调用实际的启动脚本
call docker\scripts\start.bat
