@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo =========================================
echo MySQL慢日志分析工具 - Docker部署
echo =========================================

REM 切换到项目根目录
cd /d "%~dp0\..\.."

REM 创建必要的目录
echo 创建目录结构...
if not exist "slowlogs" mkdir slowlogs
if not exist "output" mkdir output  
if not exist "logs" mkdir logs

REM 检查慢日志文件
dir slowlogs\*.log >nul 2>&1
if errorlevel 1 (
    echo ⚠️  警告: slowlogs目录为空
    echo    请将MySQL慢日志文件放入 .\slowlogs\ 目录
    echo    例如: copy C:\mysql\logs\slow.log .\slowlogs\
)

REM 检查配置文件
if not exist "config\config.env" (
    echo ❌ 错误: 请先配置config\config.env文件
    echo    请复制config\config.env并修改其中的ES配置
    pause
    exit /b 1
)

REM 检查Docker
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: 请先安装Docker Desktop
    pause
    exit /b 1
)

REM 检查Docker Compose
docker-compose --version >nul 2>&1
if errorlevel 1 (
    docker compose version >nul 2>&1
    if errorlevel 1 (
        echo ❌ 错误: 请先安装Docker Compose
        pause
        exit /b 1
    )
)

REM 显示配置信息
echo.
echo 目录结构：
echo ├── slowlogs\        # 慢日志文件目录
echo ├── output\          # 分析结果输出目录
echo ├── logs\            # 运行日志目录
echo ├── config\          # 配置文件目录
echo │   └── config.env   # ES配置文件
echo └── docker\          # Docker配置目录

REM 构建并启动服务
echo.
echo 构建Docker镜像...
docker-compose -f docker\docker-compose.yml build

echo.
echo 启动服务...
docker-compose -f docker\docker-compose.yml up -d

REM 等待服务启动
echo.
echo 等待服务启动...
timeout /t 5 /nobreak >nul

REM 检查服务状态
echo.
echo 服务状态：
docker-compose -f docker\docker-compose.yml ps

REM 显示访问信息
echo.
echo =========================================
echo 🎉 部署完成！
echo =========================================
echo.
echo 📋 定时任务：
echo - 每天2点: 分析当天数据，TOP 30写入ES
echo - 每周一3点: 分析最近7天，TOP 30写入ES
echo - 每月1号4点: 分析最近30天，TOP 50写入ES

echo.
echo 📁 目录说明：
echo - 慢日志文件: .\slowlogs\*.log
echo - 分析结果: .\output\
echo - 运行日志: .\logs\

echo.
echo 🔧 常用命令：
echo - 查看实时日志: docker-compose -f docker\docker-compose.yml logs -f
echo - 停止服务: docker-compose -f docker\docker-compose.yml down
echo - 重启服务: docker-compose -f docker\docker-compose.yml restart

echo.
echo 📊 ES数据将写入索引: mysql-slowlog-YYYY.MM.DD
echo =========================================
pause
