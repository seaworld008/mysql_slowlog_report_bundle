#!/bin/bash

# MySQL慢日志分析工具 - 快速启动脚本

set -e

echo "========================================="
echo "MySQL慢日志分析工具 - Docker部署"
echo "========================================="

# 创建必要的目录
echo "创建目录结构..."
mkdir -p slowlogs output logs

# 检查慢日志文件
if [ ! "$(ls -A slowlogs 2>/dev/null)" ]; then
    echo "⚠️  警告: slowlogs目录为空"
    echo "   请将MySQL慢日志文件放入 ./slowlogs/ 目录"
    echo "   例如: cp /var/log/mysql/slow.log ./slowlogs/"
fi

# 检查配置文件
if [ ! -f "config.env" ]; then
    echo "❌ 错误: 请先配置config.env文件"
    echo "   请复制config.env并修改其中的ES配置"
    exit 1
fi

# 检查Docker
if ! command -v docker &> /dev/null; then
    echo "❌ 错误: 请先安装Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ 错误: 请先安装Docker Compose"
    exit 1
fi

# 显示配置信息
echo ""
echo "目录结构："
echo "├── slowlogs/     # 慢日志文件目录"
echo "├── output/       # 分析结果输出目录"
echo "├── logs/         # 运行日志目录"
echo "└── config.env    # ES配置文件"

# 构建并启动服务
echo ""
echo "构建Docker镜像..."
docker-compose build

echo ""
echo "启动服务..."
docker-compose --env-file config.env up -d

# 等待服务启动
echo ""
echo "等待服务启动..."
sleep 5

# 检查服务状态
echo ""
echo "服务状态："
docker-compose ps

# 显示访问信息
echo ""
echo "========================================="
echo "🎉 部署完成！"
echo "========================================="
echo ""
echo "📋 定时任务："
echo "- 每天2点: 分析当天数据，TOP 30写入ES"
echo "- 每周一3点: 分析最近7天，TOP 30写入ES"
echo "- 每月1号4点: 分析最近30天，TOP 50写入ES"

echo ""
echo "📁 目录说明："
echo "- 慢日志文件: ./slowlogs/*.log"
echo "- 分析结果: ./output/"
echo "- 运行日志: ./logs/"

echo ""
echo "🔧 常用命令："
echo "- 查看实时日志: docker-compose logs -f"
echo "- 停止服务: docker-compose down"
echo "- 重启服务: docker-compose restart"
echo "- 手动执行分析: docker-compose exec mysql-slowlog-analyzer python mysql_slowlog_analyzer.py /app/slowlogs/*.log --today --top 30 --es-host \"\$ES_HOST\" --es-user \"\$ES_USER\" --es-password \"\$ES_PASSWORD\""

echo ""
echo "📊 ES数据将写入索引: mysql-slowlog-YYYY.MM.DD"
echo "========================================="
