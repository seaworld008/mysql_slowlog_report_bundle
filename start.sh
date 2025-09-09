#!/bin/bash

# MySQL慢日志分析工具 - 主启动脚本
# 该脚本会调用docker/scripts/start.sh

echo "🚀 MySQL慢日志分析工具 - 启动中..."
echo ""

# 检查docker目录是否存在
if [ ! -d "docker" ]; then
    echo "❌ 错误: 找不到docker目录"
    echo "   请确保在项目根目录运行此脚本"
    exit 1
fi

# 调用实际的启动脚本
exec ./docker/scripts/start.sh
