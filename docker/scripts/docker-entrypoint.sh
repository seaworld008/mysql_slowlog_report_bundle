#!/bin/bash

# MySQL慢日志分析工具 - Docker启动脚本
echo "========================================="
echo "MySQL慢日志分析工具 - 容器启动中..."
echo "启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

# 创建必要的目录
mkdir -p /app/logs /app/output /app/slowlogs

# 启动cron服务
echo "启动定时任务服务..."
service cron start

# 检查环境变量
echo "当前环境配置："
echo "- 时区: $TZ"
echo "- Python版本: $(python --version)"
echo "- 工作目录: $(pwd)"
echo "- 慢日志路径: ${SLOWLOG_PATH:-未配置}"
echo "- TOP配置: 日${TOP_DAILY:-30}/周${TOP_WEEKLY:-50}/月${TOP_MONTHLY:-100}"
echo "- 最小时间: ${MIN_TIME:-0.5}s"
echo "- 并发数: ${JOBS:-2}"
echo "- ES主机: ${ES_HOST:-未配置}"
echo "- ES用户: ${ES_USER:-未配置}"

# 显示定时任务配置
echo ""
echo "定时任务配置："
crontab -l

# 输出启动完成信息
echo ""
echo "========================================="
echo "容器启动完成！"
echo "- 定时任务已配置并启动"
echo "- 日志目录: /app/logs"
echo "- 输出目录: /app/output"
echo "- 慢日志目录: /app/slowlogs"
if [ -n "$ES_HOST" ]; then
    echo "- Elasticsearch: $ES_HOST"
fi
echo "========================================="

# 保持容器运行并监控日志
echo "开始监控日志输出..."
while true; do
    # 检查是否有新日志文件
    if ls /app/logs/*.log 1> /dev/null 2>&1; then
        tail -f /app/logs/*.log
        break
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 等待日志文件生成..."
    sleep 30
done
