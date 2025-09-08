# MySQL慢日志分析工具 - Docker镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    cron \
    tzdata \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 设置时区为中国时间
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 复制依赖文件并安装Python包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用文件
COPY mysql_slowlog_analyzer.py .
COPY docker-entrypoint.sh .
COPY crontab-config .

# 设置执行权限
RUN chmod +x docker-entrypoint.sh
RUN chmod +x mysql_slowlog_analyzer.py

# 创建必要目录
RUN mkdir -p /app/logs /app/output /app/slowlogs

# 设置crontab
RUN crontab crontab-config

# 启动脚本
CMD ["./docker-entrypoint.sh"]