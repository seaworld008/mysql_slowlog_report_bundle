# MySQL慢日志 Kibana 仪表板配置指南

## 📊 仪表板概览

基于我们写入ES的MySQL慢日志数据，创建专业的Kibana仪表板，实现：
- **实时TOP慢查询排行榜**
- **多维度性能分析图表**
- **告警监控和趋势分析**
- **交互式数据钻取**

---

## 🔧 前置准备

### 1. 确认数据已写入ES
```bash
# 检查索引是否存在
curl -X GET "http://your-es-server:9200/_cat/indices/mysql-slowlog-*?v"

# 检查数据样本
curl -X GET "http://your-es-server:9200/mysql-slowlog-*/_search?size=1&pretty"
```

### 2. 访问Kibana
- 打开浏览器访问：`http://your-kibana-server:5601`
- 使用ES相同的用户名密码登录

---

## 📋 步骤一：创建索引模式

### 1.1 进入索引模式管理
```
Kibana左侧菜单 → Management → Stack Management → Kibana → Index Patterns
```

### 1.2 创建索引模式
- **索引模式名称**：`mysql-slowlog-*`
- **时间字段**：选择 `@timestamp`
- **点击"创建索引模式"**

### 1.3 验证字段映射
确认以下关键字段已正确识别：
```
@timestamp                              # 时间戳
mysql.slowlog.query_time.total_seconds  # 总耗时
mysql.slowlog.query_time.avg_seconds    # 平均耗时
mysql.slowlog.samples                   # 样本数
mysql.slowlog.database                  # 数据库名
mysql.slowlog.table                     # 表名
mysql.slowlog.sql.normalized            # 规范化SQL
mysql.slowlog.sql.example               # 示例SQL
host.name                               # 主机名
```

---

## 🎨 步骤二：创建核心可视化图表

### 2.1 TOP慢查询表格（主要图表）

#### 创建步骤：
1. **Kibana → Visualize → Create Visualization → Data Table**
2. **配置指标（Metrics）**：
   ```
   Y轴指标：
   - Aggregation: Sum
   - Field: mysql.slowlog.query_time.total_seconds
   - Custom Label: "总耗时(秒)"
   ```
3. **配置分桶（Buckets）**：
   ```
   行分桶：
   - Aggregation: Terms
   - Field: mysql.slowlog.fingerprint.keyword
   - Order By: metric: 总耗时(秒)
   - Order: Descending
   - Size: 30
   - Custom Label: "SQL指纹"
   ```
4. **添加更多列**：
   - 点击"Add metrics"添加以下指标：
     ```
     平均耗时: Average → mysql.slowlog.query_time.avg_seconds
     样本数: Sum → mysql.slowlog.samples  
     数据库: Top Hit → mysql.slowlog.database.keyword
     示例SQL: Top Hit → mysql.slowlog.sql.example.keyword
     ```
5. **保存**：命名为"MySQL慢查询TOP30"

### 2.2 时间序列趋势图

#### 创建步骤：
1. **Create Visualization → Line Chart**
2. **配置**：
   ```
   Y轴: Sum of mysql.slowlog.query_time.total_seconds
   X轴: Date Histogram on @timestamp (间隔: 1小时)
   Split Series: Terms on mysql.slowlog.database.keyword (Top 5)
   ```
3. **保存**：命名为"慢查询趋势-按数据库"

### 2.3 数据库分布饼图

#### 创建步骤：
1. **Create Visualization → Pie Chart**
2. **配置**：
   ```
   Slice Size: Sum of mysql.slowlog.query_time.total_seconds
   Split Slices: Terms on mysql.slowlog.database.keyword
   ```
3. **保存**：命名为"慢查询耗时分布-按数据库"

### 2.4 主机性能对比

#### 创建步骤：
1. **Create Visualization → Vertical Bar Chart**
2. **配置**：
   ```
   Y轴: Sum of mysql.slowlog.query_time.total_seconds
   X轴: Terms on host.name.keyword
   Split Series: Terms on mysql.slowlog.database.keyword
   ```
3. **保存**：命名为"主机慢查询对比"

---

## 📊 步骤三：创建综合仪表板

### 3.1 创建仪表板
```
Kibana → Dashboard → Create Dashboard
```

### 3.2 添加可视化图表
按以下布局添加图表：
```
+------------------+------------------+
|   TOP慢查询表格   |   时间序列趋势图   |
|    (全宽显示)     |    (右侧显示)     |
+------------------+------------------+
|  数据库分布饼图   |   主机性能对比    |
|   (左下显示)      |   (右下显示)     |
+------------------+------------------+
```

### 3.3 配置时间范围选择器
- 默认时间范围：最近24小时
- 快速选择：最近1小时、4小时、1天、7天

### 3.4 添加筛选器
添加常用筛选器：
```
- host.name.keyword (主机筛选)
- mysql.slowlog.database.keyword (数据库筛选)  
- mysql.slowlog.query_time.avg_seconds (平均耗时范围)
```

### 3.5 保存仪表板
- **标题**：MySQL慢日志分析仪表板
- **描述**：实时监控MySQL慢查询性能，支持多维度分析

---

## 🔍 步骤四：高级配置

### 4.1 配置表格显示优化

#### TOP慢查询表格增强：
1. **编辑可视化图表**
2. **Options标签页**：
   ```
   - Show partial rows: false
   - Show metrics for every bucket: true
   - Show total: true
   ```
3. **添加格式化**：
   - 总耗时：`0,0.00` (数字格式，保留2位小数)
   - 平均耗时：`0,0.000` (数字格式，保留3位小数)
   - 样本数：`0,0` (整数格式)

### 4.2 创建告警监控

#### 配置Watcher告警：
```json
{
  "trigger": {
    "schedule": {
      "interval": "5m"
    }
  },
  "input": {
    "search": {
      "request": {
        "search_type": "query_then_fetch",
        "indices": ["mysql-slowlog-*"],
        "body": {
          "query": {
            "bool": {
              "filter": [
                {
                  "range": {
                    "@timestamp": {
                      "gte": "now-5m"
                    }
                  }
                },
                {
                  "range": {
                    "mysql.slowlog.query_time.avg_seconds": {
                      "gte": 10
                    }
                  }
                }
              ]
            }
          },
          "aggs": {
            "slow_queries": {
              "terms": {
                "field": "mysql.slowlog.sql.normalized.keyword",
                "size": 5
              },
              "aggs": {
                "avg_time": {
                  "avg": {
                    "field": "mysql.slowlog.query_time.avg_seconds"
                  }
                }
              }
            }
          }
        }
      }
    }
  },
  "condition": {
    "compare": {
      "ctx.payload.hits.total": {
        "gt": 0
      }
    }
  },
  "actions": {
    "send_email": {
      "email": {
        "to": ["dba@company.com"],
        "subject": "MySQL慢查询告警",
        "body": "检测到平均执行时间超过10秒的慢查询，请及时处理。"
      }
    }
  }
}
```

---

## 📱 步骤五：移动端适配

### 5.1 响应式布局
- 确保图表在手机端可正常显示
- 调整表格列宽，优先显示关键指标

### 5.2 创建精简版仪表板
专门为移动端创建：
- 仅显示TOP10慢查询
- 使用更大的字体和图标
- 简化交互操作

---

## 🎯 使用建议

### 日常监控流程
1. **每日巡检**：查看TOP慢查询是否有新增
2. **性能分析**：通过时间序列图观察趋势
3. **问题定位**：点击具体SQL查看详细信息
4. **优化验证**：对比优化前后的性能变化

### 最佳实践
- **设置书签**：将仪表板添加到浏览器书签
- **定期导出**：将关键图表导出为PDF报告
- **团队共享**：设置仪表板权限，与DBA团队共享
- **定制化**：根据不同业务需求创建专门的仪表板

---

## 🚀 扩展功能

### 集成钉钉/企业微信告警
```python
# webhook_alert.py - 钉钉告警脚本示例
import requests
import json

def send_dingtalk_alert(slow_queries):
    webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN"
    
    message = {
        "msgtype": "markdown",
        "markdown": {
            "title": "MySQL慢查询告警",
            "text": f"""
## MySQL慢查询告警 🚨

**检测时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

**慢查询TOP5**:
{chr(10).join([f"- {sql[:50]}... (平均{avg_time:.2f}s)" for sql, avg_time in slow_queries])}

**处理建议**: 
- 检查索引是否合理
- 分析SQL执行计划
- 考虑查询优化

[查看详细仪表板](http://your-kibana:5601/app/dashboards#/view/mysql-slowlog-dashboard)
            """
        }
    }
    
    requests.post(webhook_url, json=message)
```

### 自动化报告生成
```bash
# daily_report.sh - 每日报告生成脚本
#!/bin/bash
DATE=$(date +%Y-%m-%d)
curl -X POST "http://kibana:5601/api/reporting/generate/png" \
  -H "Content-Type: application/json" \
  -d '{
    "layout": {"id": "png", "dimensions": {"width": 1920, "height": 1080}},
    "relativeUrls": ["/app/dashboards#/view/mysql-slowlog-dashboard"]
  }' > "/reports/mysql-slowlog-${DATE}.png"
```

---

## 📞 技术支持

如需帮助配置或遇到问题：
1. 检查ES数据是否正常写入
2. 确认Kibana版本兼容性
3. 验证字段映射是否正确
4. 查看Kibana日志排查错误

**配置完成后，您将拥有一个专业的MySQL慢日志监控仪表板！** 🎉
