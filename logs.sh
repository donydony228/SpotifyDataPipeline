#!/bin/bash
# 日誌查看腳本 - 修復版

export AIRFLOW_HOME=~/airflow

echo "📋 Airflow日誌查看"
echo "=================="

LOG_FILE="$AIRFLOW_HOME/logs/standalone.log"

if [ -f "$LOG_FILE" ]; then
    echo "📊 實時日誌 (按Ctrl+C退出):"
    echo "📁 日誌文件: $LOG_FILE"
    echo "=========================="
    tail -f "$LOG_FILE"
else
    echo "❌ 日誌文件不存在: $LOG_FILE"
    echo "💡 請先啟動Airflow: ~/airflow/start.sh"
fi
