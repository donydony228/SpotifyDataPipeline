#!/bin/bash
echo "🔄 重啟Airflow服務"
echo "=================="

# 停止
echo "⏹️  停止服務..."
~/airflow/stop.sh

# 等待
echo "⏳ 等待3秒..."
sleep 3

# 啟動
echo "▶️  啟動服務..."
~/airflow/start.sh
