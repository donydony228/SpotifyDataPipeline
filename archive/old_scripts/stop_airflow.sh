#!/bin/bash
echo "🛑 Stopping Airflow processes..."

# 找到並停止 Airflow 相關進程
pkill -f "airflow"

echo "✅ Airflow processes stopped"
