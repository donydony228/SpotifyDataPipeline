#!/bin/bash
echo "🔄 Resetting Airflow environment..."

# 確保在虛擬環境中
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "❌ Please activate virtual environment first: ./start_dev.sh"
    exit 1
fi

# 停止所有 Airflow 進程
./stop_airflow.sh

# 重置資料庫
export AIRFLOW_HOME=$(pwd)/airflow_home
rm -f $AIRFLOW_HOME/airflow.db
airflow db init

# 重新建立用戶
airflow users create \
    --username admin \
    --firstname Data \
    --lastname Engineer \
    --role Admin \
    --email admin@jobdata.com \
    --password admin123

echo "✅ Airflow reset complete"
