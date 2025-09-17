#!/bin/bash
echo "🚀 Starting US Job Data Engineering Environment"

# 啟動虛擬環境
source us-job-env/bin/activate

# 設定環境變數
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "✅ Environment activated"
echo "📊 Airflow Home: $AIRFLOW_HOME" 
echo "🌐 To start Airflow: ./airflow_start.sh"
echo "🌐 Airflow UI will be at: http://localhost:8080"
echo "👤 Username: admin, Password: admin123"
