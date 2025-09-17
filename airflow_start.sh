echo "🌊 Starting Airflow Standalone..."

# 確保在虛擬環境中
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "❌ Please activate virtual environment first: ./start_dev.sh"
    exit 1
fi

# 確保環境變數設定
if [[ "$AIRFLOW_HOME" == "" ]]; then
    export AIRFLOW_HOME=$(pwd)/airflow_home
fi

# 啟動 Airflow
echo "🚀 Starting Airflow at http://localhost:8080"
airflow standalone
