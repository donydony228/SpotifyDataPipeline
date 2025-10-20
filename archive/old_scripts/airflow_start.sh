#!/bin/bash

echo "🚀 啟動 Airflow..."

cd /Users/desmond/airflow

# 啟動虛擬環境
source venv/bin/activate

# 確保 providers 已安裝
echo "📦 檢查 providers..."
pip install apache-airflow-providers-standard --quiet 2>/dev/null || true

# 驗證 SSL
echo "🔍 檢查 SSL 版本..."
python -c "import ssl; print('SSL:', ssl.OPENSSL_VERSION)"

# 清理舊的 PID 文件
rm -f airflow_home/*.pid

# 設定環境變數
export AIRFLOW_HOME=$(pwd)/airflow_home

# 從 .env 讀取
if [ -f .env ]; then
    export SUPABASE_DB_URL=$(grep SUPABASE_DB_URL .env | cut -d'=' -f2 | tr -d '"')
    export MONGODB_ATLAS_URL=$(grep MONGODB_ATLAS_URL .env | cut -d'=' -f2 | tr -d '"')
    export MONGODB_ATLAS_DB_NAME=$(grep MONGODB_ATLAS_DB_NAME .env | cut -d'=' -f2 | tr -d '"')
fi

# 檢查數據庫
echo "🗄️ 檢查 Airflow 數據庫..."
airflow db check

echo "🌐 啟動 Airflow (http://localhost:8080)..."
echo "📌 帳號: admin / 密碼: 查看下方輸出"
airflow standalone
