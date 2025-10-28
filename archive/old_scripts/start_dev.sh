#!/bin/bash
echo "🚀 Starting Local Development Environment..."

# 載入環境變數
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "✅ Loaded .env file"
else
    echo "❌ .env file not found!"
    exit 1
fi

# 創建虛擬環境
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# 啟動虛擬環境
source venv/bin/activate

# 安裝依賴
echo "📚 Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# 設置 Airflow
export AIRFLOW_HOME=$(pwd)/airflow_home
mkdir -p $AIRFLOW_HOME

# 初始化 Airflow (如果需要)
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "🗄️ Initializing Airflow database..."
    airflow db init
    
    # 創建管理員用戶
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@jobdata.com \
        --password admin123
fi

echo "✅ Development environment ready!"
echo "📝 Run './airflow_start.sh' to start Airflow"