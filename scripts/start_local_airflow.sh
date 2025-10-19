#!/bin/bash
# scripts/start_local_airflow.sh
# 本地 Airflow 啟動腳本（不使用容器）

set -e

echo "🎵 啟動音樂資料工程平台 - 本地模式"
echo "==============================================="

# 檢查虛擬環境
if [ ! -d "venv" ]; then
    echo "❌ 虛擬環境不存在，請先執行: make venv-setup"
    exit 1
fi

# 啟動虛擬環境
echo "🐍 啟動虛擬環境..."
source venv/bin/activate

# 檢查必要套件
echo "📦 檢查套件安裝..."
python -c "import airflow" 2>/dev/null || {
    echo "❌ Airflow 未安裝，安裝中..."
    pip install -r requirements.txt
}

# 設置 Airflow Home
export AIRFLOW_HOME=$(pwd)/airflow_home
mkdir -p $AIRFLOW_HOME
echo "📁 AIRFLOW_HOME: $AIRFLOW_HOME"

# 載入環境變數
echo "📝 載入環境變數..."
if [ -f .env ]; then
    # 使用 python-dotenv 正確載入環境變數
    export $(python -c "
from dotenv import load_dotenv
import os
load_dotenv()
for key, value in os.environ.items():
    if any(key.startswith(prefix) for prefix in ['SPOTIFY_', 'SUPABASE_', 'MONGODB_', 'AIRFLOW__']):
        print(f'{key}={value}')
")
    echo "✅ 環境變數已載入"
    
    # 複製 .env 到 Airflow home 以防萬一
    cp .env $AIRFLOW_HOME/.env
else
    echo "❌ .env 檔案不存在！"
    echo "請執行: make env-setup"
    exit 1
fi

# 檢查關鍵環境變數
echo "🔍 檢查環境變數..."
python -c "
import os
spotify_vars = ['SPOTIFY_CLIENT_ID', 'SPOTIFY_CLIENT_SECRET']
missing = []
for var in spotify_vars:
    if not os.getenv(var):
        missing.append(var)
if missing:
    print(f'❌ 缺少環境變數: {missing}')
    exit(1)
else:
    print('✅ Spotify 環境變數已設定')
"

# 設置 Airflow 配置
echo "⚙️ 設置 Airflow 配置..."
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# 檢查是否已經在運行
if pgrep -f "airflow standalone" > /dev/null; then
    echo "ℹ️ Airflow 已在運行"
    echo "🌐 訪問: http://localhost:8080"
    echo "👤 用戶名: admin / 密碼: admin"
    exit 0
fi

# 初始化資料庫（如果需要）
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "🗄️ 初始化 Airflow 資料庫..."
    airflow db init
fi

# 檢查是否需要創建管理員用戶
USER_EXISTS=$(airflow users list 2>/dev/null | grep -c "admin" || echo "0")
if [ "$USER_EXISTS" -eq "0" ]; then
    echo "👤 創建管理員用戶..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@musicdata.com \
        --password admin123
fi

echo "🚀 啟動 Airflow standalone..."
echo "💡 這會同時啟動 webserver 和 scheduler"

# 創建 logs 目錄
mkdir -p $AIRFLOW_HOME/logs

# 啟動 Airflow standalone (背景執行)
nohup airflow standalone > $AIRFLOW_HOME/logs/standalone.log 2>&1 &
AIRFLOW_PID=$!

echo "🔄 等待 Airflow 啟動..."
echo "📊 進程 ID: $AIRFLOW_PID"

# 等待啟動完成
for i in {1..30}; do
    echo -n "."
    sleep 2
    
    # 檢查進程是否還在運行
    if ! kill -0 $AIRFLOW_PID 2>/dev/null; then
        echo ""
        echo "❌ Airflow 進程意外退出"
        echo "📋 錯誤日誌:"
        tail -20 $AIRFLOW_HOME/logs/standalone.log
        exit 1
    fi
    
    # 檢查 web server 是否已啟動
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo ""
        echo "✅ Airflow 啟動成功！"
        break
    fi
done

# 最終狀態檢查
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo ""
    echo "🎉 音樂資料工程平台已啟動！"
    echo "=============================="
    echo "🌐 Web UI: http://localhost:8080"
    echo ""
    echo "📊 系統資訊:"
    echo "  • 進程 ID: $AIRFLOW_PID"
    echo "  • 執行器: SequentialExecutor"
    echo "  • 資料庫: SQLite (本地)"
    echo "  • DAGs 目錄: $(pwd)/dags"
    echo ""
    echo "📋 管理指令:"
    echo "  • 檢查狀態: make dev-status"
    echo "  • 查看日誌: make logs"
    echo "  • 停止服務: make stop"
    echo "  • 測試環境變數: make env-check"
    echo ""
    echo "💡 如果看不到 DAGs，請檢查 dags/ 目錄中的檔案"
else
    echo ""
    echo "⚠️ Airflow 啟動時間較長，請稍候..."
    echo "📋 查看日誌: tail -f $AIRFLOW_HOME/logs/standalone.log"
    echo "🔍 手動檢查狀態: make dev-status"
fi

echo ""
echo "✅ 啟動腳本執行完成"