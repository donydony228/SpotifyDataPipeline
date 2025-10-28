#!/bin/bash
# Airflow 2.8+ 啟動腳本 - 修復版

# 設置 AIRFLOW_HOME
export AIRFLOW_HOME=~/airflow

# 加載環境變量
if [ -f "$AIRFLOW_HOME/.env" ]; then
    export $(grep -v '^#' $AIRFLOW_HOME/.env | xargs)
fi

echo "🚀 啟動Airflow（Standalone模式）"
echo "================================"
echo "📁 AIRFLOW_HOME: $AIRFLOW_HOME"

# 確保目錄存在
mkdir -p $AIRFLOW_HOME/logs

# 檢查是否已經在運行
if pgrep -f "airflow standalone" > /dev/null; then
    echo "ℹ️  Airflow standalone已在運行"
    echo "🌐 訪問: http://localhost:8080"
    echo "👤 登入: admin / admin"
    exit 0
fi

echo "▶️  啟動Airflow standalone..."
echo "💡 這會同時啟動webserver和scheduler"
echo ""

# 後台啟動standalone模式
nohup airflow standalone > $AIRFLOW_HOME/logs/standalone.log 2>&1 &
STANDALONE_PID=$!

echo "🔄 等待服務啟動..."
echo "📊 進程ID: $STANDALONE_PID"

# 等待啟動
for i in {1..30}; do
    echo -n "."
    sleep 2
    
    # 檢查進程是否還在運行
    if ! kill -0 $STANDALONE_PID 2>/dev/null; then
        echo ""
        echo "❌ Airflow進程意外退出"
        echo "📋 檢查錯誤日誌:"
        tail -20 $AIRFLOW_HOME/logs/standalone.log
        exit 1
    fi
    
    # 檢查端口是否開始監聽
    if lsof -i :8080 > /dev/null 2>&1; then
        echo ""
        echo "✅ Airflow standalone啟動成功"
        echo "📊 進程ID: $STANDALONE_PID"
        echo ""
        echo "🎉 Airflow已啟動完成!"
        echo "🌐 訪問: http://localhost:8080"
        echo "👤 登入: admin / admin (standalone模式默認)"
        echo ""
        echo "📋 管理命令:"
        echo "  • 檢查狀態: ~/airflow/status.sh"
        echo "  • 查看日誌: ~/airflow/logs.sh"
        echo "  • 停止服務: ~/airflow/stop.sh"
        echo ""
        echo "💡 如果無法登入，請檢查日誌確認用戶名密碼"
        exit 0
    fi
done

echo ""
echo "⚠️  啟動時間較長，檢查日誌..."
echo "📋 最近日誌:"
tail -10 $AIRFLOW_HOME/logs/standalone.log
echo ""
echo "💡 繼續等待或手動檢查: ~/airflow/status.sh"
