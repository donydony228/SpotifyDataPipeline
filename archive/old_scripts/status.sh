#!/bin/bash
# 狀態檢查腳本 - 修復版

export AIRFLOW_HOME=~/airflow

echo "📊 Airflow服務狀態"
echo "=================="
echo "📁 AIRFLOW_HOME: $AIRFLOW_HOME"

# 檢查進程
if pgrep -f "airflow standalone" > /dev/null; then
    PID=$(pgrep -f "airflow standalone")
    echo "✅ Airflow standalone正在運行 (PID: $PID)"
    
    # 檢查端口
    if lsof -i :8080 > /dev/null 2>&1; then
        echo "✅ Web服務器正在監聽端口8080"
        echo "🌐 訪問: http://localhost:8080"
    else
        echo "⚠️  端口8080未監聽"
    fi
    
    # 檢查日誌最後幾行
    echo ""
    echo "📋 最近日誌 (最後10行):"
    if [ -f "$AIRFLOW_HOME/logs/standalone.log" ]; then
        tail -10 "$AIRFLOW_HOME/logs/standalone.log"
    else
        echo "日誌文件不存在"
    fi
    
else
    echo "❌ Airflow未運行"
fi

# 顯示DAG信息
echo ""
echo "📦 DAG信息:"
if [ -d "$AIRFLOW_HOME/dags" ]; then
    DAG_COUNT=$(find "$AIRFLOW_HOME/dags" -name "*.py" 2>/dev/null | wc -l)
    echo "   DAG文件數量: $DAG_COUNT"
    
    if [ $DAG_COUNT -gt 0 ]; then
        echo "   DAG文件列表:"
        find "$AIRFLOW_HOME/dags" -name "*.py" -exec basename {} \; | sed 's/^/     - /'
    fi
else
    echo "   DAG目錄不存在"
fi

echo ""
echo "🔧 管理命令:"
echo "  • 啟動: ~/airflow/start.sh"
echo "  • 停止: ~/airflow/stop.sh"
echo "  • 重啟: ~/airflow/restart.sh"
echo "  • 日誌: ~/airflow/logs.sh"
