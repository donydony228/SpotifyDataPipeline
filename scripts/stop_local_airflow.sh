#!/bin/bash
# scripts/stop_local_airflow.sh
# 本地 Airflow 停止腳本

echo "🛑 停止 Airflow 本地服務..."

# 尋找並停止所有 Airflow 相關進程
AIRFLOW_PIDS=$(pgrep -f "airflow")

if [ -z "$AIRFLOW_PIDS" ]; then
    echo "ℹ️ 沒有發現運行中的 Airflow 進程"
else
    echo "📊 找到 Airflow 進程: $AIRFLOW_PIDS"
    
    # 優雅地停止進程
    echo "🔄 正在停止 Airflow 進程..."
    for pid in $AIRFLOW_PIDS; do
        echo "  停止進程 $pid..."
        kill $pid 2>/dev/null || true
    done
    
    # 等待進程停止
    sleep 3
    
    # 檢查是否還有進程在運行
    REMAINING_PIDS=$(pgrep -f "airflow" || true)
    if [ -n "$REMAINING_PIDS" ]; then
        echo "⚠️ 強制終止剩餘進程: $REMAINING_PIDS"
        for pid in $REMAINING_PIDS; do
            kill -9 $pid 2>/dev/null || true
        done
    fi
    
    echo "✅ Airflow 進程已停止"
fi

# 檢查端口是否還被佔用
if lsof -i :8080 > /dev/null 2>&1; then
    echo "⚠️ 端口 8080 仍被佔用"
    PORT_PID=$(lsof -ti :8080)
    if [ -n "$PORT_PID" ]; then
        echo "🔄 終止佔用端口 8080 的進程 $PORT_PID"
        kill -9 $PORT_PID 2>/dev/null || true
    fi
fi

# 最終檢查
sleep 1
if pgrep -f "airflow" > /dev/null; then
    echo "❌ 部分 Airflow 進程可能仍在運行"
    echo "📋 剩餘進程:"
    pgrep -f "airflow" -l || true
else
    echo "✅ 所有 Airflow 進程已成功停止"
fi

if ! lsof -i :8080 > /dev/null 2>&1; then
    echo "✅ 端口 8080 已釋放"
else
    echo "⚠️ 端口 8080 仍被佔用"
fi

echo "🛑 停止完成"