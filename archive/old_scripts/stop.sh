#!/bin/bash
echo "⏹️  停止Airflow服務"

# 停止standalone進程
if pgrep -f "airflow standalone" > /dev/null; then
    echo "🛑 停止Airflow standalone..."
    pkill -f "airflow standalone"
    sleep 3
    
    # 確認停止
    if pgrep -f "airflow standalone" > /dev/null; then
        echo "⚠️  強制停止..."
        pkill -9 -f "airflow standalone"
    fi
    
    echo "✅ Airflow standalone已停止"
else
    echo "ℹ️  Airflow standalone未運行"
fi

# 清理其他airflow進程
if pgrep -f "airflow" > /dev/null; then
    echo "🧹 清理其他Airflow進程..."
    pkill -f "airflow"
    sleep 2
    
    # 最後檢查
    if pgrep -f "airflow" > /dev/null; then
        echo "⚠️  強制清理殘留進程..."
        pkill -9 -f "airflow"
    fi
fi

echo "🛑 所有Airflow服務已停止"
