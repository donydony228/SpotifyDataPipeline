#!/bin/bash
echo "🚀 Railway 啟動 (Supabase 修復版)"

export AIRFLOW_HOME=/app/airflow_home

# 啟動健康檢查
python /app/scripts/simple_health_server.py &
HEALTH_PID=$!
sleep 5

# 修復 Supabase 連線
echo "🔧 修復 Supabase 連線..."
python /app/scripts/fix_supabase_ipv6.py

# 確定使用的資料庫
if [ -f "/tmp/supabase_fixed_url.txt" ]; then
    DB_URL=$(cat /tmp/supabase_fixed_url.txt)
    echo "✅ 使用修復的 Supabase"
else
    DB_URL="sqlite:////app/airflow_home/airflow.db"
    echo "📁 使用 SQLite 備用"
fi

# 建立配置
mkdir -p $AIRFLOW_HOME
cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
base_log_folder = /app/logs
logging_level = INFO
executor = LocalExecutor
load_examples = False
fernet_key = railway-fernet-key-32-chars-long!

[database]
sql_alchemy_conn = ${DB_URL}

[webserver]
web_server_port = 8081
secret_key = railway-secret-key
expose_config = True

[api]
auth_backends = airflow.api.auth.backend.basic_auth

[scheduler]
catchup_by_default = False

[logging]
logging_level = INFO
remote_logging = False
base_log_folder = /app/logs
EOC

# 背景初始化 Airflow
(
    echo "🗃️ 初始化 Airflow..."
    airflow db init
    
    echo "👤 建立用戶..."
    airflow users create \
        --username admin \
        --firstname Railway \
        --lastname Admin \
        --role Admin \
        --email admin@railway.app \
        --password admin123 || echo "用戶已存在"
    
    echo "📅 啟動服務..."
    airflow scheduler &
    sleep 20
    airflow webserver --port 8081 --hostname 0.0.0.0 &
    sleep 30
    
    echo "🔄 切換到代理..."
    kill $HEALTH_PID 2>/dev/null || true
    
    # 簡化代理
    exec socat TCP-LISTEN:8080,fork TCP:localhost:8081
) &

echo "✅ 服務啟動中..."
wait $HEALTH_PID
