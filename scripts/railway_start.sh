#!/bin/bash
# scripts/railway_start.sh
# 最終修復版 Railway 啟動腳本

echo "🚀 Starting Railway Airflow deployment..."

export AIRFLOW_HOME=/app/airflow_home

# 建立必要目錄
mkdir -p /app/airflow_home/logs /app/logs /tmp

# 先啟動健康檢查服務器
echo "🏥 Starting health check server..."
python3 /app/scripts/simple_health_server.py &
HEALTH_PID=$!

# 等待健康檢查服務器啟動
sleep 5

echo "🗃️ Initializing Airflow..."

# 檢查 Supabase 連線
echo "📊 Testing database connection..."
python3 -c "
import os
import psycopg2
from urllib.parse import urlparse

def test_supabase_connection():
    try:
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if not supabase_url:
            print('❌ SUPABASE_DB_URL not found')
            return False
        
        # 快速連線測試（5秒超時）
        conn = psycopg2.connect(supabase_url, connect_timeout=5)
        conn.close()
        print('✅ Supabase connection successful')
        return True
    except Exception as e:
        print(f'❌ Supabase connection failed: {str(e)}')
        return False

# 測試連線並決定資料庫策略
use_supabase = test_supabase_connection()
with open('/tmp/db_strategy.txt', 'w') as f:
    f.write('supabase' if use_supabase else 'sqlite')
print(f'Database strategy: {\"supabase\" if use_supabase else \"sqlite\"}')
"

# 讀取資料庫策略
DB_STRATEGY=$(cat /tmp/db_strategy.txt 2>/dev/null || echo "sqlite")

if [ "$DB_STRATEGY" = "supabase" ]; then
    echo "📊 Using Supabase PostgreSQL"
    DB_URL="${SUPABASE_DB_URL}"
else
    echo "📁 Using SQLite fallback"
    DB_URL="sqlite:////app/airflow_home/airflow.db"
fi

# 建立 Airflow 配置
echo "⚙️ Creating Airflow configuration..."
cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
logging_level = INFO
executor = LocalExecutor
load_examples = False
fernet_key = ${AIRFLOW__CORE__FERNET_KEY:-railway-fernet-key-32-chars-long!}

[database]
sql_alchemy_conn = ${DB_URL}

[webserver]
web_server_port = 8081
secret_key = ${AIRFLOW__WEBSERVER__SECRET_KEY:-railway-secret-key}
expose_config = True
authenticate = True

[api]
auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

[scheduler]
catchup_by_default = False

[logging]
logging_level = INFO
remote_logging = False
base_log_folder = /app/logs
processor_log_folder = /app/logs
EOC

# 啟動 Airflow 初始化（背景進行）
(
    echo "🗃️ Initializing Airflow database..."
    
    # 初始化資料庫
    airflow db init || {
        echo "⚠️ Database init failed, trying migration..."
        airflow db migrate || echo "Migration also failed, continuing..."
    }
    
    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@railway.app \
        --password admin123 || echo "User creation failed or already exists"
    
    echo "📅 Starting Airflow Scheduler..."
    airflow scheduler &
    SCHEDULER_PID=$!
    
    # 等待 scheduler 啟動
    sleep 15
    
    echo "🌐 Starting Airflow Webserver..."
    airflow webserver --port 8081 --hostname 0.0.0.0 &
    WEBSERVER_PID=$!
    
    # 等待 webserver 啟動
    sleep 30
    
    echo "🔄 Starting proxy..."
    # 停止健康檢查服務器
    kill $HEALTH_PID 2>/dev/null || true
    
    # 啟動代理服務器（8080 -> 8081）
    python3 -c "
import socket
import threading
import time
import sys

def proxy_connection(client_sock, target_host, target_port):
    try:
        target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        target_sock.settimeout(10)
        target_sock.connect((target_host, target_port))
        
        def forward(src, dst):
            try:
                while True:
                    data = src.recv(4096)
                    if not data:
                        break
                    dst.send(data)
            except:
                pass
            finally:
                try:
                    src.close()
                    dst.close()
                except:
                    pass
        
        t1 = threading.Thread(target=forward, args=(client_sock, target_sock))
        t2 = threading.Thread(target=forward, args=(target_sock, client_sock))
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()
        
    except Exception as e:
        try:
            client_sock.close()
        except:
            pass

def start_proxy():
    try:
        proxy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        proxy_sock.bind(('0.0.0.0', 8080))
        proxy_sock.listen(10)
        
        print('🔄 Proxy started: 8080 -> 8081')
        print('✅ Railway deployment ready!')
        
        while True:
            try:
                client_sock, addr = proxy_sock.accept()
                t = threading.Thread(target=proxy_connection, args=(client_sock, 'localhost', 8081))
                t.daemon = True
                t.start()
            except Exception as e:
                print(f'Proxy error: {e}')
                continue
                
    except Exception as e:
        print(f'Critical proxy error: {e}')
        sys.exit(1)

start_proxy()
"
    
) &

# 保持健康檢查服務器運行
echo "✅ Health check server running on port 8080"
echo "⏳ Airflow initializing in background..."
echo "🌐 Database: $DB_STRATEGY"

# 等待背景進程或健康檢查服務器
wait $HEALTH_PID 2>/dev/null || wait