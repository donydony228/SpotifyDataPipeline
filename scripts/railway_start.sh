#!/bin/bash
# scripts/railway_start_fixed.sh
# 修復版 Railway 啟動腳本 - 處理 Supabase 連線問題

echo "🚀 Starting Railway deployment with fallback strategy..."

export AIRFLOW_HOME=/app/airflow_home

# 先啟動健康檢查服務器
echo "🏥 Starting health check server..."
python /app/scripts/simple_health_server.py &
HEALTH_PID=$!

# 等待健康檢查服務器啟動
sleep 5

echo "🗃️ Initializing Airflow with fallback strategy..."

# 檢查 Supabase 連線並決定資料庫策略
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
        
        # 解析 URL 檢查格式
        parsed = urlparse(supabase_url)
        if not all([parsed.hostname, parsed.username, parsed.password]):
            print('❌ SUPABASE_DB_URL format invalid')
            return False
            
        conn = psycopg2.connect(supabase_url)
        conn.close()
        print('✅ Supabase connection successful')
        return True
    except Exception as e:
        print(f'❌ Supabase connection failed: {str(e)}')
        return False

# 測試連線並寫入結果檔案
use_supabase = test_supabase_connection()
with open('/tmp/db_strategy.txt', 'w') as f:
    f.write('supabase' if use_supabase else 'sqlite')
"

# 讀取資料庫策略
DB_STRATEGY=$(cat /tmp/db_strategy.txt 2>/dev/null || echo "sqlite")

if [ "$DB_STRATEGY" = "supabase" ]; then
    echo "📊 Using Supabase PostgreSQL"
    DB_URL="${SUPABASE_DB_URL}"
else
    echo "📁 Using SQLite fallback (Supabase unavailable)"
    DB_URL="sqlite:////app/airflow_home/airflow.db"
fi

# 建立 Airflow 配置
cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
base_log_folder = /app/logs
logging_level = INFO
executor = LocalExecutor
load_examples = False
fernet_key = ${AIRFLOW__CORE__FERNET_KEY:-railway-fernet-key-32-chars-long!}

[database]
sql_alchemy_conn = ${DB_URL}

[webserver]
web_server_port = 8081
secret_key = ${AIRFLOW__WEBSERVER__SECRET_KEY:-railway-secret-key}
base_url = ${RAILWAY_PUBLIC_DOMAIN:+https://${RAILWAY_PUBLIC_DOMAIN}}
expose_config = True

[api]
auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

[scheduler]
catchup_by_default = False

[logging]
logging_level = INFO
remote_logging = False
base_log_folder = /app/logs
EOC

# 在背景初始化 Airflow
(
    echo "🗃️ Initializing Airflow database..."
    
    # 嘗試資料庫遷移，如果失敗則初始化
    if ! airflow db migrate 2>/dev/null; then
        echo "⚠️ Migration failed, trying init..."
        airflow db init
    fi
    
    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Data \
        --lastname Engineer \
        --role Admin \
        --email admin@railway.app \
        --password admin123 2>/dev/null || echo "ℹ️ User already exists"
    
    echo "🧪 Database setup complete"
    
    echo "📅 Starting Airflow Scheduler..."
    airflow scheduler &
    SCHEDULER_PID=$!
    
    # 等待一下
    sleep 20
    
    echo "🌐 Starting Airflow Webserver on port 8081..."
    airflow webserver --port 8081 --hostname 0.0.0.0 &
    WEBSERVER_PID=$!
    
    # 等待 Airflow 完全啟動
    sleep 30
    
    echo "🔄 Switching to Airflow..."
    # 殺掉健康檢查服務器
    kill $HEALTH_PID 2>/dev/null || true
    
    # 啟動代理，將 8080 的請求轉發到 8081
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
        
        def forward(src, dst, direction):
            try:
                while True:
                    data = src.recv(4096)
                    if not data:
                        break
                    dst.send(data)
            except Exception as e:
                pass
            finally:
                try:
                    src.close()
                    dst.close()
                except:
                    pass
        
        t1 = threading.Thread(target=forward, args=(client_sock, target_sock, 'c2s'))
        t2 = threading.Thread(target=forward, args=(target_sock, client_sock, 's2c'))
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()
        
        # 等待轉發完成
        t1.join(timeout=300)
        t2.join(timeout=300)
        
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
        proxy_sock.listen(5)
        
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
                break
                
    except Exception as e:
        print(f'Proxy setup failed: {e}')
        sys.exit(1)

if __name__ == '__main__':
    start_proxy()
" &
    
    # 等待代理啟動
    wait
    
) &

# 保持健康檢查服務器運行直到 Airflow 準備就緒
echo "✅ Health check server is running on port 8080"
echo "⏳ Airflow is initializing in the background..."
echo "🌐 Database strategy: $DB_STRATEGY"

# 等待健康檢查服務器或背景進程完成
wait $HEALTH_PID