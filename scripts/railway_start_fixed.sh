#!/bin/bash
# Railway 啟動腳本 - 修復 Supabase IPv6 連線問題

echo "🚀 Starting Railway deployment with Supabase IPv4 fix..."

export AIRFLOW_HOME=/app/airflow_home

# 先啟動健康檢查服務器
echo "🏥 Starting health check server..."
python /app/scripts/simple_health_server.py &
HEALTH_PID=$!

# 等待健康檢查服務器啟動
sleep 5

echo "🗃️ Initializing Airflow with IPv4 Supabase connection..."

# 修復 Supabase 連線 - 強制解析為 IPv4
python3 -c "
import os
import psycopg2
import socket
from urllib.parse import urlparse, urlunparse

def fix_supabase_url():
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if not supabase_url:
        print('❌ SUPABASE_DB_URL not found')
        return None
    
    try:
        # 解析 URL
        parsed = urlparse(supabase_url)
        host = parsed.hostname
        
        # 強制解析為 IPv4
        print(f'🔍 Resolving {host} to IPv4...')
        ipv4_address = socket.getaddrinfo(host, None, socket.AF_INET)[0][4][0]
        print(f'✅ IPv4 address: {ipv4_address}')
        
        # 重建 URL 使用 IPv4 地址
        new_netloc = f'{parsed.username}:{parsed.password}@{ipv4_address}:{parsed.port}'
        fixed_url = urlunparse((
            parsed.scheme,
            new_netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        # 測試連線
        conn = psycopg2.connect(fixed_url)
        conn.close()
        print('✅ IPv4 Supabase connection successful')
        
        return fixed_url
        
    except Exception as e:
        print(f'❌ IPv4 connection failed: {str(e)}')
        # 嘗試 SSL 模式
        try:
            ssl_url = supabase_url + '?sslmode=require'
            conn = psycopg2.connect(ssl_url)
            conn.close()
            print('✅ SSL mode connection successful')
            return ssl_url
        except Exception as e2:
            print(f'❌ SSL mode also failed: {str(e2)}')
            return None

# 取得修復的 URL
fixed_url = fix_supabase_url()
if fixed_url:
    with open('/tmp/fixed_db_url.txt', 'w') as f:
        f.write(fixed_url)
    print('✅ Fixed URL saved')
else:
    print('❌ Using SQLite fallback')
    with open('/tmp/fixed_db_url.txt', 'w') as f:
        f.write('sqlite:////app/airflow_home/airflow.db')
"

# 讀取修復的 URL
if [ -f "/tmp/fixed_db_url.txt" ]; then
    FIXED_DB_URL=$(cat /tmp/fixed_db_url.txt)
    echo "📊 Using database: $(echo $FIXED_DB_URL | sed 's/:[^:]*@/:***@/')"
else
    FIXED_DB_URL="sqlite:////app/airflow_home/airflow.db"
    echo "📁 Using SQLite fallback"
fi

# 建立 Airflow 配置
cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
base_log_folder = /app/logs
logging_level = INFO
executor = LocalExecutor
load_examples = False
fernet_key = \${AIRFLOW__CORE__FERNET_KEY:-railway-fernet-key-32-chars-long!}

[database]
sql_alchemy_conn = \${FIXED_DB_URL}

[webserver]
web_server_port = 8081
secret_key = \${AIRFLOW__WEBSERVER__SECRET_KEY:-railway-secret-key}
base_url = \${RAILWAY_PUBLIC_DOMAIN:+https://\${RAILWAY_PUBLIC_DOMAIN}}
expose_config = True

[api]
auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

[scheduler]
catchup_by_default = False

[logging]
logging_level = INFO
remote_logging = False
base_log_folder = /app/logs

[celery]
worker_concurrency = 2

[smtp]
smtp_host = localhost
smtp_starttls = True
smtp_ssl = False
smtp_port = 587
smtp_mail_from = noreply@railway.app
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
    SCHEDULER_PID=\$!
    
    # 等待一下
    sleep 20
    
    echo "🌐 Starting Airflow Webserver on port 8081..."
    airflow webserver --port 8081 --hostname 0.0.0.0 &
    WEBSERVER_PID=\$!
    
    # 等待 Airflow 完全啟動
    sleep 30
    
    echo "🔄 Switching to Airflow..."
    # 殺掉健康檢查服務器
    kill \$HEALTH_PID 2>/dev/null || true
    
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
        
        # 等待轉發完成
        t1.join(timeout=60)
        t2.join(timeout=60)
        
    except Exception:
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
            except Exception:
                break
                
    except Exception as e:
        print(f'❌ Proxy setup failed: {e}')
        # 作為備用，嘗試重新啟動健康檢查
        import time
        time.sleep(60)

if __name__ == '__main__':
    start_proxy()
" &
    
    # 等待
    wait
    
) &

# 保持健康檢查服務器運行
echo "✅ Health check server running on port 8080"
echo "⏳ Airflow initializing with fixed Supabase connection..."
echo "🌐 Fixed database URL configured"

# 等待
wait $HEALTH_PID
