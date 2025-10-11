#!/bin/bash
echo "🚀 Railway Start with Supabase IPv6 Fix"

export AIRFLOW_HOME=/app/airflow_home

# 啟動健康檢查
python /app/scripts/simple_health_server.py &
HEALTH_PID=$!
sleep 5

# 嘗試修復 Supabase 連線
echo "🔧 修復 Supabase IPv6 連線問題..."
python /app/scripts/fix_supabase_ipv6.py

# 讀取修復的 URL
if [ -f "/tmp/supabase_fixed_url.txt" ]; then
    FIXED_SUPABASE_URL=$(cat /tmp/supabase_fixed_url.txt)
    echo "✅ 使用修復的 Supabase URL"
    DB_URL="$FIXED_SUPABASE_URL"
else
    echo "❌ Supabase 修復失敗，使用 SQLite"
    DB_URL="sqlite:////app/airflow_home/airflow.db"
fi

# 建立 Airflow 配置
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
sql_alchemy_conn = $DB_URL

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

# 初始化 Airflow
(
    echo "🗃️ 初始化 Airflow..."
    if [[ "$DB_URL" == *"postgresql"* ]]; then
        echo "📊 使用 PostgreSQL (Supabase)"
        airflow db migrate || airflow db init
    else
        echo "📁 使用 SQLite"
        airflow db init
    fi
    
    echo "👤 建立用戶..."
    airflow users create \
        --username admin \
        --firstname Railway \
        --lastname Admin \
        --role Admin \
        --email admin@railway.app \
        --password admin123 || echo "用戶已存在"
    
    echo "📅 啟動 Scheduler..."
    airflow scheduler &
    
    sleep 20
    
    echo "🌐 啟動 Webserver..."
    airflow webserver --port 8081 --hostname 0.0.0.0 &
    
    sleep 30
    
    echo "🔄 啟動代理..."
    kill $HEALTH_PID 2>/dev/null || true
    
    # 簡單的 HTTP 代理
    python3 -c "
import socket, threading, sys

def handle_request(client_socket):
    try:
        # 轉發到 8081
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.connect(('localhost', 8081))
        
        def forward(source, destination):
            try:
                while True:
                    data = source.recv(4096)
                    if not data:
                        break
                    destination.send(data)
            except:
                pass
            finally:
                source.close()
                destination.close()
        
        # 雙向轉發
        threading.Thread(target=forward, args=(client_socket, server_socket)).start()
        threading.Thread(target=forward, args=(server_socket, client_socket)).start()
        
    except Exception as e:
        print(f'轉發錯誤: {e}')
        client_socket.close()

# 啟動代理服務器
try:
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    proxy_socket.bind(('0.0.0.0', 8080))
    proxy_socket.listen(5)
    print('🔄 代理啟動: 8080 -> 8081')
    
    while True:
        client_socket, addr = proxy_socket.accept()
        threading.Thread(target=handle_request, args=(client_socket,)).start()

except Exception as e:
    print(f'代理啟動失敗: {e}')
    sys.exit(1)
" &
    
    wait
) &

echo "✅ 健康檢查運行中..."
echo "🔧 Supabase IPv6 修復版本"
wait $HEALTH_PID
