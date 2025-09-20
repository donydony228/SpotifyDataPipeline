#!/bin/bash
echo "🔧 建立 Railway Supabase 修復檔案..."

# 1. 建立 IPv6 修復 Python 腳本
cat > scripts/fix_supabase_ipv6.py << 'EOF'
#!/usr/bin/env python3
import os
import socket
import requests
from urllib.parse import urlparse, urlunparse

def get_ipv4_for_supabase():
    host = "db.mzxadnjwgexlvhgleuwm.supabase.co"
    
    # 方法1: Google DNS API
    try:
        print("🔍 使用 Google DNS 查詢 IPv4...")
        url = f"https://dns.google/resolve?name={host}&type=A"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if 'Answer' in data:
            for answer in data['Answer']:
                if answer['type'] == 1:  # A record (IPv4)
                    print(f"✅ 找到 IPv4: {answer['data']}")
                    return answer['data']
    except Exception as e:
        print(f"❌ Google DNS 失敗: {e}")
    
    # 方法2: Cloudflare DNS API
    try:
        print("🔍 使用 Cloudflare DNS 查詢 IPv4...")
        url = f"https://cloudflare-dns.com/dns-query?name={host}&type=A"
        headers = {'Accept': 'application/dns-json'}
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        
        if 'Answer' in data:
            for answer in data['Answer']:
                if answer['type'] == 1:
                    print(f"✅ 找到 IPv4: {answer['data']}")
                    return answer['data']
    except Exception as e:
        print(f"❌ Cloudflare DNS 失敗: {e}")
    
    # 方法3: 嘗試已知 IP
    known_ips = ["52.209.78.15", "18.132.53.90", "3.123.75.248"]
    for ip in known_ips:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((ip, 5432))
            sock.close()
            if result == 0:
                print(f"✅ 測試 IP {ip} 可連接")
                return ip
        except:
            continue
    
    return None

def create_fixed_url():
    original_url = os.getenv('SUPABASE_DB_URL')
    if not original_url:
        print("❌ SUPABASE_DB_URL 未設定")
        return None
    
    parsed = urlparse(original_url)
    ipv4 = get_ipv4_for_supabase()
    
    if not ipv4:
        print("❌ 無法獲取 IPv4，使用原始 URL")
        return original_url + "?sslmode=require"
    
    # 建立新 URL
    new_netloc = f"{parsed.username}:{parsed.password}@{ipv4}:{parsed.port or 5432}"
    fixed_url = urlunparse((
        parsed.scheme, new_netloc, parsed.path, 
        parsed.params, "sslmode=require&connect_timeout=30", parsed.fragment
    ))
    
    print(f"🔧 修復 URL: postgresql://***@{ipv4}:5432/***")
    return fixed_url

if __name__ == "__main__":
    print("🔧 修復 Supabase IPv6 連線...")
    fixed_url = create_fixed_url()
    if fixed_url:
        with open('/tmp/supabase_fixed_url.txt', 'w') as f:
            f.write(fixed_url)
        print("✅ 修復 URL 已保存")
    else:
        print("❌ URL 修復失敗")
EOF

# 2. 建立修復版啟動腳本
cat > scripts/railway_start_fixed.sh << 'EOF'
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
EOF

chmod +x scripts/railway_start_fixed.sh

# 3. 更新 Dockerfile
cat > Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ curl postgresql-client \
    ca-certificates socat && \
    rm -rf /var/lib/apt/lists/* && \
    update-ca-certificates

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt requests

COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

RUN mkdir -p /app/logs /app/data /app/airflow_home

ENV AIRFLOW_HOME=/app/airflow_home

COPY scripts/railway_start_fixed.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080

HEALTHCHECK --interval=60s --timeout=30s --start-period=180s --retries=5 \
  CMD curl -f http://localhost:8080/health || curl -f http://localhost:8080/ || exit 1

CMD ["/app/start.sh"]
EOF

# 4. 建立環境變數檔案
cat > railway_env_fixed.txt << 'ENVEOF'
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__CORE__FERNET_KEY=railway-fernet-key-32-chars-long!
AIRFLOW__WEBSERVER__SECRET_KEY=railway-secret-key
ENVIRONMENT=production
ENVEOF

# 如果 .env 存在，加入 Supabase 設定
if [ -f ".env" ]; then
    echo "# Supabase 設定 (會自動修復 IPv6 問題)" >> railway_env_fixed.txt
    grep "SUPABASE_DB_URL" .env >> railway_env_fixed.txt 2>/dev/null || true
    grep "MONGODB_ATLAS_URL" .env >> railway_env_fixed.txt 2>/dev/null || true
    grep "MONGODB_ATLAS_DB_NAME" .env >> railway_env_fixed.txt 2>/dev/null || true
fi

echo "✅ 所有修復檔案已建立！"
echo ""
echo "📁 建立的檔案："
echo "  - scripts/fix_supabase_ipv6.py"
echo "  - scripts/railway_start_fixed.sh"
echo "  - Dockerfile (更新)"
echo "  - railway_env_fixed.txt"
echo ""
echo "🚀 現在執行："
echo "  git add ."
echo "  git commit -m 'Add Railway Supabase IPv6 fix'"
echo "  git push origin main"