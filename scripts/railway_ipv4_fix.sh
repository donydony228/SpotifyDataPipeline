#!/bin/bash
# scripts/railway_ipv4_fix.sh
# Force IPv4 resolution for Supabase

echo "🔧 Applying IPv4 fix for Railway deployment..."

# 1. Create IPv4 resolution script
cat > scripts/force_ipv4_dns.py << 'EOF'
import socket
import os
from urllib.parse import urlparse

def get_ipv4_address(hostname):
    """Get IPv4 address for hostname"""
    try:
        # Force IPv4 only
        result = socket.getaddrinfo(hostname, None, socket.AF_INET)
        if result:
            return result[0][4][0]
    except:
        pass
    return None

def fix_supabase_url():
    """Convert Supabase URL to use IPv4 address"""
    original_url = os.getenv('SUPABASE_DB_URL')
    if not original_url:
        return None
    
    parsed = urlparse(original_url)
    hostname = parsed.hostname
    
    # Get IPv4 address
    ipv4 = get_ipv4_address(hostname)
    if ipv4:
        # Replace hostname with IPv4
        fixed_url = original_url.replace(hostname, ipv4)
        return fixed_url
    
    return original_url

if __name__ == "__main__":
    fixed = fix_supabase_url()
    if fixed:
        print(fixed)
EOF

# 2. Update Railway startup script
cat > scripts/railway_start_simple.sh << 'EOF'
#!/bin/bash
echo "🚀 Railway Start - IPv4 Fix Version"

export AIRFLOW_HOME=/app/airflow_home

# Get IPv4-fixed Supabase URL
FIXED_URL=$(python3 /app/scripts/force_ipv4_dns.py)

if [[ "$FIXED_URL" == *"postgresql"* ]]; then
    echo "✅ Using IPv4-fixed Supabase URL"
    DB_URL="$FIXED_URL"
else
    echo "📁 Falling back to SQLite"
    DB_URL="sqlite:////app/airflow_home/airflow.db"
fi

# Create Airflow config
mkdir -p $AIRFLOW_HOME
cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
executor = LocalExecutor
load_examples = False
fernet_key = railway-fernet-key-32-chars-long!

[database]
sql_alchemy_conn = $DB_URL

[webserver]
web_server_port = 8080
secret_key = railway-secret-key
EOC

# Initialize and start Airflow
echo "🗃️ Initializing Airflow..."
airflow db init

echo "👤 Creating user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@railway.app \
    --password admin123 || echo "User exists"

echo "🚀 Starting Airflow..."
airflow scheduler &
sleep 10
exec airflow webserver --port 8080 --hostname 0.0.0.0
EOF

chmod +x scripts/railway_start_simple.sh

# 3. Update Dockerfile
cat > Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ curl postgresql-client && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

RUN mkdir -p /app/logs /app/data /app/airflow_home

ENV AIRFLOW_HOME=/app/airflow_home

COPY scripts/railway_start_simple.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["/app/start.sh"]
EOF

echo "✅ IPv4 fix applied!"
echo ""
echo "📋 Next steps:"
echo "1. git add ."
echo "2. git commit -m 'Add IPv4 fix for Railway Supabase connection'"
echo "3. git push origin main"
echo "4. Redeploy on Railway"