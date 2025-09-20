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
