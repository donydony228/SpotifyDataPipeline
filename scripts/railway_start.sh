#!/bin/bash

echo "🚀 Starting Airflow on Railway..."

export AIRFLOW_HOME=/app/airflow_home

cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
base_log_folder = /app/logs
logging_level = INFO
executor = LocalExecutor
sql_alchemy_conn = ${SUPABASE_DB_URL}
load_examples = False
fernet_key = ${AIRFLOW__CORE__FERNET_KEY}

[webserver]
web_server_port = 8080
secret_key = ${AIRFLOW__WEBSERVER__SECRET_KEY}
base_url = ${RAILWAY_PUBLIC_DOMAIN:+https://${RAILWAY_PUBLIC_DOMAIN}}
expose_config = True

[api]
auth_backend = airflow.api.auth.backend.basic_auth

[scheduler]
catchup_by_default = False

[logging]
logging_level = INFO
remote_logging = False
EOC

echo "🗃️  Initializing Airflow database..."
airflow db init

echo "👤 Creating admin user..."
airflow users create \
    --username admin \
    --firstname Data \
    --lastname Engineer \
    --role Admin \
    --email admin@jobdata.com \
    --password admin123 || echo "User already exists"

echo "🧪 Testing connections..."
python -c "
import os, psycopg2
from pymongo import MongoClient
from pymongo.server_api import ServerApi

try:
    conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
    conn.close()
    print('✅ Supabase OK')
    
    client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1'))
    client.admin.command('ping')
    client.close()
    print('✅ MongoDB OK')
except Exception as e:
    print(f'⚠️  Connection warning: {e}')
"

echo "📅 Starting Airflow Scheduler..."
airflow scheduler &

echo "⏳ Waiting 15 seconds..."
sleep 15

echo "🌐 Starting Airflow Webserver..."
exec airflow webserver --port 8080 --hostname 0.0.0.0
