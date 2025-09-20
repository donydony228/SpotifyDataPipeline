#!/bin/bash
echo "🚀 Render Deployment - US Job Data Engineering Platform"

export AIRFLOW_HOME=/app/airflow_home

# 測試 Supabase 連線
echo "🔗 測試資料庫連線..."
python3 -c "
import os
import psycopg2

try:
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if supabase_url:
        conn = psycopg2.connect(supabase_url)
        conn.close()
        print('✅ Supabase 連線成功')
        with open('/tmp/db_status.txt', 'w') as f:
            f.write('supabase_ok')
    else:
        print('⚠️ SUPABASE_DB_URL 未設定')
        with open('/tmp/db_status.txt', 'w') as f:
            f.write('sqlite_fallback')
except Exception as e:
    print(f'❌ Supabase 連線失敗: {e}')
    print('📁 使用 SQLite 備用')
    with open('/tmp/db_status.txt', 'w') as f:
        f.write('sqlite_fallback')
"

# 根據連線測試結果設定資料庫
DB_STATUS=$(cat /tmp/db_status.txt 2>/dev/null || echo "sqlite_fallback")

if [ "$DB_STATUS" = "supabase_ok" ]; then
    echo "📊 使用 Supabase PostgreSQL"
    DB_URL="$SUPABASE_DB_URL"
else
    echo "📁 使用 SQLite 備用資料庫"
    DB_URL="sqlite:////app/airflow_home/airflow.db"
fi

# 建立 Airflow 配置檔案
echo "⚙️ 建立 Airflow 配置..."
mkdir -p $AIRFLOW_HOME

cat > $AIRFLOW_HOME/airflow.cfg << EOC
[core]
dags_folder = /app/dags
base_log_folder = /app/logs
logging_level = INFO
executor = LocalExecutor
load_examples = False
fernet_key = ${AIRFLOW__CORE__FERNET_KEY:-render-fernet-key-32-chars-long!!}

[database]
sql_alchemy_conn = $DB_URL

[webserver]
web_server_port = 8080
secret_key = ${AIRFLOW__WEBSERVER__SECRET_KEY:-render-secret-key}
expose_config = True
base_url = http://0.0.0.0:8080

[api]
auth_backends = airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

[scheduler]
catchup_by_default = False

[logging]
logging_level = INFO
remote_logging = False
base_log_folder = /app/logs
EOC

# 初始化 Airflow 資料庫
echo "🗃️ 初始化 Airflow 資料庫..."
if [[ "$DB_URL" == *"postgresql"* ]]; then
    echo "📊 使用 PostgreSQL，嘗試遷移..."
    airflow db migrate || airflow db init
else
    echo "📁 使用 SQLite，執行初始化..."
    airflow db init
fi

# 建立管理員用戶
echo "👤 建立管理員用戶..."
airflow users create \
    --username admin \
    --firstname Render \
    --lastname Admin \
    --role Admin \
    --email admin@render.com \
    --password admin123 \
    --verbose || echo "ℹ️ 用戶可能已存在"

# 啟動 Airflow Scheduler (背景執行)
echo "📅 啟動 Airflow Scheduler..."
airflow scheduler &
SCHEDULER_PID=$!

# 等待一段時間讓 Scheduler 啟動
sleep 15

# 啟動 Airflow Webserver (前景執行)
echo "🌐 啟動 Airflow Webserver..."
echo "🎉 Render 部署完成！存取 URL: https://你的應用名.onrender.com"
echo "👤 登入帳號: admin / admin123"

# 前景執行 webserver，讓容器保持運行
exec airflow webserver --port 8080 --hostname 0.0.0.0
