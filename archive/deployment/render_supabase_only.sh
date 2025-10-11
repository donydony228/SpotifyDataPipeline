#!/bin/bash
# scripts/render_supabase_only.sh
# Render 部署 - 強制使用 Supabase，不提供 SQLite 備用方案

echo "🎯 Render + Supabase 強制連線版本"

# 1. 更新啟動腳本 - 只支援 Supabase
cat > scripts/render_start.sh << 'EOF'
#!/bin/bash
echo "🚀 Render + Supabase 部署"
echo "========================="

export AIRFLOW_HOME=/app/airflow_home

# 檢查必要環境變數
echo "🔍 檢查環境變數..."
if [ -z "$SUPABASE_DB_URL" ]; then
    echo "❌ 錯誤：SUPABASE_DB_URL 環境變數未設定"
    echo "請在 Render Dashboard 設定此環境變數"
    exit 1
fi

echo "✅ SUPABASE_DB_URL 已設定"

# 測試 Supabase 連線 - 必須成功
echo "🔗 測試 Supabase 連線..."
python3 -c "
import os
import psycopg2
import sys
from urllib.parse import urlparse

def test_supabase_connection():
    supabase_url = os.getenv('SUPABASE_DB_URL')
    
    try:
        print(f'🔍 連線到 Supabase...')
        
        # 解析 URL 顯示基本資訊（隱藏密碼）
        parsed = urlparse(supabase_url)
        masked_url = f'postgresql://{parsed.username}:***@{parsed.hostname}:{parsed.port}{parsed.path}'
        print(f'📍 目標: {masked_url}')
        
        # 嘗試連線，增加超時時間
        conn = psycopg2.connect(
            supabase_url,
            connect_timeout=30,
            application_name='render-airflow'
        )
        
        # 測試查詢
        cur = conn.cursor()
        cur.execute('SELECT version();')
        version = cur.fetchone()[0]
        print(f'✅ PostgreSQL 版本: {version[:50]}...')
        
        # 檢查是否有我們的 schema
        cur.execute(\"\"\"
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('dwh', 'raw_staging', 'clean_staging', 'business_staging')
        \"\"\")
        schemas = [row[0] for row in cur.fetchall()]
        if schemas:
            print(f'✅ 找到資料庫 Schema: {schemas}')
        else:
            print('⚠️ 未找到專案 Schema，但連線正常')
        
        conn.close()
        print('✅ Supabase 連線測試成功！')
        return True
        
    except psycopg2.OperationalError as e:
        print(f'❌ Supabase 連線失敗 (OperationalError): {e}')
        return False
    except Exception as e:
        print(f'❌ Supabase 連線失敗: {e}')
        return False

if not test_supabase_connection():
    print('')
    print('🚨 Supabase 連線失敗，無法繼續')
    print('請檢查：')
    print('1. SUPABASE_DB_URL 格式是否正確')
    print('2. Supabase 專案是否正常運行')
    print('3. 網路連線是否穩定')
    print('4. 資料庫密碼是否正確')
    sys.exit(1)
"

echo "📊 使用 Supabase PostgreSQL 作為唯一資料庫"

# 建立 Airflow 配置
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
sql_alchemy_conn = $SUPABASE_DB_URL

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
echo "📊 使用 Supabase PostgreSQL"

# 先嘗試遷移，失敗則初始化
if airflow db migrate; then
    echo "✅ 資料庫遷移成功"
else
    echo "⚠️ 遷移失敗，執行初始化..."
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

# 等待 Scheduler 啟動
echo "⏳ 等待 Scheduler 啟動..."
sleep 20

# 啟動 Airflow Webserver (前景執行)
echo "🌐 啟動 Airflow Webserver..."
echo "🎉 Render + Supabase 部署完成！"
echo "📍 存取 URL: https://你的應用名.onrender.com"
echo "👤 登入帳號: admin / admin123"
echo "📊 資料庫: Supabase PostgreSQL"

# 前景執行 webserver，讓容器保持運行
exec airflow webserver --port 8080 --hostname 0.0.0.0
EOF

chmod +x scripts/render_start.sh

# 2. 更新 Dockerfile - 移除不必要的複雜度
cat > Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式檔案
COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

# 建立必要目錄
RUN mkdir -p /app/logs /app/data /app/airflow_home

# 設定環境變數
ENV AIRFLOW_HOME=/app/airflow_home
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# 複製啟動腳本
COPY scripts/render_start.sh /app/start.sh
RUN chmod +x /app/start.sh

# 暴露端口
EXPOSE 8080

# 健康檢查 - 檢查 Airflow Webserver
HEALTHCHECK --interval=30s --timeout=10s --start-period=180s --retries=5 \
  CMD curl -f http://localhost:8080/health || exit 1

# 啟動命令
CMD ["/app/start.sh"]
EOF

# 3. 更新環境變數說明
cat > render_environment_variables.txt << 'ENVEOF'
# Render 環境變數設定 - Supabase 專用版本
# 在 Render Dashboard > Environment 頁面設定這些變數

# ===== 必要設定 =====

# Airflow 核心設定
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__CORE__FERNET_KEY=render-fernet-key-32-chars-long!!
AIRFLOW__WEBSERVER__SECRET_KEY=render-secret-key

# ===== 資料庫連線 (必須設定) =====

# Supabase PostgreSQL (必須設定，否則啟動失敗)
SUPABASE_DB_URL=postgresql://postgres:[你的密碼]@db.xxx.supabase.co:5432/postgres

# MongoDB Atlas (可選)
MONGODB_ATLAS_URL=mongodb+srv://[用戶名]:[密碼]@xxx.mongodb.net/?retryWrites=true&w=majority
MONGODB_ATLAS_DB_NAME=job_market_data

# ===== 部署標記 =====
ENVIRONMENT=production
DEPLOYMENT_PLATFORM=render

# ===== 重要說明 =====
# 1. SUPABASE_DB_URL 是必須的，沒有這個變數啟動會失敗
# 2. 請從你的本地 .env 檔案複製正確的連線字串
# 3. 確保 Supabase 專案正常運行
# 4. 如果連線失敗，檢查 Render 部署日誌
ENVEOF

echo ""
echo "✅ Render + Supabase 強制連線版本準備完成！"
echo "============================================="
echo ""
echo "🎯 主要變更："
echo "  ✅ 移除 SQLite 備用方案"
echo "  ✅ 強制要求 SUPABASE_DB_URL 環境變數"
echo "  ✅ 詳細的連線測試和錯誤訊息"
echo "  ✅ 啟動前必須確認 Supabase 連線成功"
echo ""
echo "🚀 下一步："
echo "  1. git add ."
echo "  2. git commit -m 'Force Supabase connection, remove SQLite fallback'"
echo "  3. git push origin main"
echo "  4. 在 Render 重新部署"
echo "  5. 確保 SUPABASE_DB_URL 環境變數正確設定"
echo ""
echo "⚠️ 重要："
echo "  - 必須在 Render 設定正確的 SUPABASE_DB_URL"
echo "  - 如果 Supabase 連線失敗，容器會立即退出"
echo "  - 這樣確保只有在 Supabase 正常時才啟動服務"