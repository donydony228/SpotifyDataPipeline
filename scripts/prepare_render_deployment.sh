#!/bin/bash
# scripts/prepare_render_deployment.sh
# 美國求職市場資料工程專案 - Render 部署準備

echo "🚀 準備 Render 部署..."
echo "=========================="

# 1. 建立簡化的 Dockerfile (移除 Railway 特定的修復邏輯)
echo "📦 建立 Render 專用 Dockerfile..."

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

# 健康檢查 (Render 友善)
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
  CMD curl -f http://localhost:8080/health || curl -f http://localhost:8080/ || exit 1

# 啟動命令
CMD ["/app/start.sh"]
EOF

# 2. 建立 Render 專用啟動腳本 (簡化版本)
echo "🔧 建立 Render 啟動腳本..."

cat > scripts/render_start.sh << 'EOF'
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
EOF

chmod +x scripts/render_start.sh

# 3. 建立 Render 環境變數檔案
echo "📝 準備環境變數..."

cat > render_environment_variables.txt << 'ENVEOF'
# Render 環境變數設定
# 在 Render Dashboard > Environment 頁面設定這些變數

# Airflow 核心設定
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__CORE__FERNET_KEY=render-fernet-key-32-chars-long!!
AIRFLOW__WEBSERVER__SECRET_KEY=render-secret-key

# 部署標記
ENVIRONMENT=production
DEPLOYMENT_PLATFORM=render

# 雲端資料庫連線 (請從 .env 檔案複製實際值)
ENVEOF

# 從 .env 檔案取得實際的雲端連線資訊
if [ -f ".env" ]; then
    echo "🔍 從 .env 取得雲端連線資訊..."
    
    # 加入 Supabase 設定
    if grep -q "SUPABASE_DB_URL" .env; then
        echo "SUPABASE_DB_URL=$(grep SUPABASE_DB_URL .env | cut -d'=' -f2-)" >> render_environment_variables.txt
    fi
    
    # 加入 MongoDB 設定
    if grep -q "MONGODB_ATLAS_URL" .env; then
        echo "MONGODB_ATLAS_URL=$(grep MONGODB_ATLAS_URL .env | cut -d'=' -f2-)" >> render_environment_variables.txt
    fi
    
    if grep -q "MONGODB_ATLAS_DB_NAME" .env; then
        echo "MONGODB_ATLAS_DB_NAME=$(grep MONGODB_ATLAS_DB_NAME .env | cut -d'=' -f2-)" >> render_environment_variables.txt
    fi
    
    echo "✅ 雲端連線資訊已加入"
else
    echo "⚠️ .env 檔案不存在，請手動加入雲端連線資訊"
fi

# 4. 建立 Render 部署檢查清單
echo "📋 建立部署檢查清單..."

cat > RENDER_DEPLOYMENT_CHECKLIST.md << 'EOF'
# 🚀 Render 部署檢查清單

## ✅ 準備階段

### 本地準備
- [ ] 程式碼已推送到 GitHub
- [ ] Dockerfile 已更新為 Render 版本
- [ ] 啟動腳本已建立 (scripts/render_start.sh)
- [ ] 環境變數檔案已準備 (render_environment_variables.txt)

### 雲端資料庫確認
- [ ] Supabase PostgreSQL 正常運行
- [ ] MongoDB Atlas 正常運行
- [ ] 本地可以正常連接雲端資料庫

## 🌐 Render 部署步驟

### 1. 建立 Render 服務
1. 前往 https://render.com
2. 點擊 "New +" → "Web Service"
3. 連接你的 GitHub repository
4. 選擇 repository: `us-job-market-data-engineering`

### 2. 基本設定
- **Name**: `us-job-data-platform` (或你喜歡的名稱)
- **Environment**: `Docker`
- **Region**: `Oregon (US West)` 或 `Ohio (US East)`
- **Branch**: `main`

### 3. 進階設定
- **Dockerfile Path**: `./Dockerfile` (預設值)
- **Auto-Deploy**: `Yes`

### 4. 環境變數設定
在 "Environment" 頁面加入以下變數：

```
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__CORE__FERNET_KEY=render-fernet-key-32-chars-long!!
AIRFLOW__WEBSERVER__SECRET_KEY=render-secret-key
ENVIRONMENT=production
DEPLOYMENT_PLATFORM=render

# 從 render_environment_variables.txt 複製 Supabase 和 MongoDB 設定
SUPABASE_DB_URL=postgresql://postgres:[password]@db.xxx.supabase.co:5432/postgres
MONGODB_ATLAS_URL=mongodb+srv://...
MONGODB_ATLAS_DB_NAME=job_market_data
```

### 5. 部署
- 點擊 "Create Web Service"
- 等待建置和部署完成 (約 5-10 分鐘)

## 🔍 部署後驗證

### 檢查部署狀態
- [ ] 部署成功 (綠色狀態)
- [ ] 服務正在運行
- [ ] 沒有錯誤日誌

### 測試 Airflow
- [ ] 能夠存取 Airflow UI (`https://你的應用名.onrender.com`)
- [ ] 能夠登入 (admin / admin123)
- [ ] 可以看到 DAGs 列表
- [ ] hello_world_dag 存在並可執行

### 測試資料庫連線
- [ ] 檢查日誌中的資料庫連線狀態
- [ ] 如果使用 Supabase，確認連線成功
- [ ] 如果降級到 SQLite，確認正常運作

## 🚨 故障排除

### 常見問題
1. **部署失敗**
   - 檢查 Dockerfile 語法
   - 確認所有檔案都已推送到 GitHub

2. **服務無法啟動**
   - 檢查 Render 日誌
   - 確認環境變數設定正確

3. **無法存取 Airflow UI**
   - 確認服務正在運行
   - 檢查健康檢查狀態

4. **資料庫連線失敗**
   - 確認 Supabase URL 正確
   - 檢查網路連線日誌
   - 確認會自動降級到 SQLite

### 預期結果
✅ **成功標準**：
- Render 部署成功
- Airflow UI 可正常存取
- 管理員帳號可正常登入
- DAGs 可正常顯示和執行
- 資料庫連線正常 (Supabase 或 SQLite)

## 📞 下一步
部署成功後：
1. 測試現有 DAGs
2. 開發第一個爬蟲
3. 設定監控和告警
4. 逐步完善 ETL Pipeline
EOF

# 5. 測試連線 (可選)
echo "🧪 測試雲端資料庫連線..."
if [ -f ".env" ]; then
    python3 scripts/test_supabase_connection.py 2>/dev/null && echo "✅ Supabase 連線正常"
    python3 scripts/test_mongodb_atlas.py 2>/dev/null && echo "✅ MongoDB Atlas 連線正常"
fi

echo ""
echo "🎉 Render 部署準備完成！"
echo "=========================="
echo ""
echo "📁 建立的檔案："
echo "  ✅ Dockerfile (Render 優化版)"
echo "  ✅ scripts/render_start.sh"
echo "  ✅ render_environment_variables.txt"
echo "  ✅ RENDER_DEPLOYMENT_CHECKLIST.md"
echo ""
echo "🚀 下一步："
echo "  1. 檢查檔案內容"
echo "  2. git add ."
echo "  3. git commit -m 'Prepare Render deployment'"
echo "  4. git push origin main"
echo "  5. 前往 https://render.com 建立服務"
echo ""
echo "💡 重點："
echo "  - 相比 Railway，移除了複雜的 IPv6 修復邏輯"
echo "  - 智慧資料庫連線：優先 Supabase，失敗則用 SQLite"
echo "  - 簡化的健康檢查機制"
echo "  - Render 原生支援 PostgreSQL 外部連線"