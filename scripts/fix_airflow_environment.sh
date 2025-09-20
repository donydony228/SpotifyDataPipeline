#!/bin/bash
# scripts/fix_airflow_environment.sh
# 修復 Airflow 無法讀取 .env 檔案的問題

echo "🔧 修復 Airflow 環境變數問題"
echo "============================="

# 檢查 .env 檔案是否存在
if [ ! -f ".env" ]; then
    echo "❌ .env 檔案不存在於當前目錄"
    echo "請確保 .env 檔案存在並包含正確的雲端資料庫連線資訊"
    exit 1
fi

echo "✅ 找到 .env 檔案"

# 檢查 .env 內容
echo "🔍 檢查 .env 內容..."
required_vars=("SUPABASE_DB_URL" "MONGODB_ATLAS_URL" "MONGODB_ATLAS_DB_NAME")
missing_vars=()

for var in "${required_vars[@]}"; do
    if grep -q "^${var}=" .env; then
        value=$(grep "^${var}=" .env | cut -d'=' -f2-)
        if [ -n "$value" ] && [ "$value" != "[你的密碼]" ] && [ "$value" != "[password]" ]; then
            echo "✅ $var: 已設定"
        else
            echo "❌ $var: 值為空或是範例值"
            missing_vars+=("$var")
        fi
    else
        echo "❌ $var: 未找到"
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  發現 ${#missing_vars[@]} 個缺少或無效的環境變數:"
    for var in "${missing_vars[@]}"; do
        echo "   - $var"
    done
    echo ""
    echo "請確保這些變數在 .env 檔案中正確設定"
    read -p "修正後按 Enter 繼續，或按 Ctrl+C 取消..."
fi

# 方法1: 複製 .env 到 Airflow 容器中
echo ""
echo "🔧 方法1: 複製 .env 到 Airflow 容器..."

if docker compose ps | grep -q "airflow-webserver.*Up"; then
    echo "📋 Airflow 容器正在運行，複製 .env 檔案..."
    
    # 複製到多個可能的位置
    docker compose exec airflow-webserver mkdir -p /opt/airflow || true
    docker compose cp .env airflow-webserver:/opt/airflow/.env
    docker compose cp .env airflow-webserver:/app/.env
    
    echo "✅ .env 檔案已複製到 Airflow 容器"
    
    # 驗證複製結果
    echo "🔍 驗證複製結果..."
    docker compose exec airflow-webserver ls -la /opt/airflow/.env || echo "❌ /opt/airflow/.env 不存在"
    docker compose exec airflow-webserver ls -la /app/.env || echo "❌ /app/.env 不存在"
else
    echo "⚠️  Airflow 容器未運行，跳過容器內複製"
fi

# 方法2: 更新 docker-compose.yml 加入環境變數
echo ""
echo "🔧 方法2: 更新 docker-compose.yml..."

# 備份原始檔案
cp docker-compose.yml docker-compose.yml.backup

# 檢查是否已經有 env_file 設定
if grep -q "env_file:" docker-compose.yml; then
    echo "✅ docker-compose.yml 已經包含 env_file 設定"
else
    echo "📝 更新 docker-compose.yml 加入 env_file..."
    
    # 建立臨時檔案
    temp_file=$(mktemp)
    
    # 在 airflow 相關服務中加入 env_file
    awk '
    /^  airflow-webserver:|^  airflow-scheduler:/ {
        service_name = $1
        print $0
        in_service = 1
        next
    }
    in_service && /^  [a-zA-Z]/ && !/^    / {
        in_service = 0
    }
    in_service && /^    environment:/ {
        print $0
        print "    env_file:"
        print "      - .env"
        next
    }
    in_service && /^    volumes:/ && !env_added {
        print "    env_file:"
        print "      - .env"
        env_added = 1
    }
    { print $0 }
    ' docker-compose.yml > "$temp_file"
    
    mv "$temp_file" docker-compose.yml
    echo "✅ docker-compose.yml 已更新"
fi

# 方法3: 建立環境變數設定腳本
echo ""
echo "🔧 方法3: 建立環境變數設定腳本..."

cat > scripts/set_airflow_env.sh << 'EOF'
#!/bin/bash
# 為 Airflow 設定環境變數

if [ -f ".env" ]; then
    echo "📝 載入 .env 檔案中的環境變數..."
    export $(grep -v '^#' .env | xargs)
    
    echo "✅ 環境變數已設定:"
    echo "   SUPABASE_DB_URL: ${SUPABASE_DB_URL:0:30}***"
    echo "   MONGODB_ATLAS_URL: ${MONGODB_ATLAS_URL:0:30}***"
    echo "   MONGODB_ATLAS_DB_NAME: $MONGODB_ATLAS_DB_NAME"
else
    echo "❌ .env 檔案不存在"
    exit 1
fi
EOF

chmod +x scripts/set_airflow_env.sh
echo "✅ 環境變數設定腳本已建立: scripts/set_airflow_env.sh"

# 方法4: 建立修復版 DAG
echo ""
echo "🔧 方法4: 部署修復版 DAG..."

# 檢查修復版 DAG 是否存在
if [ -f "dags/scrapers/linkedin_mock_scraper_env_fixed.py" ]; then
    echo "✅ 環境變數修復版 DAG 已存在"
else
    echo "📝 建立環境變數修復版 DAG..."
    echo "請從 Claude 的回應中複製 linkedin_mock_scraper_env_fixed.py 到 dags/scrapers/ 目錄"
fi

# 重啟 Airflow 服務
echo ""
echo "🔄 重啟 Airflow 服務以載入新設定..."
read -p "是否重啟 Airflow 服務？(y/n): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🔄 重啟 Airflow 服務..."
    docker compose restart airflow-webserver airflow-scheduler
    
    echo "⏳ 等待服務重新啟動..."
    sleep 30
    
    # 檢查服務狀態
    if docker compose ps | grep -q "airflow-webserver.*Up"; then
        echo "✅ Airflow Webserver 已重啟"
    else
        echo "❌ Airflow Webserver 重啟失敗"
    fi
    
    if docker compose ps | grep -q "airflow-scheduler.*Up"; then
        echo "✅ Airflow Scheduler 已重啟"
    else
        echo "❌ Airflow Scheduler 重啟失敗"
    fi
fi

# 測試環境變數
echo ""
echo "🧪 測試環境變數載入..."

if command -v python3 &> /dev/null; then
    python3 -c "
import os
from dotenv import load_dotenv

print('🔍 測試本地環境變數載入...')

# 載入 .env
load_dotenv()

vars_to_check = ['SUPABASE_DB_URL', 'MONGODB_ATLAS_URL', 'MONGODB_ATLAS_DB_NAME']
for var in vars_to_check:
    value = os.getenv(var)
    if value:
        masked = f'{value[:20]}***' if len(value) > 20 else '***'
        print(f'✅ {var}: {masked}')
    else:
        print(f'❌ {var}: 未設定')

print('\\n🔗 測試資料庫連線...')
try:
    import psycopg2
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if supabase_url:
        conn = psycopg2.connect(supabase_url, connect_timeout=10)
        conn.close()
        print('✅ Supabase 連線成功')
    else:
        print('❌ Supabase URL 未設定')
except Exception as e:
    print(f'❌ Supabase 連線失敗: {e}')

try:
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    if mongodb_url:
        client = MongoClient(mongodb_url, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        print('✅ MongoDB Atlas 連線成功')
    else:
        print('❌ MongoDB Atlas URL 未設定')
except Exception as e:
    print(f'❌ MongoDB Atlas 連線失敗: {e}')
"
else
    echo "⚠️  Python3 不可用，跳過連線測試"
fi

echo ""
echo "📋 修復摘要"
echo "============"
echo "✅ 已執行的修復步驟:"
echo "   1. 檢查 .env 檔案內容"
echo "   2. 複製 .env 到 Airflow 容器 (如果運行中)"
echo "   3. 更新 docker-compose.yml 加入 env_file"
echo "   4. 建立環境變數設定腳本"
echo "   5. 重啟 Airflow 服務 (如果選擇)"
echo ""
echo "🚀 下一步測試:"
echo "   1. 前往 Airflow UI: http://localhost:8080"
echo "   2. 查找 'linkedin_mock_scraper_env_fixed' DAG"
echo "   3. 手動觸發執行"
echo "   4. 檢查是否能正確讀取環境變數"
echo ""
echo "📞 如果問題持續:"
echo "   1. 檢查 .env 檔案格式 (無空格、無引號)"
echo "   2. 確認雲端資料庫 URL 正確"
echo "   3. 查看 Airflow 容器日誌: docker compose logs airflow-webserver"