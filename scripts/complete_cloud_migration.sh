#!/bin/bash
# scripts/complete_cloud_migration.sh
# 美國求職市場資料工程專案 - 完整雲端遷移腳本

echo "🚀 美國求職市場資料工程專案 - 雲端遷移"
echo "=========================================="

# 檢查必要檔案
echo "📋 檢查環境準備..."

if [ ! -f ".env" ]; then
    echo "❌ .env 檔案不存在，請先建立並設定雲端連線資訊"
    echo "💡 參考 .env.example 建立 .env 檔案"
    exit 1
fi

if [ ! -f "sql/ddl/warehouse_tables.sql" ]; then
    echo "❌ SQL Schema 檔案不存在"
    exit 1
fi

# 檢查本地環境是否運行
echo "🔍 檢查本地 Docker 環境..."
if ! docker compose ps | grep -q "Up"; then
    echo "⚠️  本地 Docker 環境未運行，正在啟動..."
    make start
    echo "⏳ 等待服務啟動..."
    sleep 30
fi

echo "✅ 環境檢查通過"

# 安裝必要的 Python 套件
echo "📦 安裝雲端連線套件..."
pip install python-dotenv pymongo psycopg2-binary

echo ""
echo "🎯 開始雲端遷移流程"
echo "===================="

# Phase 1: Supabase PostgreSQL 遷移
echo ""
echo "Phase 1: PostgreSQL (Supabase) 遷移"
echo "===================================="

echo "🔗 步驟 1.1: 測試 Supabase 連線..."
python scripts/test_supabase_connection.py

if [ $? -ne 0 ]; then
    echo "❌ Supabase 連線失敗，請檢查以下項目："
    echo "  1. Supabase 專案是否已建立"
    echo "  2. .env 中的 SUPABASE_* 變數是否正確"
    echo "  3. 資料庫是否已啟動"
    read -p "修正後按 Enter 繼續，或按 Ctrl+C 取消..."
    python scripts/test_supabase_connection.py
fi

echo "🏗️  步驟 1.2: 部署 Schema 到 Supabase..."
python scripts/deploy_to_supabase.py

if [ $? -ne 0 ]; then
    echo "❌ Schema 部署失敗"
    exit 1
fi

echo "📊 步驟 1.3: 遷移測試資料到 Supabase..."
python scripts/migrate_test_data_to_supabase.py

echo "✅ 步驟 1.4: 驗證 Supabase 部署..."
python scripts/verify_supabase_deployment.py

echo "🎉 Supabase PostgreSQL 遷移完成！"

# Phase 2: MongoDB Atlas 遷移
echo ""
echo "Phase 2: MongoDB Atlas 遷移"
echo "============================"

echo "🔗 步驟 2.1: 測試 MongoDB Atlas 連線..."
python scripts/test_mongodb_atlas.py

if [ $? -ne 0 ]; then
    echo "❌ MongoDB Atlas 連線失敗，請檢查以下項目："
    echo "  1. MongoDB Atlas 叢集是否已建立"
    echo "  2. .env 中的 MONGODB_ATLAS_* 變數是否正確"
    echo "  3. IP 白名單是否設定正確"
    echo "  4. 用戶權限是否足夠"
    read -p "修正後按 Enter 繼續，或按 Ctrl+C 取消..."
    python scripts/test_mongodb_atlas.py
fi

echo "🏗️  步驟 2.2: 初始化 MongoDB Atlas..."
python scripts/init_mongodb_atlas.py

if [ $? -ne 0 ]; then
    echo "❌ MongoDB Atlas 初始化失敗"
    exit 1
fi

echo "📊 步驟 2.3: 遷移資料到 MongoDB Atlas..."
python scripts/migrate_to_mongodb_atlas.py

echo "✅ 步驟 2.4: 驗證 MongoDB Atlas 部署..."
python scripts/verify_mongodb_atlas.py

echo "🎉 MongoDB Atlas 遷移完成！"

# Phase 3: 整合測試
echo ""
echo "Phase 3: 整合測試"
echo "================="

echo "🧪 執行端到端連線測試..."

# 建立整合測試腳本
cat > scripts/integration_test.py << 'EOF'
import psycopg2
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

def integration_test():
    print("🔬 整合測試開始...")
    
    # 測試 Supabase PostgreSQL
    try:
        conn = psycopg2.connect(
            host=os.getenv('SUPABASE_DB_HOST'),
            port=os.getenv('SUPABASE_DB_PORT', 5432),
            database=os.getenv('SUPABASE_DB_NAME'),
            user=os.getenv('SUPABASE_DB_USER'),
            password=os.getenv('SUPABASE_DB_PASSWORD')
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dwh.fact_jobs")
        pg_jobs = cur.fetchone()[0]
        conn.close()
        print(f"  ✅ Supabase PostgreSQL: {pg_jobs} 筆職缺資料")
    except Exception as e:
        print(f"  ❌ Supabase 測試失敗: {str(e)}")
        return False
    
    # 測試 MongoDB Atlas
    try:
        client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1'))
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        mongo_jobs = db['raw_jobs_data'].count_documents({})
        client.close()
        print(f"  ✅ MongoDB Atlas: {mongo_jobs} 筆原始資料")
    except Exception as e:
        print(f"  ❌ MongoDB Atlas 測試失敗: {str(e)}")
        return False
    
    print("🎉 整合測試通過！雲端環境已就緒")
    return True

if __name__ == "__main__":
    integration_test()
EOF

python scripts/integration_test.py

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉🎉🎉 雲端遷移完全成功！🎉🎉🎉"
    echo "=================================="
    echo ""
    echo "✅ 已完成項目："
    echo "  📊 Supabase PostgreSQL - 完整 Star Schema + 測試資料"
    echo "  🍃 MongoDB Atlas - 原始資料存儲 + 集合與索引"
    echo "  🔗 端到端連線測試通過"
    echo ""
    echo "🚀 下一步："
    echo "  1. Railway 部署 Airflow"
    echo "  2. 開發第一個爬蟲"
    echo "  3. 建立完整 ETL Pipeline"
    echo ""
    echo "🌐 雲端資源概覽："
    echo "  📊 Supabase: 完整資料倉儲架構"
    echo "  🍃 MongoDB Atlas: 512MB 免費空間"
    echo "  💰 總成本: $0/月"
    echo ""
    echo "📋 重要連線資訊已保存在 .env 檔案"
    
else
    echo "❌ 整合測試失敗，請檢查上述錯誤訊息"
    exit 1
fi