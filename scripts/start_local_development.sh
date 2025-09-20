#!/bin/bash
# scripts/start_local_development.sh
# 本地開發環境啟動腳本 - 連接雲端資料庫

echo "🚀 美國求職市場資料工程 - 本地開發環境"
echo "==========================================="

# 檢查 .env 檔案
if [ ! -f ".env" ]; then
    echo "❌ .env 檔案不存在"
    echo "💡 請執行: cp .env.example .env 並設定雲端資料庫連線"
    exit 1
fi

# 檢查 Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安裝"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker 未運行，請啟動 Docker Desktop"
    exit 1
fi

echo "✅ Docker 環境正常"

# 測試雲端資料庫連線
echo "🔍 測試雲端資料庫連線..."

# 測試 Supabase
if command -v python3 &> /dev/null; then
    python3 -c "
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if supabase_url:
        conn = psycopg2.connect(supabase_url, connect_timeout=10)
        conn.close()
        print('✅ Supabase 連線正常')
    else:
        print('⚠️ SUPABASE_DB_URL 未設定')
except Exception as e:
    print(f'❌ Supabase 連線失敗: {e}')
    print('⚠️ 將使用本地 PostgreSQL DWH')
" 2>/dev/null || echo "⚠️ 無法測試 Supabase（缺少 python 依賴）"

    # 測試 MongoDB Atlas
    python3 -c "
import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

try:
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    if mongodb_url:
        client = MongoClient(mongodb_url, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        print('✅ MongoDB Atlas 連線正常')
    else:
        print('⚠️ MONGODB_ATLAS_URL 未設定')
except Exception as e:
    print(f'❌ MongoDB Atlas 連線失敗: {e}')
    print('⚠️ 將使用本地 MongoDB')
" 2>/dev/null || echo "⚠️ 無法測試 MongoDB Atlas（缺少 python 依賴）"
fi

# 啟動本地服務
echo "🐳 啟動本地 Docker 服務..."

# 停止可能已運行的服務
docker compose down 2>/dev/null

# 啟動服務
docker compose up -d

echo "⏳ 等待服務啟動..."
sleep 30

# 檢查服務狀態
echo "🔍 檢查服務狀態..."
docker compose ps

# 檢查 Airflow 是否就緒
echo "🌊 等待 Airflow 就緒..."
for i in {1..12}; do
    if curl -f http://localhost:8080/health &>/dev/null; then
        echo "✅ Airflow 已就緒"
        break
    fi
    echo "  等待中... ($i/12)"
    sleep 10
done

# 顯示存取資訊
echo ""
echo "🎉 本地開發環境啟動完成！"
echo "=============================="
echo ""
echo "🌐 服務存取 URL："
echo "  📊 Airflow UI:      http://localhost:8080"
echo "  🏛️ PostgreSQL DWH:  localhost:5433 (dwh_user/dwh_password)"
echo "  🍃 MongoDB:         localhost:27017 (admin/admin123)"
echo "  ⚡ Redis:           localhost:6379"
echo "  🗄️ MinIO Console:   http://localhost:9001 (minioadmin/minioadmin123)"
echo "  📈 Grafana:         http://localhost:3000 (admin/admin123)"
echo ""
echo "👤 Airflow 登入："
echo "  用戶名: admin"
echo "  密碼: admin123"
echo ""
echo "☁️ 雲端資料庫："
echo "  📊 Supabase (目標 DWH): 已設定"
echo "  🍃 MongoDB Atlas (原始資料): 已設定"
echo ""
echo "📋 下一步："
echo "  1. 打開 http://localhost:8080"
echo "  2. 執行 'local_development_test' DAG"
echo "  3. 檢查雲端資料庫連線狀態"
echo "  4. 開始開發你的爬蟲！"
echo ""
echo "🛠️ 開發指令："
echo "  make logs                    # 查看服務日誌"
echo "  make stop                    # 停止所有服務"
echo "  make cloud-test              # 測試雲端連線"
echo "  make check-local-data        # 檢查本地資料"