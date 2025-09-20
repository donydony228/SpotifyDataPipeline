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
