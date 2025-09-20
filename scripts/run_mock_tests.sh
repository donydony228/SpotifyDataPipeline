#!/bin/bash
# scripts/run_mock_tests.sh
# 執行模擬測試的完整腳本

echo "🧪 LinkedIn 爬蟲模擬測試執行腳本"
echo "=================================="

# 檢查環境
check_environment() {
    echo "🔍 檢查測試環境..."
    
    # 檢查 Python 環境
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python3 未安裝"
        exit 1
    fi
    
    # 檢查必要套件
    python3 -c "import requests, bs4, pymongo, psycopg2" 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "⚠️  缺少必要 Python 套件，正在安裝..."
        pip install requests beautifulsoup4 pymongo psycopg2-binary lxml
    fi
    
    # 檢查 Docker 環境
    if ! docker compose ps | grep -q "Up"; then
        echo "⚠️  本地 Docker 環境未運行"
        echo "請執行: make start"
        read -p "是否現在啟動？(y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            make start
            echo "⏳ 等待服務啟動..."
            sleep 30
        else
            echo "❌ 測試需要本地環境運行"
            exit 1
        fi
    fi
    
    echo "✅ 環境檢查通過"
}

# 部署測試檔案
deploy_test_files() {
    echo "📁 部署測試檔案..."
    
    # 檢查檔案是否存在
    if [ ! -f "src/scrapers/mock_linkedin_scraper.py" ]; then
        echo "❌ 缺少模擬爬蟲檔案"
        echo "請確保已經建立 src/scrapers/mock_linkedin_scraper.py"
        exit 1
    fi
    
    if [ ! -f "dags/scrapers/linkedin_mock_scraper_dag.py" ]; then
        echo "❌ 缺少模擬測試 DAG"
        echo "請確保已經建立 dags/scrapers/linkedin_mock_scraper_dag.py"
        exit 1
    fi
    
    echo "✅ 測試檔案已就位"
}

# 檢查 Airflow DAG
check_airflow_dag() {
    echo "🌊 檢查 Airflow DAG 狀態..."
    
    # 等待 Airflow 完全啟動
    echo "⏳ 等待 Airflow 啟動..."
    for i in {1..12}; do
        if curl -f http://localhost:8080/health &>/dev/null; then
            echo "✅ Airflow 已就緒"
            break
        fi
        echo "   等待中... ($i/12)"
        sleep 10
    done
    
    # 檢查 DAG 是否出現
    echo "🔍 檢查測試 DAG 是否載入..."
    
    # 給 Airflow 一些時間來載入 DAG
    sleep 10
    
    echo "💡 請手動確認 DAG 狀態:"
    echo "   1. 前往 http://localhost:8080"
    echo "   2. 登入 (admin/admin123)"
    echo "   3. 查找 'linkedin_mock_scraper_test' DAG"
    echo "   4. 確認 DAG 沒有錯誤 (沒有紅色錯誤標記)"
    
    read -p "確認 DAG 正常載入後按 Enter 繼續..."
}

# 執行模擬測試
run_mock_test() {
    echo "🚀 執行模擬測試..."
    
    echo "請在 Airflow UI 中手動觸發測試:"
    echo "1. 前往 http://localhost:8080"
    echo "2. 找到 'linkedin_mock_scraper_test' DAG"
    echo "3. 點擊 DAG 進入詳細頁面"
    echo "4. 點擊右上角的 'Trigger DAG' 按鈕"
    echo "5. 觀察所有 Task 的執行狀況"
    
    echo ""
    echo "📊 預期執行時間: 2-3 分鐘"
    echo "📋 預期結果:"
    echo "   - 7 個 Task 全部成功 (綠色)"
    echo "   - 生成約 15 筆模擬職缺資料"
    echo "   - 資料同時儲存到 MongoDB 和 PostgreSQL"
    
    read -p "測試執行完成後按 Enter 繼續..."
}

# 驗證測試結果
validate_results() {
    echo "🔍 驗證測試結果..."
    
    if [ -f "scripts/validate_mock_test_results.py" ]; then
        echo "執行自動驗證腳本..."
        python3 scripts/validate_mock_test_results.py
        
        if [ $? -eq 0 ]; then
            echo "🎉 自動驗證通過！"
        else
            echo "⚠️  自動驗證發現問題，請檢查輸出"
        fi
    else
        echo "⚠️  自動驗證腳本不存在，請手動檢查"
        manual_verification
    fi
}

# 手動驗證指引
manual_verification() {
    echo "📋 手動驗證步驟:"
    echo ""
    echo "1. 檢查 MongoDB Atlas:"
    echo "   - 登入 MongoDB Atlas"
    echo "   - 進入 job_market_data 資料庫"
    echo "   - 查看 raw_jobs_data 集合"
    echo "   - 確認有新的文檔，且 metadata.is_mock_data = true"
    echo ""
    echo "2. 檢查 Supabase PostgreSQL:"
    echo "   - 登入 Supabase Dashboard"
    echo "   - 進入 SQL Editor"
    echo "   - 執行: SELECT COUNT(*) FROM raw_staging.linkedin_jobs_raw WHERE 'mock_data' = ANY(data_quality_flags);"
    echo "   - 確認有新的記錄"
    echo ""
    echo "3. 檢查 Airflow 日誌:"
    echo "   - 在 Airflow UI 中查看每個 Task 的日誌"
    echo "   - 確認沒有錯誤訊息"
    echo "   - 查看最終的測試評估結果"
}

# 清理測試資料
cleanup_test_data() {
    echo "🧹 清理測試資料..."
    
    read -p "是否要清理模擬測試資料？(y/n): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "正在清理模擬資料..."
        
        # 清理 MongoDB 模擬資料
        python3 -c "
import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

try:
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    if mongodb_url:
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        result = db['raw_jobs_data'].delete_many({'metadata.is_mock_data': True})
        print(f'MongoDB: 刪除了 {result.deleted_count} 筆模擬資料')
        client.close()
except Exception as e:
    print(f'MongoDB 清理失敗: {e}')
"
        
        # 清理 PostgreSQL 模擬資料
        python3 -c "
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if supabase_url:
        conn = psycopg2.connect(supabase_url)
        cur = conn.cursor()
        cur.execute(\"DELETE FROM raw_staging.linkedin_jobs_raw WHERE 'mock_data' = ANY(data_quality_flags)\")
        deleted_count = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f'PostgreSQL: 刪除了 {deleted_count} 筆模擬資料')
except Exception as e:
    print(f'PostgreSQL 清理失敗: {e}')
"
        
        echo "✅ 模擬資料清理完成"
    else
        echo "保留模擬資料"
    fi
}

# 主執行流程
main() {
    echo "開始執行模擬測試..."
    echo ""
    
    check_environment
    echo ""
    
    deploy_test_files
    echo ""
    
    check_airflow_dag
    echo ""
    
    run_mock_test
    echo ""
    
    validate_results
    echo ""
    
    cleanup_test_data
    echo ""
    
    echo "🎉 模擬測試流程完成！"
    echo ""
    echo "📋 下一步建議："
    if [ -f "/tmp/test_success" ]; then
        echo "✅ 測試成功 - 可以進行真實爬蟲測試"
        echo "   1. 修改原始 LinkedIn DAG 使用真實爬蟲"
        echo "   2. 設定小規模測試 (target_jobs: 5)"
        echo "   3. 執行真實爬蟲測試"
    else
        echo "⚠️  需要解決測試中發現的問題"
        echo "   1. 檢查錯誤日誌"
        echo "   2. 修復問題後重新測試"
        echo "   3. 確保所有 Task 都能成功執行"
    fi
}

# 快速測試選項
quick_test() {
    echo "⚡ 快速測試模式"
    echo "假設環境已經準備就緒，直接執行核心測試"
    
    echo "🔍 檢查 Airflow 狀態..."
    if ! curl -f http://localhost:8080/health &>/dev/null; then
        echo "❌ Airflow 未運行，請先執行完整測試"
        exit 1
    fi
    
    echo "🚀 請手動觸發 linkedin_mock_scraper_test DAG"
    echo "⏳ 等待執行完成..."
    
    read -p "執行完成後按 Enter 驗證結果..."
    
    validate_results
}

# 解析命令行參數
case "${1:-}" in
    "quick")
        quick_test
        ;;
    "validate")
        validate_results
        ;;
    "cleanup")
        cleanup_test_data
        ;;
    "check")
        check_environment
        check_airflow_dag
        ;;
    *)
        main
        ;;
esac