# scripts/prepare_railway_deployment.py
import os
import json
from dotenv import load_dotenv

def prepare_railway_deployment():
    """準備 Railway 部署所需的環境變數和配置"""
    
    load_dotenv()
    
    print("🚀 準備 Railway 部署...")
    
    # 檢查必要的環境變數
    required_vars = {
        'SUPABASE_DB_URL': os.getenv('SUPABASE_DB_URL'),
        'MONGODB_ATLAS_URL': os.getenv('MONGODB_ATLAS_URL'),
        'SUPABASE_DB_HOST': os.getenv('SUPABASE_DB_HOST'),
        'MONGODB_ATLAS_DB_NAME': os.getenv('MONGODB_ATLAS_DB_NAME')
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        print("❌ 缺少必要環境變數:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\n請確認 .env 檔案包含所有雲端連線資訊")
        return False
    
    print("✅ 環境變數檢查通過")
    
    # 生成 Railway 環境變數設定指令
    railway_env_vars = {
        # Airflow 核心配置
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False", 
        "AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO",
        "AIRFLOW__CORE__FERNET_KEY": os.getenv('AIRFLOW__CORE__FERNET_KEY', 'railway-fernet-key-32-chars-long!'),
        "AIRFLOW__WEBSERVER__SECRET_KEY": os.getenv('AIRFLOW__WEBSERVER__SECRET_KEY', 'railway-secret-key'),
        
        # 雲端資料庫連線
        "SUPABASE_DB_URL": os.getenv('SUPABASE_DB_URL'),
        "MONGODB_ATLAS_URL": os.getenv('MONGODB_ATLAS_URL'),
        "MONGODB_ATLAS_DB_NAME": os.getenv('MONGODB_ATLAS_DB_NAME'),
        
        # 爬蟲配置
        "REQUEST_DELAY": "2.0",
        "CONCURRENT_REQUESTS": "8",  # Railway 資源較少，降低並發
        "MAX_RETRIES": "3",
        
        # 部署標記
        "ENVIRONMENT": "production",
        "DEPLOYMENT_PLATFORM": "railway"
    }
    
    print("\n📋 Railway 環境變數設定指令:")
    print("=" * 60)
    print("複製以下指令到 Railway 控制台的 Variables 頁面，或使用 Railway CLI:")
    print()
    
    for key, value in railway_env_vars.items():
        if value:
            # 隱藏敏感資訊顯示
            if any(word in key for word in ['PASSWORD', 'SECRET', 'KEY', 'URL']):
                display_value = f"{value[:15]}***" if len(value) > 15 else "***"
            else:
                display_value = value
            print(f'railway variables set {key}="{display_value}"')
    
    print(f'\n💡 實際的完整指令已保存到 railway_env_commands.txt')
    
    # 將實際的完整指令保存到檔案（包含真實的敏感資訊）
    with open('railway_env_commands.txt', 'w') as f:
        f.write("# Railway 環境變數設定指令\n")
        f.write("# 在 Railway CLI 中執行這些指令，或在 Railway 控制台手動設定\n\n")
        for key, value in railway_env_vars.items():
            if value:
                f.write(f'railway variables set {key}="{value}"\n')
    
    # 建立部署檢查清單
    checklist = """
📋 Railway 部署檢查清單
========================

✅ 準備工作：
□ 雲端遷移已完成 (Supabase + MongoDB Atlas)
□ 本地測試正常
□ Git repository 已推送到 GitHub
□ Railway 帳號已建立

🔧 Railway 設定步驟：
□ 1. 前往 https://railway.app
□ 2. 點擊 "Start a New Project"
□ 3. 選擇 "Deploy from GitHub repo"
□ 4. 選擇你的 repository
□ 5. 在 Variables 頁面設定環境變數（使用 railway_env_commands.txt）
□ 6. 等待自動部署完成

🚀 部署後驗證：
□ 部署成功無錯誤（檢查 Deployments 頁面）
□ Airflow UI 可正常存取
□ 登入成功 (admin/admin123)
□ 健康檢查 DAG 出現在 DAGs 列表
□ 健康檢查 DAG 能成功執行

🌐 預期結果：
□ Airflow UI: https://你的專案名.up.railway.app
□ 登入帳號: admin / admin123
□ 健康檢查 DAG 每 10 分鐘執行一次
□ 可以看到雲端資料庫連線正常的日誌

🔧 故障排除：
如果部署失敗，檢查：
□ Railway Deployments 頁面的錯誤日誌
□ 環境變數是否正確設定
□ Supabase 和 MongoDB Atlas 連線是否正常
□ Dockerfile 語法是否正確
"""
    
    print(checklist)
    
    # 檢查必要檔案是否存在
    required_files = [
        'Dockerfile',
        'railway.json',
        'scripts/railway_start.sh',
        'requirements.txt'
    ]
    
    print("📂 檢查必要檔案:")
    missing_files = []
    for file in required_files:
        if os.path.exists(file):
            print(f"  ✅ {file}")
        else:
            print(f"  ❌ {file} - 檔案不存在")
            missing_files.append(file)
    
    if missing_files:
        print(f"\n⚠️  缺少 {len(missing_files)} 個必要檔案")
        print("💡 請先運行：./scripts/create_railway_files.sh")
        return False
    
    # 測試本地 Docker 構建（可選）
    print("\n🐳 可選：測試 Docker 構建")
    print("如果你想在本地測試 Docker 構建，可以運行：")
    print("  docker build -t test-railway-deployment .")
    print("  docker run -p 8080:8080 test-railway-deployment")
    
    return True

def test_cloud_connections():
    """測試雲端資料庫連線"""
    print("\n🧪 測試雲端資料庫連線...")
    
    try:
        # 測試 Supabase
        import psycopg2
        conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dwh.fact_jobs")
        job_count = cur.fetchone()[0]
        conn.close()
        print(f"  ✅ Supabase PostgreSQL: {job_count} jobs in warehouse")
        
    except Exception as e:
        print(f"  ❌ Supabase 連線失敗: {str(e)}")
        return False
    
    try:
        # 測試 MongoDB Atlas
        from pymongo import MongoClient
        from pymongo.server_api import ServerApi
        client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1'))
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        raw_count = db['raw_jobs_data'].count_documents({})
        client.close()
        print(f"  ✅ MongoDB Atlas: {raw_count} raw jobs")
        
    except Exception as e:
        print(f"  ❌ MongoDB Atlas 連線失敗: {str(e)}")
        return False
    
    print("✅ 所有雲端資料庫連線正常")
    return True

if __name__ == "__main__":
    print("🚀 Railway 部署準備腳本")
    print("=" * 40)
    
    # 準備部署
    success = prepare_railway_deployment()
    
    if success:
        # 測試連線
        connections_ok = test_cloud_connections()
        
        if connections_ok:
            print("\n🎉 Railway 部署準備完成！")
            print("📝 下一步：")
            print("  1. 前往 https://railway.app")
            print("  2. 建立新專案並連接 GitHub")
            print("  3. 設定環境變數（使用 railway_env_commands.txt）")
            print("  4. 等待自動部署完成")
            print("  5. 存取你的 Airflow URL")
        else:
            print("\n❌ 雲端連線測試失敗")
            print("請先確保 Supabase 和 MongoDB Atlas 都正常運作")
    else:
        print("\n❌ 部署準備失敗，請修正上述問題")