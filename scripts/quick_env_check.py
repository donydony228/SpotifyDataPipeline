# scripts/quick_env_check.py
# 快速檢查環境變數問題

import os
import sys
from pathlib import Path

def load_env_file():
    """手動載入 .env 檔案"""
    env_vars = {}
    
    # 可能的 .env 位置
    possible_paths = [
        '.env',
        '../.env',
        '/opt/airflow/.env',
        '/app/.env'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"🔍 找到 .env 檔案: {path}")
            try:
                with open(path, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            env_vars[key.strip()] = value.strip().strip('"').strip("'")
                
                print(f"✅ 載入了 {len(env_vars)} 個環境變數")
                return env_vars, path
                
            except Exception as e:
                print(f"❌ 讀取失敗: {e}")
                continue
    
    print("❌ 未找到任何 .env 檔案")
    return {}, None

def check_current_env():
    """檢查當前環境變數"""
    critical_vars = ['SUPABASE_DB_URL', 'MONGODB_ATLAS_URL', 'MONGODB_ATLAS_DB_NAME']
    
    print("🔍 檢查當前環境變數:")
    found_vars = {}
    
    for var in critical_vars:
        value = os.getenv(var)
        if value:
            masked = f"{value[:20]}***" if len(value) > 20 else "***"
            print(f"  ✅ {var}: {masked}")
            found_vars[var] = value
        else:
            print(f"  ❌ {var}: 未設定")
    
    return found_vars

def test_database_connections(env_vars):
    """測試資料庫連線"""
    print("\n🔗 測試資料庫連線:")
    
    # 測試 Supabase
    supabase_url = env_vars.get('SUPABASE_DB_URL')
    if supabase_url:
        try:
            import psycopg2
            conn = psycopg2.connect(supabase_url, connect_timeout=10)
            conn.close()
            print("  ✅ Supabase PostgreSQL: 連線成功")
        except ImportError:
            print("  ⚠️  Supabase: psycopg2 未安裝")
        except Exception as e:
            print(f"  ❌ Supabase: 連線失敗 - {str(e)}")
    else:
        print("  ❌ Supabase: URL 未設定")
    
    # 測試 MongoDB Atlas
    mongodb_url = env_vars.get('MONGODB_ATLAS_URL')
    if mongodb_url:
        try:
            from pymongo import MongoClient
            from pymongo.server_api import ServerApi
            client = MongoClient(mongodb_url, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            print("  ✅ MongoDB Atlas: 連線成功")
        except ImportError:
            print("  ⚠️  MongoDB: pymongo 未安裝")
        except Exception as e:
            print(f"  ❌ MongoDB Atlas: 連線失敗 - {str(e)}")
    else:
        print("  ❌ MongoDB Atlas: URL 未設定")

def check_airflow_environment():
    """檢查 Airflow 特定環境"""
    print("\n🌊 Airflow 環境檢查:")
    
    # 檢查 AIRFLOW_HOME
    airflow_home = os.getenv('AIRFLOW_HOME')
    if airflow_home:
        print(f"  ✅ AIRFLOW_HOME: {airflow_home}")
    else:
        print("  ⚠️  AIRFLOW_HOME: 未設定")
    
    # 檢查當前工作目錄
    print(f"  📁 當前目錄: {os.getcwd()}")
    
    # 檢查 Python 路徑
    print(f"  🐍 Python 路徑: {sys.executable}")
    
    # 檢查是否在容器中
    if os.path.exists('/.dockerenv'):
        print("  🐳 運行環境: Docker 容器")
    else:
        print("  💻 運行環境: 本地機器")

def generate_fix_suggestions(env_vars, env_file_path):
    """生成修復建議"""
    print("\n🔧 修復建議:")
    
    if not env_vars:
        print("  1. 確保 .env 檔案存在於專案根目錄")
        print("  2. 檢查 .env 檔案格式 (KEY=value，無空格)")
        print("  3. 執行: cp .env.example .env")
        return
    
    missing_vars = []
    critical_vars = ['SUPABASE_DB_URL', 'MONGODB_ATLAS_URL', 'MONGODB_ATLAS_DB_NAME']
    
    for var in critical_vars:
        if var not in env_vars or not env_vars[var]:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"  1. 在 .env 檔案中設定缺少的變數:")
        for var in missing_vars:
            print(f"     {var}=your_actual_value_here")
    
    if env_file_path and not env_file_path.startswith('/opt/airflow'):
        print("  2. 複製 .env 到 Airflow 容器:")
        print("     docker compose cp .env airflow-webserver:/opt/airflow/.env")
    
    print("  3. 重啟 Airflow 服務:")
    print("     docker compose restart airflow-webserver airflow-scheduler")
    
    print("  4. 使用修復版 DAG 測試:")
    print("     觸發 'linkedin_mock_scraper_env_fixed' DAG")

def main():
    print("🚀 快速環境變數檢查")
    print("=" * 40)
    
    # 1. 檢查當前環境變數
    current_env_vars = check_current_env()
    
    # 2. 載入 .env 檔案
    print("\n📁 檢查 .env 檔案:")
    env_file_vars, env_file_path = load_env_file()
    
    # 3. 合併環境變數 (優先使用檔案中的)
    all_env_vars = {**current_env_vars, **env_file_vars}
    
    # 4. 測試資料庫連線
    if all_env_vars:
        test_database_connections(all_env_vars)
    
    # 5. Airflow 環境檢查
    check_airflow_environment()
    
    # 6. 生成修復建議
    generate_fix_suggestions(all_env_vars, env_file_path)
    
    print("\n" + "=" * 40)
    print("✅ 環境檢查完成")
    
    if all_env_vars and len(all_env_vars) >= 3:
        print("🎉 環境變數載入正常，可以繼續測試")
        return True
    else:
        print("⚠️  發現環境變數問題，請按照建議修復")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)