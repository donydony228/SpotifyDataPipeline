#!/usr/bin/env python3
"""
測試Airflow DAG執行環境中的依賴套件
"""

def test_imports():
    """測試所有必要的導入"""
    try:
        import psycopg2
        print("✅ psycopg2 導入成功")
    except ImportError as e:
        print(f"❌ psycopg2 導入失敗: {e}")
        
    try:
        import pymongo
        print("✅ pymongo 導入成功")
    except ImportError as e:
        print(f"❌ pymongo 導入失敗: {e}")
        
    try:
        import requests
        print("✅ requests 導入成功")
    except ImportError as e:
        print(f"❌ requests 導入失敗: {e}")
        
    try:
        import pandas
        print("✅ pandas 導入成功")
    except ImportError as e:
        print(f"❌ pandas 導入失敗: {e}")

def test_database_connections():
    """測試資料庫連接"""
    import os
    from pathlib import Path
    
    # 載入環境變數
    env_file = Path.home() / 'airflow' / '.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith('#') and '=' in line:
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    # 測試Supabase
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if supabase_url:
        try:
            import psycopg2
            conn = psycopg2.connect(supabase_url, connect_timeout=10)
            conn.close()
            print("✅ Supabase連接成功")
        except Exception as e:
            print(f"❌ Supabase連接失敗: {e}")
    else:
        print("⚠️  SUPABASE_DB_URL 未設置")
    
    # 測試MongoDB
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    if mongodb_url:
        try:
            from pymongo import MongoClient
            from pymongo.server_api import ServerApi
            client = MongoClient(mongodb_url, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            print("✅ MongoDB Atlas連接成功")
        except Exception as e:
            print(f"❌ MongoDB Atlas連接失敗: {e}")
    else:
        print("⚠️  MONGODB_ATLAS_URL 未設置")

if __name__ == "__main__":
    print("🧪 測試DAG執行環境")
    print("=================")
    
    print("\n📦 測試套件導入:")
    test_imports()
    
    print("\n🔗 測試資料庫連接:")
    test_database_connections()
    
    print("\n🎉 環境測試完成!")
