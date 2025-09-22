#!/usr/bin/env python3
"""
最終的數據庫連接驗證
"""

import os
import urllib3
from pathlib import Path

# 禁用urllib3警告
urllib3.disable_warnings()

def load_env():
    """載入環境變數"""
    env_file = Path.home() / 'airflow' / '.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith('#') and '=' in line:
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

def test_all_databases():
    """測試所有數據庫連接"""
    load_env()
    
    print("🧪 最終數據庫連接測試")
    print("===================")
    
    results = {}
    
    # 測試Supabase
    print("\n📊 測試Supabase PostgreSQL...")
    try:
        import psycopg2
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if supabase_url:
            conn = psycopg2.connect(supabase_url, connect_timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            conn.close()
            print(f"✅ Supabase連接成功")
            print(f"   版本: {version.split()[1]}")
            results['supabase'] = True
        else:
            print("❌ SUPABASE_DB_URL 未設置")
            results['supabase'] = False
    except Exception as e:
        print(f"❌ Supabase連接失敗: {e}")
        results['supabase'] = False
    
    # 測試MongoDB
    print("\n🍃 測試MongoDB Atlas...")
    try:
        from pymongo import MongoClient
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')
        
        if mongodb_url:
            client = MongoClient(mongodb_url, serverSelectionTimeoutMS=10000)
            client.admin.command('ping')
            
            db = client[db_name]
            collections = db.list_collection_names()
            
            print(f"✅ MongoDB Atlas連接成功")
            print(f"   數據庫: {db_name}")
            print(f"   集合數: {len(collections)}")
            
            client.close()
            results['mongodb'] = True
        else:
            print("❌ MONGODB_ATLAS_URL 未設置")
            results['mongodb'] = False
    except Exception as e:
        print(f"❌ MongoDB Atlas連接失敗: {e}")
        results['mongodb'] = False
    
    # 總結
    print("\n📊 最終結果")
    print("===========")
    success_count = sum(results.values())
    total_count = len(results)
    
    for db, status in results.items():
        status_icon = "✅" if status else "❌"
        print(f"{status_icon} {db.title()}: {'正常' if status else '需要修復'}")
    
    print(f"\n🎯 總體狀況: {success_count}/{total_count} 個數據庫正常")
    
    if success_count == total_count:
        print("🎉 所有數據庫連接完美！可以開始開發真實爬蟲了！")
    elif success_count > 0:
        print("👍 部分數據庫正常，可以開始開發，稍後修復其餘問題")
    else:
        print("⚠️  需要進一步調查數據庫連接問題")
    
    return results

if __name__ == "__main__":
    test_all_databases()
