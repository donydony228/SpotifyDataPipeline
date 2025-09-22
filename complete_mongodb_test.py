#!/usr/bin/env python3
"""
完整的MongoDB Atlas功能測試
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

def test_mongodb_comprehensive():
    """全面測試MongoDB功能"""
    load_env()
    
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')
    
    if not mongodb_url:
        print("❌ MONGODB_ATLAS_URL 未設置")
        return False
    
    try:
        from pymongo import MongoClient
        from pymongo.server_api import ServerApi
        import datetime
        
        print(f"📊 使用數據庫: {db_name}")
        print(f"🔗 連接URL: {mongodb_url[:30]}...")
        
        # 建立連接
        client = MongoClient(mongodb_url, serverSelectionTimeoutMS=10000)
        
        # 測試連接
        print("🧪 測試基本連接...")
        client.admin.command('ping')
        print("✅ 基本連接成功")
        
        # 獲取數據庫
        db = client[db_name]
        
        # 列出集合
        collections = db.list_collection_names()
        print(f"📦 發現集合數量: {len(collections)}")
        for collection in collections:
            count = db[collection].count_documents({})
            print(f"   - {collection}: {count} 個文檔")
        
        # 測試寫入操作
        print("\n🧪 測試寫入操作...")
        test_collection = db['test_connection']
        
        test_doc = {
            'test_id': 'connection_test',
            'timestamp': datetime.datetime.utcnow(),
            'message': 'MongoDB連接測試成功',
            'version': 'fixed'
        }
        
        result = test_collection.insert_one(test_doc)
        print(f"✅ 寫入成功，文檔ID: {result.inserted_id}")
        
        # 測試讀取操作
        print("🧪 測試讀取操作...")
        retrieved_doc = test_collection.find_one({'test_id': 'connection_test'})
        if retrieved_doc:
            print(f"✅ 讀取成功: {retrieved_doc['message']}")
        
        # 清理測試數據
        test_collection.delete_one({'test_id': 'connection_test'})
        print("🧹 清理測試數據完成")
        
        # 測試聚合操作
        if 'raw_jobs_data' in collections:
            print("\n🧪 測試聚合操作...")
            pipeline = [
                {'$limit': 1},
                {'$project': {'_id': 1}}
            ]
            result = list(db['raw_jobs_data'].aggregate(pipeline))
            print(f"✅ 聚合測試成功，樣本數據: {len(result)}")
        
        client.close()
        print("\n🎉 MongoDB全部功能測試通過！")
        return True
        
    except Exception as e:
        print(f"❌ MongoDB測試失敗: {e}")
        print(f"錯誤類型: {type(e).__name__}")
        return False

if __name__ == "__main__":
    print("🧪 MongoDB Atlas完整功能測試")
    print("============================")
    success = test_mongodb_comprehensive()
    
    if success:
        print("\n✅ MongoDB已完全修復並正常工作！")
    else:
        print("\n❌ MongoDB仍有問題需要進一步調查")
