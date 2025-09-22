#!/usr/bin/env python3
"""
MongoDB Atlas連接修復版測試
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

def test_mongodb_connection():
    """測試不同的MongoDB連接方式"""
    load_env()
    
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')
    
    if not mongodb_url:
        print("❌ MONGODB_ATLAS_URL 未設置")
        return False
    
    from pymongo import MongoClient
    
    # 方法1: 基本連接
    try:
        print("🧪 方法1: 基本連接")
        client = MongoClient(mongodb_url, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        
        # 測試資料庫操作
        db = client[db_name]
        collections = db.list_collection_names()
        print(f"✅ 成功！發現 {len(collections)} 個集合")
        
        client.close()
        return True
        
    except Exception as e1:
        print(f"❌ 方法1失敗: {e1}")
    
    # 方法2: 明確SSL設置
    try:
        print("🧪 方法2: 明確SSL設置")
        client = MongoClient(mongodb_url,
                           serverSelectionTimeoutMS=10000,
                           ssl=True,
                           ssl_cert_reqs=False)  # 寬鬆證書檢查
        client.admin.command('ping')
        
        db = client[db_name] 
        collections = db.list_collection_names()
        print(f"✅ 成功！發現 {len(collections)} 個集合")
        
        client.close()
        return True
        
    except Exception as e2:
        print(f"❌ 方法2失敗: {e2}")
    
    # 方法3: 最寬鬆設置（僅測試用）
    try:
        print("🧪 方法3: 寬鬆設置")
        client = MongoClient(mongodb_url,
                           serverSelectionTimeoutMS=15000,
                           tlsAllowInvalidCertificates=True)
        client.admin.command('ping')
        
        db = client[db_name]
        collections = db.list_collection_names() 
        print(f"✅ 成功！發現 {len(collections)} 個集合")
        print("⚠️  使用了寬鬆SSL設置")
        
        client.close()
        return True
        
    except Exception as e3:
        print(f"❌ 方法3失敗: {e3}")
    
    print("❌ 所有連接方法都失敗")
    return False

if __name__ == "__main__":
    print("🧪 MongoDB Atlas連接修復測試")
    print("============================")
    success = test_mongodb_connection()
    
    if success:
        print("\n🎉 MongoDB連接修復成功！")
    else:
        print("\n💡 建議：")
        print("1. 檢查MongoDB Atlas網絡設置")
        print("2. 確認IP白名單配置")  
        print("3. 驗證連接字符串格式")
        print("4. 可以先專注於Supabase開發，稍後修復MongoDB")
