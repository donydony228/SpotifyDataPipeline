#!/usr/bin/env python3
"""
MongoDB 連線測試腳本 - 除錯版本
"""
import os
import sys
from dotenv import load_dotenv

print("=" * 60)
print("🔍 MongoDB Atlas 連線測試 (除錯模式)")
print("=" * 60)

# 步驟 1: 載入環境變數
print("\n1️⃣  載入環境變數...")
env_loaded = load_dotenv(override=True)  # 強制覆蓋
print(f"   load_dotenv() 結果: {env_loaded}")

# 步驟 2: 檢查環境變數
print("\n2️⃣  檢查環境變數...")
mongodb_url = os.getenv('MONGODB_ATLAS_URL')
mongodb_db = os.getenv('MONGODB_ATLAS_DB_NAME', 'music_data')

if not mongodb_url:
    print("   ❌ MONGODB_ATLAS_URL 未設定!")
    print("\n請檢查 .env 檔案:")
    print("  1. 確認檔案存在: ls -la .env")
    print("  2. 確認內容正確: cat .env | grep MONGODB")
    sys.exit(1)

print(f"   MONGODB_ATLAS_URL: {mongodb_url[:60]}...")
print(f"   MONGODB_ATLAS_DB_NAME: {mongodb_db}")

# 檢查 URL 格式
if not mongodb_url.startswith('mongodb+srv://'):
    print(f"   ❌ URL 格式錯誤!")
    print(f"   當前: {mongodb_url[:30]}...")
    print(f"   應該: mongodb+srv://...")
    sys.exit(1)

print("   ✅ URL 格式正確")

# 步驟 3: 測試連線
print("\n3️⃣  測試 MongoDB Atlas 連線...")

try:
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    import certifi
    
    print("   📦 套件載入成功")
    print(f"   🔒 使用憑證: {certifi.where()}")
    
    # 建立連線
    print("\n   🔗 建立連線...")
    client = MongoClient(
        mongodb_url,
        server_api=ServerApi('1'),
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000
    )
    
    # 測試連線
    print("   📡 測試 ping...")
    result = client.admin.command('ping')
    
    print("\n   ✅ 連線成功!")
    print(f"   📊 Ping 結果: {result}")
    
    # 測試資料庫
    db = client[mongodb_db]
    collections = db.list_collection_names()
    
    print(f"\n   📁 資料庫: {mongodb_db}")
    print(f"   📂 Collections: {len(collections)}")
    
    if collections:
        for coll in collections:
            count = db[coll].count_documents({})
            print(f"      - {coll}: {count} 筆")
    
    client.close()
    
    print("\n🎉 所有測試通過!")
    sys.exit(0)
    
except Exception as e:
    print(f"\n   ❌ 連線失敗!")
    print(f"\n錯誤訊息:")
    print(f"   {str(e)[:200]}")
    
    import traceback
    print("\n詳細錯誤:")
    traceback.print_exc()
    
    sys.exit(1)
