from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

def test_mongodb_atlas():
    try:
        # 連接 MongoDB Atlas
        client = MongoClient(
            os.getenv('MONGODB_ATLAS_URL'),
            server_api=ServerApi('1')
        )
        
        # 測試連線
        client.admin.command('ping')
        print("✅ MongoDB Atlas 連線成功！")
        
        # 取得資料庫
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        
        # 檢查現有集合
        collections = db.list_collection_names()
        print(f"📂 現有集合: {collections}")
        
        # 測試建立集合權限
        test_collection = db['test_collection']
        test_collection.insert_one({"test": "data"})
        test_collection.drop()
        print("✅ 讀寫權限正常")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ MongoDB Atlas 連線失敗: {str(e)}")
        print("🔧 請檢查：")
        print("  1. 連線字串是否正確")
        print("  2. 用戶密碼是否正確")
        print("  3. IP 白名單設定")
        print("  4. 網路連線")
        return False

if __name__ == "__main__":
    test_mongodb_atlas()