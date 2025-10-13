from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("🔗 測試 MongoDB Atlas 連線...")
    
    client = MongoClient(
        os.getenv('MONGODB_ATLAS_URL'),
        server_api=ServerApi('1')
    )
    
    db = client['music_data']
    
    # 測試查詢
    count = db.daily_listening_history.count_documents({})
    print(f"✅ 連線成功!")
    print(f"📊 daily_listening_history: {count} 筆資料")
    
    # 列出所有 collections
    collections = db.list_collection_names()
    print(f"📁 Collections: {collections}")
    
    client.close()
    
except Exception as e:
    print(f"❌ 連線失敗: {e}")
