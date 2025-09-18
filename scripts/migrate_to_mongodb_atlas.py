from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

def migrate_to_mongodb_atlas():
    local_client = None
    atlas_client = None
    
    try:
        # 連接本地 MongoDB
        print("🔗 連接本地 MongoDB...")
        local_client = MongoClient("mongodb://admin:admin123@localhost:27017")
        local_db = local_client['job_market_data']
        
        # 連接 MongoDB Atlas
        print("🔗 連接 MongoDB Atlas...")
        atlas_client = MongoClient(
            os.getenv('MONGODB_ATLAS_URL'),
            server_api=ServerApi('1')
        )
        atlas_db = atlas_client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        
        print("\n🔄 開始遷移資料...")
        
        # 取得本地集合清單
        local_collections = local_db.list_collection_names()
        print(f"📂 本地集合: {local_collections}")
        
        migrated_documents = 0
        
        for collection_name in local_collections:
            print(f"\n📦 遷移集合: {collection_name}")
            
            # 取得本地資料
            local_collection = local_db[collection_name]
            documents = list(local_collection.find())
            
            if not documents:
                print(f"  ⚠️  {collection_name} 沒有資料")
                continue
            
            # 插入到 Atlas
            atlas_collection = atlas_db[collection_name]
            
            try:
                # 使用 upsert 避免重複插入
                for doc in documents:
                    if 'job_data' in doc and 'job_id' in doc.get('job_data', {}):
                        # 使用 job_id 和 source 作為唯一識別
                        filter_condition = {
                            'job_data.job_id': doc['job_data']['job_id'],
                            'source': doc.get('source')
                        }
                        atlas_collection.replace_one(filter_condition, doc, upsert=True)
                    else:
                        # 其他文檔直接插入
                        atlas_collection.insert_one(doc)
                
                print(f"  ✅ {collection_name}: {len(documents)} 筆文檔")
                migrated_documents += len(documents)
                
            except Exception as e:
                print(f"  ❌ {collection_name} 遷移失敗: {str(e)}")
        
        print(f"\n🎉 遷移完成！總計 {migrated_documents} 筆文檔")
        
        # 驗證遷移結果
        print("\n📊 驗證遷移結果:")
        atlas_collections = atlas_db.list_collection_names()
        for collection_name in atlas_collections:
            count = atlas_db[collection_name].count_documents({})
            print(f"  ✅ {collection_name}: {count} 筆")
        
        return True
        
    except Exception as e:
        print(f"❌ 遷移失敗: {str(e)}")
        return False
        
    finally:
        if local_client:
            local_client.close()
        if atlas_client:
            atlas_client.close()

if __name__ == "__main__":
    migrate_to_mongodb_atlas()