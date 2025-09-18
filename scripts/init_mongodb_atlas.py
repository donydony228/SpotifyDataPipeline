from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def init_mongodb_atlas():
    try:
        client = MongoClient(
            os.getenv('MONGODB_ATLAS_URL'),
            server_api=ServerApi('1')
        )
        
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        
        print("🚀 初始化 MongoDB Atlas 資料庫...")
        
        # 1. 建立 raw_jobs_data 集合並設定驗證規則
        print("📝 建立 raw_jobs_data 集合...")
        
        # 檢查集合是否已存在
        if 'raw_jobs_data' not in db.list_collection_names():
            db.create_collection('raw_jobs_data', validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["source", "job_data", "metadata"],
                    "properties": {
                        "source": {
                            "bsonType": "string",
                            "enum": ["linkedin", "indeed", "glassdoor", "angellist"]
                        },
                        "job_data": {
                            "bsonType": "object"
                        },
                        "metadata": {
                            "bsonType": "object",
                            "required": ["scraped_at", "batch_id"],
                            "properties": {
                                "scraped_at": {"bsonType": "date"},
                                "batch_id": {"bsonType": "string"},
                                "scraper_version": {"bsonType": "string"},
                                "source_url": {"bsonType": "string"}
                            }
                        }
                    }
                }
            })
            print("  ✅ raw_jobs_data 集合已建立")
        else:
            print("  ⚠️  raw_jobs_data 集合已存在")
        
        # 2. 建立其他集合
        collections_to_create = [
            'data_quality_reports',
            'scraper_logs',
            'batch_metadata'
        ]
        
        for collection_name in collections_to_create:
            if collection_name not in db.list_collection_names():
                db.create_collection(collection_name)
                print(f"  ✅ {collection_name} 集合已建立")
            else:
                print(f"  ⚠️  {collection_name} 集合已存在")
        
        # 3. 建立索引提升查詢效能
        print("\n🔍 建立索引...")
        
        raw_jobs_collection = db['raw_jobs_data']
        
        # 建立複合索引
        indexes_to_create = [
            ([("source", 1), ("metadata.scraped_at", -1)], "source_scraped_idx"),
            ([("metadata.batch_id", 1)], "batch_id_idx"),
            ([("job_data.job_id", 1), ("source", 1)], "job_id_source_idx")
        ]
        
        existing_indexes = [idx['name'] for idx in raw_jobs_collection.list_indexes()]
        
        for index_fields, index_name in indexes_to_create:
            if index_name not in existing_indexes:
                raw_jobs_collection.create_index(index_fields, name=index_name, unique=(index_name == "job_id_source_idx"))
                print(f"  ✅ 索引 {index_name} 已建立")
            else:
                print(f"  ⚠️  索引 {index_name} 已存在")
        
        # 4. 插入測試資料
        print("\n📊 插入測試資料...")
        
        test_data = {
            "source": "linkedin",
            "job_data": {
                "job_id": "atlas_test_job_001",
                "job_title": "Senior Data Engineer - Atlas Test",
                "company": "Tech Corp",
                "location": "San Francisco, CA",
                "salary": "$120,000 - $180,000",
                "description": "Looking for a Senior Data Engineer to join our growing team...",
                "skills": ["Python", "SQL", "AWS", "Docker", "Apache Airflow"],
                "employment_type": "Full-time",
                "work_arrangement": "Hybrid"
            },
            "metadata": {
                "scraped_at": datetime.utcnow(),
                "batch_id": f"atlas_test_batch_{datetime.now().strftime('%Y%m%d')}",
                "scraper_version": "1.0.0",
                "source_url": "https://linkedin.com/jobs/atlas_test_job_001"
            },
            "data_quality": {
                "completeness_score": 0.98,
                "flags": []
            }
        }
        
        # 插入測試資料（避免重複）
        existing_test = raw_jobs_collection.find_one({
            "job_data.job_id": "atlas_test_job_001",
            "source": "linkedin"
        })
        
        if not existing_test:
            result = raw_jobs_collection.insert_one(test_data)
            print(f"  ✅ 測試資料已插入，ID: {result.inserted_id}")
        else:
            print("  ⚠️  測試資料已存在")
        
        # 5. 驗證設定
        print("\n🔍 驗證 Atlas 設定...")
        
        # 檢查集合數量
        collections = db.list_collection_names()
        print(f"  📂 集合總數: {len(collections)}")
        for collection in collections:
            count = db[collection].count_documents({})
            print(f"    - {collection}: {count} 筆文檔")
        
        # 測試查詢
        test_query = raw_jobs_collection.find_one({"source": "linkedin"})
        if test_query:
            print("  ✅ 查詢測試通過")
            print(f"    範例資料: {test_query['job_data']['job_title']}")
        
        client.close()
        print("\n🎉 MongoDB Atlas 初始化完成！")
        return True
        
    except Exception as e:
        print(f"❌ MongoDB Atlas 初始化失敗: {str(e)}")
        return False

if __name__ == "__main__":
    init_mongodb_atlas()