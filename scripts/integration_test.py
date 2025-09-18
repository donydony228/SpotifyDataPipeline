import psycopg2
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

def integration_test():
    print("🔬 整合測試開始...")
    
    # 測試 Supabase PostgreSQL
    try:
        conn = psycopg2.connect(
            host=os.getenv('SUPABASE_DB_HOST'),
            port=os.getenv('SUPABASE_DB_PORT', 5432),
            database=os.getenv('SUPABASE_DB_NAME'),
            user=os.getenv('SUPABASE_DB_USER'),
            password=os.getenv('SUPABASE_DB_PASSWORD')
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dwh.fact_jobs")
        pg_jobs = cur.fetchone()[0]
        conn.close()
        print(f"  ✅ Supabase PostgreSQL: {pg_jobs} 筆職缺資料")
    except Exception as e:
        print(f"  ❌ Supabase 測試失敗: {str(e)}")
        return False
    
    # 測試 MongoDB Atlas
    try:
        client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1'))
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        mongo_jobs = db['raw_jobs_data'].count_documents({})
        client.close()
        print(f"  ✅ MongoDB Atlas: {mongo_jobs} 筆原始資料")
    except Exception as e:
        print(f"  ❌ MongoDB Atlas 測試失敗: {str(e)}")
        return False
    
    print("🎉 整合測試通過！雲端環境已就緒")
    return True

if __name__ == "__main__":
    integration_test()
