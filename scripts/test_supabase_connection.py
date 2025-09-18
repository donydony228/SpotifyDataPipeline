import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_supabase_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('SUPABASE_DB_HOST'),
            port=os.getenv('SUPABASE_DB_PORT'),
            database=os.getenv('SUPABASE_DB_NAME'),
            user=os.getenv('SUPABASE_DB_USER'),
            password=os.getenv('SUPABASE_DB_PASSWORD')
        )
        
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"✅ Supabase PostgreSQL 連線成功！")
        print(f"📊 PostgreSQL 版本: {version[0]}")
        
        # 測試建立 schema 權限
        cur.execute("CREATE SCHEMA IF NOT EXISTS test_schema;")
        cur.execute("DROP SCHEMA test_schema;")
        print("✅ Schema 建立權限正常")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Supabase 連線失敗: {str(e)}")
        return False

if __name__ == "__main__":
    test_supabase_connection()