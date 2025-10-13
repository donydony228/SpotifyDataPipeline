import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("🔗 測試 Supabase PostgreSQL 連線...")
    
    conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
    cur = conn.cursor()
    
    # 測試查詢
    cur.execute("""
        SELECT COUNT(*) 
        FROM raw_staging.spotify_listening_history
    """)
    count = cur.fetchone()[0]
    
    print(f"✅ 連線成功!")
    print(f"📊 spotify_listening_history: {count} 筆資料")
    
    # 檢查表格數量
    cur.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables
        WHERE table_schema IN ('raw_staging', 'dwh')
    """)
    table_count = cur.fetchone()[0]
    print(f"📁 表格總數: {table_count}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ 連線失敗: {e}")
