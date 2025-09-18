import psycopg2
import json
from datetime import datetime, date
import os
from dotenv import load_dotenv

load_dotenv()

def migrate_test_data():
    local_conn = None
    supabase_conn = None
    
    try:
        # 連接本地資料庫
        local_conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="job_data_warehouse", 
            user="dwh_user",
            password="dwh_password"
        )
        
        # 連接 Supabase
        supabase_conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
        
        local_cur = local_conn.cursor()
        supabase_cur = supabase_conn.cursor()
        
        print("🔄 開始遷移測試資料...")
        
        # 遷移順序：維度表 → 事實表 → 橋接表
        tables_to_migrate = [
            ('dwh.dim_companies', 'company_key'),
            ('dwh.dim_locations', 'location_key'), 
            ('dwh.dim_job_roles', 'role_key'),
            ('dwh.dim_skills', 'skill_key'),
            ('dwh.dim_dates', 'date_key'),
            ('dwh.fact_jobs', 'job_key'),
            ('dwh.bridge_job_skills', None)  # 複合主鍵
        ]
        
        for table_name, primary_key in tables_to_migrate:
            print(f"📦 遷移 {table_name}...")
            
            # 從本地讀取資料
            local_cur.execute(f"SELECT * FROM {table_name}")
            rows = local_cur.fetchall()
            
            if not rows:
                print(f"  ⚠️  {table_name} 沒有資料")
                continue
            
            # 取得欄位名稱
            local_cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{table_name.split('.')[0]}' 
                AND table_name = '{table_name.split('.')[1]}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in local_cur.fetchall()]
            
            # 插入到 Supabase
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            insert_sql = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """
            
            supabase_cur.executemany(insert_sql, rows)
            print(f"  ✅ {table_name}: {len(rows)} 筆資料")
        
        supabase_conn.commit()
        print("\n🎉 測試資料遷移完成！")
        
        # 驗證資料
        print("\n📊 驗證遷移結果：")
        for table_name, _ in tables_to_migrate:
            supabase_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = supabase_cur.fetchone()[0]
            print(f"  - {table_name}: {count} 筆")
        
        return True
        
    except Exception as e:
        print(f"❌ 資料遷移失敗: {str(e)}")
        return False
        
    finally:
        if local_conn:
            local_conn.close()
        if supabase_conn:
            supabase_conn.close()

if __name__ == "__main__":
    migrate_test_data()