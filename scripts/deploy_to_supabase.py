import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def deploy_schema_to_supabase():
    try:
        # 讀取本地的 SQL 檔案
        with open('sql/ddl/warehouse_tables.sql', 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # 連接到 Supabase
        conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
        cur = conn.cursor()
        
        print("🚀 開始部署 Schema 到 Supabase...")
        
        # 執行 Schema 建立
        cur.execute(schema_sql)
        conn.commit()
        
        print("✅ Schema 部署成功！")
        
        # 驗證表格建立
        cur.execute("""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname IN ('raw_staging', 'clean_staging', 'business_staging', 'dwh')
            ORDER BY schemaname, tablename;
        """)
        
        tables = cur.fetchall()
        print(f"\n📊 成功建立 {len(tables)} 個表格：")
        for schema, table in tables:
            print(f"  - {schema}.{table}")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Schema 部署失敗: {str(e)}")
        return False

if __name__ == "__main__":
    deploy_schema_to_supabase()