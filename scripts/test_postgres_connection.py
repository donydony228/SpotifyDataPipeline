import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("Testing connection to Postgres database...")
    
    conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
    cur = conn.cursor()
    
    # Test querying the spotify_listening_history table
    cur.execute("""
        SELECT COUNT(*) 
        FROM raw_staging.spotify_listening_history
    """)
    count = cur.fetchone()[0]

    print(f"Connection successful!")
    print(f"spotify_listening_history: {count} documents found")

    # Check table count in raw_staging and dwh schemas
    cur.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables
        WHERE table_schema IN ('raw_staging', 'dwh')
    """)
    table_count = cur.fetchone()[0]
    print(f"Total tables: {table_count}")

    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Connection failed: {e}")
