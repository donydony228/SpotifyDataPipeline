from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("Testing MongoDB Atlas connection...")
    
    client = MongoClient(
        os.getenv('MONGODB_ATLAS_URL'),
        server_api=ServerApi('1')
    )
    
    db = client['music_data']

    # Test query
    count = db.daily_listening_history.count_documents({})
    print(f"Connection successful!")
    print(f"daily_listening_history: {count} documents found")

    # List all collections
    collections = db.list_collection_names()
    print(f"Collections: {collections}")
    
    client.close()
    
except Exception as e:
    print(f"Connection failed: {e}")
