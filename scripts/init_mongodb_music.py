#!/usr/bin/env python3
# scripts/setup_mongodb_music_database.py
# 建立 Spotify 音樂追蹤資料庫

from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
from datetime import datetime

def setup_music_database():
    """建立 Spotify 音樂追蹤資料庫"""
    
    load_dotenv()
    
    # 連接 MongoDB Atlas
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    client = MongoClient(mongodb_url, server_api=ServerApi('1'))
    
    # 切換到新資料庫
    db = client['music_data']
    
    print("🎵 建立 Spotify 音樂追蹤資料庫")
    print("=" * 60)
    
    # ========================================================================
    # 階段 1: 建立每日聽歌記錄 Collection
    # ========================================================================
    
    print("\n📊 階段 1: 建立每日聽歌記錄 Collection")
    
    try:
        db.create_collection(
            "daily_listening_history",
            validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["track_id", "played_at", "track_info"],
                    "properties": {
                        "_id": {
                            "bsonType": "objectId"
                        },
                        "track_id": {
                            "bsonType": "string",
                            "description": "Spotify Track ID (必要)"
                        },
                        "played_at": {
                            "bsonType": "date",
                            "description": "播放時間戳 (必要)"
                        },
                        "track_info": {
                            "bsonType": "object",
                            "required": ["name", "artists", "album"],
                            "properties": {
                                "name": { 
                                    "bsonType": "string",
                                    "description": "歌曲名稱"
                                },
                                "artists": {
                                    "bsonType": "array",
                                    "description": "藝術家列表",
                                    "items": {
                                        "bsonType": "object",
                                        "properties": {
                                            "id": { "bsonType": "string" },
                                            "name": { "bsonType": "string" }
                                        }
                                    }
                                },
                                "album": {
                                    "bsonType": "object",
                                    "properties": {
                                        "id": { "bsonType": "string" },
                                        "name": { "bsonType": "string" },
                                        "release_date": { "bsonType": "string" },
                                        "images": { "bsonType": "array" }
                                    }
                                },
                                "duration_ms": { "bsonType": "int" },
                                "explicit": { "bsonType": "bool" },
                                "popularity": { "bsonType": "int" }
                            }
                        },
                        "batch_info": {
                            "bsonType": "object",
                            "properties": {
                                "batch_id": { 
                                    "bsonType": "string",
                                    "description": "批次 ID (格式: daily_YYYYMMDD)"
                                },
                                "collected_at": { 
                                    "bsonType": "date",
                                    "description": "資料收集時間"
                                },
                                "api_version": { 
                                    "bsonType": "string",
                                    "description": "API 版本"
                                }
                            }
                        },
                        "raw_api_response": {
                            "bsonType": "object",
                            "description": "完整的 Spotify API 回應"
                        }
                    }
                }
            }
        )
        print("✅ daily_listening_history Collection 已建立")
    except Exception as e:
        if "already exists" in str(e):
            print("ℹ️  daily_listening_history Collection 已存在")
        else:
            print(f"❌ 建立 daily_listening_history 失敗: {e}")
    
    # 建立索引
    try:
        db.daily_listening_history.create_index(
            [("track_id", 1), ("played_at", -1)], 
            name="idx_track_played",
            background=True
        )
        
        db.daily_listening_history.create_index(
            [("played_at", -1)], 
            name="idx_played_time",
            background=True
        )
        
        db.daily_listening_history.create_index(
            [("batch_info.batch_id", 1)], 
            name="idx_batch",
            background=True
        )
        
        db.daily_listening_history.create_index(
            [("track_id", 1), ("played_at", 1)],
            name="idx_unique_play",
            unique=True
        )
        
        print("   - 索引: track_id + played_at")
        print("   - 索引: played_at (時間查詢)")
        print("   - 索引: batch_id (批次追蹤)")
        print("   - 唯一索引: 避免重複記錄")
        
    except Exception as e:
        print(f"⚠️  建立索引時發生錯誤: {e}")
    
    # ========================================================================
    # 階段 2: 建立參考資料 Collections
    # ========================================================================
    
    print("\n📚 階段 2: 建立參考資料 Collections")
    
    # Track Details Collection
    try:
        db.create_collection("track_details", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["track_id", "name"],
                "properties": {
                    "track_id": {
                        "bsonType": "string",
                        "description": "Spotify Track ID (主鍵)"
                    },
                    "name": { "bsonType": "string" },
                    "artists": { "bsonType": "array" },
                    "album": { "bsonType": "object" },
                    "duration_ms": { "bsonType": "int" },
                    "explicit": { "bsonType": "bool" },
                    "popularity": { "bsonType": "int" },
                    
                    # 音訊特徵 (未來從 Audio Features API 獲取)
                    "audio_features": {
                        "bsonType": "object",
                        "properties": {
                            "danceability": { "bsonType": "double" },
                            "energy": { "bsonType": "double" },
                            "key": { "bsonType": "int" },
                            "loudness": { "bsonType": "double" },
                            "mode": { "bsonType": "int" },
                            "speechiness": { "bsonType": "double" },
                            "acousticness": { "bsonType": "double" },
                            "instrumentalness": { "bsonType": "double" },
                            "liveness": { "bsonType": "double" },
                            "valence": { "bsonType": "double" },
                            "tempo": { "bsonType": "double" }
                        }
                    },
                    
                    # 元資料
                    "metadata": {
                        "bsonType": "object",
                        "properties": {
                            "first_seen": { "bsonType": "date" },
                            "last_updated": { "bsonType": "date" },
                            "play_count": { "bsonType": "int" }
                        }
                    }
                }
            }
        })
        
        db.track_details.create_index(
            [("track_id", 1)], 
            unique=True, 
            name="idx_track_id"
        )
        
        print("✅ track_details Collection 已建立")
        
    except Exception as e:
        if "already exists" in str(e):
            print("ℹ️  track_details Collection 已存在")
        else:
            print(f"❌ 建立 track_details 失敗: {e}")
    
    # Artist Profiles Collection
    try:
        db.create_collection("artist_profiles", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["artist_id", "name"],
                "properties": {
                    "artist_id": { "bsonType": "string" },
                    "name": { "bsonType": "string" },
                    "genres": { "bsonType": "array" },
                    "popularity": { "bsonType": "int" },
                    "followers": { "bsonType": "int" },
                    "images": { "bsonType": "array" },
                    
                    # 元資料
                    "metadata": {
                        "bsonType": "object",
                        "properties": {
                            "first_seen": { "bsonType": "date" },
                            "last_updated": { "bsonType": "date" },
                            "track_count": { "bsonType": "int" }
                        }
                    }
                }
            }
        })
        
        db.artist_profiles.create_index(
            [("artist_id", 1)], 
            unique=True, 
            name="idx_artist_id"
        )
        
        print("✅ artist_profiles Collection 已建立")
        
    except Exception as e:
        if "already exists" in str(e):
            print("ℹ️  artist_profiles Collection 已存在")
        else:
            print(f"❌ 建立 artist_profiles 失敗: {e}")
    
    # Album Catalog Collection
    try:
        db.create_collection("album_catalog", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["album_id", "name"],
                "properties": {
                    "album_id": { "bsonType": "string" },
                    "name": { "bsonType": "string" },
                    "artists": { "bsonType": "array" },
                    "release_date": { "bsonType": "string" },
                    "total_tracks": { "bsonType": "int" },
                    "genres": { "bsonType": "array" },
                    "images": { "bsonType": "array" },
                    
                    # 元資料
                    "metadata": {
                        "bsonType": "object",
                        "properties": {
                            "first_seen": { "bsonType": "date" },
                            "last_updated": { "bsonType": "date" },
                            "play_count": { "bsonType": "int" }
                        }
                    }
                }
            }
        })
        
        db.album_catalog.create_index(
            [("album_id", 1)], 
            unique=True, 
            name="idx_album_id"
        )
        
        print("✅ album_catalog Collection 已建立")
        
    except Exception as e:
        if "already exists" in str(e):
            print("ℹ️  album_catalog Collection 已存在")
        else:
            print(f"❌ 建立 album_catalog 失敗: {e}")
    
    # ========================================================================
    # 階段 3: 系統管理 Collections
    # ========================================================================
    
    print("\n⚙️  階段 3: 建立系統管理 Collections")
    
    try:
        db.create_collection("batch_execution_log", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["batch_id", "execution_date", "status"],
                "properties": {
                    "batch_id": { "bsonType": "string" },
                    "execution_date": { "bsonType": "date" },
                    "status": { 
                        "enum": ["success", "failed", "partial", "running"],
                        "description": "執行狀態"
                    },
                    "tracks_collected": { "bsonType": "int" },
                    "api_calls_made": { "bsonType": "int" },
                    "errors": { "bsonType": "array" },
                    "started_at": { "bsonType": "date" },
                    "completed_at": { "bsonType": "date" },
                    "duration_seconds": { "bsonType": "int" }
                }
            }
        })
        
        db.batch_execution_log.create_index(
            [("execution_date", -1)], 
            name="idx_execution_date"
        )
        
        print("✅ batch_execution_log Collection 已建立")
        
    except Exception as e:
        if "already exists" in str(e):
            print("ℹ️  batch_execution_log Collection 已存在")
        else:
            print(f"❌ 建立 batch_execution_log 失敗: {e}")
    
    # ========================================================================
    # 插入測試資料
    # ========================================================================
    
    print("\n🧪 插入測試資料...")
    
    try:
        # 測試資料: 聽歌記錄
        test_data = {
            "track_id": "test_track_001",
            "played_at": datetime.utcnow(),
            "track_info": {
                "name": "測試歌曲 - Shape of You",
                "artists": [
                    { "id": "test_artist_001", "name": "Ed Sheeran" }
                ],
                "album": {
                    "id": "test_album_001",
                    "name": "÷ (Deluxe)",
                    "release_date": "2017-03-03",
                    "images": []
                },
                "duration_ms": 233713,
                "explicit": False,
                "popularity": 94
            },
            "batch_info": {
                "batch_id": f"test_batch_{datetime.now().strftime('%Y%m%d')}",
                "collected_at": datetime.utcnow(),
                "api_version": "v1"
            },
            "raw_api_response": {
                "note": "這裡會存放完整的 Spotify API 回應"
            }
        }
        
        # 使用 upsert 避免重複插入
        db.daily_listening_history.replace_one(
            {"track_id": "test_track_001", "played_at": test_data["played_at"]},
            test_data,
            upsert=True
        )
        
        print("✅ 測試資料已插入")
        
    except Exception as e:
        print(f"⚠️  插入測試資料時發生錯誤: {e}")
    
    # ========================================================================
    # 資料庫統計
    # ========================================================================
    
    print("\n📊 資料庫統計:")
    print("=" * 60)
    
    try:
        collections = db.list_collection_names()
        print(f"\n📁 Collections 總數: {len(collections)}")
        
        for coll_name in collections:
            collection = db[coll_name]
            count = collection.count_documents({})
            indexes = len(list(collection.list_indexes()))
            print(f"  - {coll_name}: {count} 筆資料, {indexes} 個索引")
        
    except Exception as e:
        print(f"❌ 獲取統計資料失敗: {e}")
    
    # ========================================================================
    # 使用建議
    # ========================================================================
    
    print("\n💡 使用建議:")
    print("=" * 60)
    print("\n階段 1 (本週): 專注於 daily_listening_history")
    print("  - 每天從 Recently Played API 獲取聽歌記錄")
    print("  - 儲存基本的歌曲資訊")
    print("  - 建立每日批次")
    print("")
    print("階段 2 (下週): 豐富參考資料")
    print("  - 定期更新 track_details (獲取完整歌曲資訊)")
    print("  - 定期更新 artist_profiles (藝術家資訊)")
    print("  - 定期更新 album_catalog (專輯資訊)")
    print("")
    print("階段 3 (未來): 分析與洞察")
    print("  - 基於 daily_listening_history 做時間序列分析")
    print("  - 結合 track_details 的 audio_features 分析音樂偏好")
    print("  - 使用 artist_profiles 分析最愛藝術家")
    
    client.close()
    
    print("\n🎉 MongoDB 資料庫初始化完成!")
    print("下一步: 建立 PostgreSQL 分析資料庫")


if __name__ == "__main__":
    setup_music_database()