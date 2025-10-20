# dags/spotify/curl_spotify_tracker.py
# 使用 curl 替代 requests 的版本 - 解決 Python requests 卡住問題
# 新增功能：批次收集 track、artist、album 詳細資訊

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import base64
import os
import time
import json
import subprocess
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# ============================================================================
# 環境變數載入
# ============================================================================

def force_load_env_vars():
    """強制載入環境變數"""
    env_files = ['/Users/desmond/airflow/.env', '.env', 'airflow_home/.env', '../.env']
    
    for env_file in env_files:
        if os.path.exists(env_file):
            print(f"📁 載入環境變數: {env_file}")
            
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('"').strip("'")
            
            print("✅ 環境變數載入完成")
            return True
    
    raise FileNotFoundError("❌ 找不到 .env 檔案")

def get_spotify_credentials():
    """獲取 Spotify 憑證"""
    force_load_env_vars()
    
    client_id = os.environ.get('SPOTIFY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')
    refresh_token = os.environ.get('SPOTIFY_REFRESH_TOKEN')
    
    if not all([client_id, client_secret, refresh_token]):
        missing = []
        if not client_id: missing.append('SPOTIFY_CLIENT_ID')
        if not client_secret: missing.append('SPOTIFY_CLIENT_SECRET')
        if not refresh_token: missing.append('SPOTIFY_REFRESH_TOKEN')
        raise ValueError(f"❌ 缺少憑證: {missing}")
    
    return client_id, client_secret, refresh_token

# ============================================================================
# MongoDB 連線
# ============================================================================

def get_mongodb_connection():
    """取得 MongoDB 連線"""
    mongodb_url = os.environ.get('MONGODB_ATLAS_URL')
    db_name = os.environ.get('MONGODB_ATLAS_DB_NAME', 'music_data')
    
    if not mongodb_url:
        raise ValueError("❌ 缺少 MONGODB_ATLAS_URL")
    
    client = MongoClient(mongodb_url, server_api=ServerApi('1'))
    db = client[db_name]
    return db

def exists_in_mongodb(collection_name: str, spotify_id: str) -> bool:
    """檢查 ID 是否已存在於 MongoDB"""
    try:
        db = get_mongodb_connection()
        collection = db[collection_name]
        
        # 根據不同 collection 使用不同的 ID 欄位
        if collection_name == 'track_details':
            id_field = 'track_id'
        elif collection_name == 'artist_profiles':
            id_field = 'artist_id'
        elif collection_name == 'album_catalog':
            id_field = 'album_id'
        else:
            id_field = 'id'
        
        result = collection.find_one({id_field: spotify_id})
        return result is not None
    except Exception as e:
        print(f"❌ 檢查 MongoDB 資料失敗: {e}")
        return False

def store_to_mongodb(collection_name: str, data: list) -> dict:
    """批次儲存資料到 MongoDB"""
    if not data:
        return {"status": "no_data", "count": 0}
    
    try:
        db = get_mongodb_connection()
        collection = db[collection_name]
        
        # 批次插入，忽略重複
        result = collection.insert_many(data, ordered=False)
        
        print(f"✅ 成功儲存 {len(result.inserted_ids)} 筆資料到 {collection_name}")
        
        return {
            "status": "success",
            "collection": collection_name,
            "inserted_count": len(result.inserted_ids),
            "total_attempted": len(data)
        }
        
    except Exception as e:
        print(f"❌ 儲存到 MongoDB 失敗: {e}")
        return {
            "status": "failed",
            "collection": collection_name,
            "error": str(e),
            "total_attempted": len(data)
        }

# ============================================================================
# 基於 curl 的 Spotify 客戶端
# ============================================================================

class CurlSpotifyClient:
    """使用 curl 的 Spotify API 客戶端"""
    
    def __init__(self):
        self.client_id, self.client_secret, self.refresh_token = get_spotify_credentials()
        self.access_token = None
        
        print(f"🔑 Curl Spotify 客戶端初始化:")
        print(f"  CLIENT_ID: {self.client_id[:10]}***")
        print(f"  CLIENT_SECRET: {self.client_secret[:10]}***")
        print(f"  REFRESH_TOKEN: {self.refresh_token[:20]}***")
    
    def get_access_token(self):
        """使用 curl 獲取 Access Token"""
        print("🔑 使用 curl 獲取 Access Token...")
        
        # 編碼憑證
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_base64 = base64.b64encode(auth_str.encode()).decode()
        
        # 構建 curl 命令
        cmd = [
            'curl', '-s', '-X', 'POST',
            'https://accounts.spotify.com/api/token',
            '-H', f'Authorization: Basic {auth_base64}',
            '-H', 'Content-Type: application/x-www-form-urlencoded',
            '-d', f'grant_type=refresh_token&refresh_token={self.refresh_token}',
            '--max-time', '50'
        ]
        
        print("📤 執行 curl 命令...")
        
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=35
            )
            
            if result.returncode == 0:
                response_data = json.loads(result.stdout)
                
                if 'access_token' in response_data:
                    self.access_token = response_data['access_token']
                    expires_in = response_data.get('expires_in', 3600)
                    
                    print(f"✅ Access Token 獲取成功!")
                    print(f"🕐 Token 過期時間: {expires_in} 秒")
                    return True
                else:
                    error_msg = response_data.get('error', 'Unknown error')
                    print(f"❌ Token 獲取失敗: {error_msg}")
                    return False
            else:
                print(f"❌ curl 執行失敗: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("❌ curl token 請求超時")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ 解析 JSON 回應失敗: {e}")
            return False
        except Exception as e:
            print(f"❌ Token 獲取異常: {e}")
            return False
    
    def make_api_call(self, endpoint: str, params: dict = None):
        """使用 curl 呼叫 Spotify API"""
        if not self.access_token:
            if not self.get_access_token():
                raise Exception("無法獲取 Access Token")
        
        url = f"https://api.spotify.com/v1/{endpoint}"
        
        # 構建 curl 命令
        cmd = [
            'curl', '-s', '-X', 'GET',
            url,
            '-H', f'Authorization: Bearer {self.access_token}',
            '--max-time', '30'
        ]
        
        # 添加查詢參數
        if params:
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            cmd[4] = f"{url}?{param_string}"
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=25
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            else:
                print(f"❌ API 呼叫失敗: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print("❌ API 呼叫超時")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ 解析 API 回應失敗: {e}")
            return None
        except Exception as e:
            print(f"❌ API 呼叫異常: {e}")
            return None
    
    def get_recently_played(self, limit=50):
        """獲取最近播放記錄"""
        print(f"🎵 獲取最近 {limit} 首播放記錄...")
        response = self.make_api_call('me/player/recently-played', {'limit': limit})
        
        if response and 'items' in response:
            print(f"✅ 成功獲取 {len(response['items'])} 首歌曲")
            return response['items']
        else:
            print("❌ 獲取播放記錄失敗")
            return []
    
    def get_several_tracks(self, track_ids: list):
        """批次獲取歌曲詳細資訊 (最多50個)"""
        if len(track_ids) > 50:
            print(f"⚠️ 歌曲數量超過限制，只處理前50個")
            track_ids = track_ids[:50]
        
        ids_string = ','.join(track_ids)
        print(f"🎵 批次獲取 {len(track_ids)} 首歌曲詳細資訊...")
        
        response = self.make_api_call('tracks', {'ids': ids_string})
        
        if response and 'tracks' in response:
            tracks = response['tracks']
            print(f"✅ 成功獲取 {len(tracks)} 首歌曲詳細資訊")
            return tracks
        else:
            print("❌ 獲取歌曲詳細資訊失敗")
            return []
    
    def get_several_artists(self, artist_ids: list):
        """批次獲取藝術家資訊 (最多50個)"""
        if len(artist_ids) > 50:
            print(f"⚠️ 藝術家數量超過限制，只處理前50個")
            artist_ids = artist_ids[:50]
        
        ids_string = ','.join(artist_ids)
        print(f"🎤 批次獲取 {len(artist_ids)} 位藝術家資訊...")
        
        response = self.make_api_call('artists', {'ids': ids_string})
        
        if response and 'artists' in response:
            artists = response['artists']
            print(f"✅ 成功獲取 {len(artists)} 位藝術家資訊")
            return artists
        else:
            print("❌ 獲取藝術家資訊失敗")
            return []
    
    def get_several_albums(self, album_ids: list):
        """批次獲取專輯資訊 (最多20個)"""
        if len(album_ids) > 20:
            print(f"⚠️ 專輯數量超過限制，只處理前20個")
            album_ids = album_ids[:20]
        
        ids_string = ','.join(album_ids)
        print(f"💿 批次獲取 {len(album_ids)} 張專輯資訊...")
        
        response = self.make_api_call('albums', {'ids': ids_string})
        
        if response and 'albums' in response:
            albums = response['albums']
            print(f"✅ 成功獲取 {len(albums)} 張專輯資訊")
            return albums
        else:
            print("❌ 獲取專輯資訊失敗")
            return []

# ============================================================================
# DAG 配置
# ============================================================================

default_args = {
    'owner': 'enhanced-spotify',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=15)
}

dag = DAG(
    'enhanced_spotify_tracker',
    default_args=default_args,
    description='🎵 完善版 Spotify 音樂追蹤 (聽歌記錄 + 詳細資訊收集)',
    schedule='0 */2 * * *',  # 每兩小時
    max_active_runs=1,
    catchup=False,
    tags=['spotify', 'enhanced', 'batch-api']
)

# ============================================================================
# Task 函數
# ============================================================================

def check_curl_availability(**context):
    """檢查 curl 是否可用"""
    print("🔍 檢查 curl 可用性...")
    
    try:
        result = subprocess.run(['curl', '--version'], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            version_info = result.stdout.split('\n')[0]
            print(f"✅ curl 可用: {version_info}")
            
            # 測試基本網路連接
            test_result = subprocess.run(
                ['curl', '-I', 'https://accounts.spotify.com', '--max-time', '10'],
                capture_output=True, text=True, timeout=15
            )
            
            if test_result.returncode == 0:
                print("✅ curl 網路連接測試成功")
                return {"status": "success", "curl_version": version_info}
            else:
                print(f"❌ curl 網路測試失敗: {test_result.stderr}")
                raise Exception(f"curl network test failed: {test_result.stderr}")
        else:
            print(f"❌ curl 不可用: {result.stderr}")
            raise Exception(f"curl not available: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print("❌ curl 檢查超時")
        raise Exception("curl check timeout")
    except Exception as e:
        print(f"❌ curl 檢查失敗: {e}")
        raise

def fetch_spotify_data(**context):
    """獲取 Spotify 數據 - 包含聽歌記錄和詳細資訊收集"""
    print("🎵 開始獲取 Spotify 數據...")
    batch_id = f"spotify_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = time.time()
    
    try:
        # 初始化客戶端
        client = CurlSpotifyClient()
        
        # 1. 獲取聽歌記錄
        print("\n" + "="*50)
        print("📻 第1階段：獲取聽歌記錄")
        print("="*50)
        
        listening_data = client.get_recently_played(limit=50)
        
        if not listening_data:
            print("⚠️ 沒有獲取到聽歌記錄")
            return {
                "status": "no_data",
                "batch_id": batch_id,
                "message": "No listening data found"
            }
        
        # 2. 收集需要查詢的 IDs
        print("\n" + "="*50)
        print("🔍 第2階段：分析需要收集的詳細資訊")
        print("="*50)
        
        new_track_ids = []
        new_artist_ids = []
        new_album_ids = []
        
        for item in listening_data:
            track = item['track']
            track_id = track['id']
            album_id = track['album']['id']
            
            # 檢查 track 是否需要更新
            if not exists_in_mongodb('track_details', track_id):
                new_track_ids.append(track_id)
                print(f"🆕 新歌曲: {track['name']}")
            
            # 檢查 album 是否需要更新
            if not exists_in_mongodb('album_catalog', album_id):
                new_album_ids.append(album_id)
                print(f"🆕 新專輯: {track['album']['name']}")
            
            # 檢查所有 artists
            for artist in track['artists']:
                artist_id = artist['id']
                if not exists_in_mongodb('artist_profiles', artist_id):
                    new_artist_ids.append(artist_id)
                    print(f"🆕 新藝術家: {artist['name']}")
        
        # 去重
        new_track_ids = list(set(new_track_ids))
        new_artist_ids = list(set(new_artist_ids))
        new_album_ids = list(set(new_album_ids))
        
        print(f"\n📊 收集摘要:")
        print(f"   🎵 需要收集的歌曲: {len(new_track_ids)}")
        print(f"   🎤 需要收集的藝術家: {len(new_artist_ids)}")
        print(f"   💿 需要收集的專輯: {len(new_album_ids)}")
        
        # 3. 批次呼叫 API 收集詳細資訊
        print("\n" + "="*50)
        print("🚀 第3階段：批次收集詳細資訊")
        print("="*50)
        
        collection_results = {
            'track_details': [],
            'artist_profiles': [],
            'album_catalog': []
        }
        
        # 批次獲取歌曲詳細資訊
        if new_track_ids:
            print(f"\n🎵 處理 {len(new_track_ids)} 首歌曲...")
            tracks_data = client.get_several_tracks(new_track_ids)
            
            for track in tracks_data:
                if track:  # 確保 track 不是 None
                    track_detail = {
                        'track_id': track['id'],
                        'name': track['name'],
                        'duration_ms': track['duration_ms'],
                        'explicit': track['explicit'],
                        'popularity': track['popularity'],
                        'preview_url': track.get('preview_url'),
                        'track_number': track.get('track_number'),
                        'artists': [{'id': a['id'], 'name': a['name']} for a in track['artists']],
                        'album': {
                            'id': track['album']['id'],
                            'name': track['album']['name']
                        },
                        'available_markets': track.get('available_markets', []),
                        'external_ids': track.get('external_ids', {}),
                        'external_urls': track.get('external_urls', {}),
                        'metadata': {
                            'first_seen': datetime.utcnow(),
                            'last_updated': datetime.utcnow(),
                            'play_count': 1
                        },
                        'raw_api_response': track
                    }
                    collection_results['track_details'].append(track_detail)
        
        # 批次獲取藝術家資訊
        if new_artist_ids:
            print(f"\n🎤 處理 {len(new_artist_ids)} 位藝術家...")
            artists_data = client.get_several_artists(new_artist_ids)
            
            for artist in artists_data:
                if artist:  # 確保 artist 不是 None
                    artist_profile = {
                        'artist_id': artist['id'],
                        'name': artist['name'],
                        'genres': artist.get('genres', []),
                        'popularity': artist.get('popularity', 0),
                        'followers': artist.get('followers', {}).get('total', 0),
                        'images': artist.get('images', []),
                        'external_urls': artist.get('external_urls', {}),
                        'metadata': {
                            'first_seen': datetime.utcnow(),
                            'last_updated': datetime.utcnow(),
                            'total_tracks_played': 1
                        },
                        'raw_api_response': artist
                    }
                    collection_results['artist_profiles'].append(artist_profile)
        
        # 批次獲取專輯資訊
        if new_album_ids:
            print(f"\n💿 處理 {len(new_album_ids)} 張專輯...")
            albums_data = client.get_several_albums(new_album_ids)
            
            for album in albums_data:
                if album:  # 確保 album 不是 None
                    album_info = {
                        'album_id': album['id'],
                        'name': album['name'],
                        'album_type': album.get('album_type'),
                        'release_date': album.get('release_date'),
                        'release_date_precision': album.get('release_date_precision'),
                        'total_tracks': album.get('total_tracks', 0),
                        'artists': [{'id': a['id'], 'name': a['name']} for a in album['artists']],
                        'images': album.get('images', []),
                        'genres': album.get('genres', []),
                        'label': album.get('label'),
                        'popularity': album.get('popularity', 0),
                        'external_urls': album.get('external_urls', {}),
                        'metadata': {
                            'first_seen': datetime.utcnow(),
                            'last_updated': datetime.utcnow(),
                            'total_plays': 1
                        },
                        'raw_api_response': album
                    }
                    collection_results['album_catalog'].append(album_info)
        
        # 4. 處理聽歌記錄
        print("\n" + "="*50)
        print("📝 第4階段：處理聽歌記錄")
        print("="*50)
        
        processed_listening = []
        for item in listening_data:
            track = item['track']
            played_at = datetime.fromisoformat(item['played_at'].replace('Z', '+00:00'))
            
            listening_record = {
                'track_id': track['id'],
                'played_at': played_at,
                'track_info': {
                    'name': track['name'],
                    'artists': [{'id': a['id'], 'name': a['name']} for a in track['artists']],
                    'album': {
                        'id': track['album']['id'],
                        'name': track['album']['name']
                    },
                    'duration_ms': track['duration_ms'],
                    'explicit': track['explicit'],
                    'popularity': track['popularity']
                },
                'batch_info': {
                    'batch_id': batch_id,
                    'collected_at': datetime.utcnow(),
                    'api_version': 'v1'
                },
                'raw_api_response': item
            }
            processed_listening.append(listening_record)
        
        collection_time = time.time() - start_time
        
        # 組合結果
        result = {
            "status": "success",
            "method": "curl_enhanced",
            "batch_id": batch_id,
            "collection_time": round(collection_time, 2),
            "listening_data": processed_listening,
            "detailed_collections": collection_results,
            "summary": {
                "total_listening_records": len(processed_listening),
                "new_tracks_collected": len(collection_results['track_details']),
                "new_artists_collected": len(collection_results['artist_profiles']),
                "new_albums_collected": len(collection_results['album_catalog'])
            }
        }
        
        print(f"\n✅ 數據收集完成! 耗時 {collection_time:.2f} 秒")
        print(f"📊 聽歌記錄: {len(processed_listening)}")
        print(f"🎵 新歌曲: {len(collection_results['track_details'])}")
        print(f"🎤 新藝術家: {len(collection_results['artist_profiles'])}")
        print(f"💿 新專輯: {len(collection_results['album_catalog'])}")
        
        # 傳遞結果給下個 Task
        context['task_instance'].xcom_push(key='spotify_data', value=result)
        return result
        
    except Exception as e:
        error_result = {
            "status": "failed",
            "method": "curl_enhanced",
            "batch_id": batch_id,
            "error": str(e),
            "collection_time": time.time() - start_time
        }
        
        print(f"❌ 數據收集失敗: {e}")
        context['task_instance'].xcom_push(key='spotify_data', value=error_result)
        raise

def store_enhanced_data(**context):
    """儲存完善的數據到 MongoDB"""
    print("🍃 開始儲存完善數據到 MongoDB...")
    
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_enhanced_spotify_data',
        key='spotify_data'
    )
    
    if not spotify_data or spotify_data.get('status') != 'success':
        print("❌ 沒有有效數據需要儲存")
        return {"status": "no_data"}
    
    storage_results = {}
    
    try:
        # 1. 儲存聽歌記錄
        listening_data = spotify_data.get('listening_data', [])
        if listening_data:
            print(f"📻 儲存 {len(listening_data)} 筆聽歌記錄...")
            result = store_to_mongodb('daily_listening_history', listening_data)
            storage_results['listening_history'] = result
        
        # 2. 儲存歌曲詳細資訊
        track_details = spotify_data.get('detailed_collections', {}).get('track_details', [])
        if track_details:
            print(f"🎵 儲存 {len(track_details)} 筆歌曲詳細資訊...")
            result = store_to_mongodb('track_details', track_details)
            storage_results['track_details'] = result
        
        # 3. 儲存藝術家資訊
        artist_profiles = spotify_data.get('detailed_collections', {}).get('artist_profiles', [])
        if artist_profiles:
            print(f"🎤 儲存 {len(artist_profiles)} 筆藝術家資訊...")
            result = store_to_mongodb('artist_profiles', artist_profiles)
            storage_results['artist_profiles'] = result
        
        # 4. 儲存專輯資訊
        album_catalog = spotify_data.get('detailed_collections', {}).get('album_catalog', [])
        if album_catalog:
            print(f"💿 儲存 {len(album_catalog)} 筆專輯資訊...")
            result = store_to_mongodb('album_catalog', album_catalog)
            storage_results['album_catalog'] = result
        
        # 5. 儲存批次執行記錄
        batch_log = {
            'batch_id': spotify_data['batch_id'],
            'execution_date': datetime.utcnow(),
            'status': 'completed',
            'summary': spotify_data['summary'],
            'storage_results': storage_results,
            'execution_time': spotify_data.get('collection_time', 0),
            'method': 'curl_enhanced'
        }
        
        print(f"📝 儲存批次執行記錄...")
        batch_result = store_to_mongodb('batch_execution_log', [batch_log])
        storage_results['batch_log'] = batch_result
        
        # 計算總儲存統計
        total_inserted = 0
        total_attempted = 0
        
        for collection, result in storage_results.items():
            if result.get('status') == 'success':
                total_inserted += result.get('inserted_count', 0)
                total_attempted += result.get('total_attempted', 0)
        
        final_result = {
            'status': 'success',
            'batch_id': spotify_data['batch_id'],
            'collections_updated': len(storage_results),
            'total_inserted': total_inserted,
            'total_attempted': total_attempted,
            'storage_details': storage_results
        }
        
        print(f"\n✅ 數據儲存完成!")
        print(f"📊 總共儲存: {total_inserted}/{total_attempted}")
        print(f"🗂️ 更新集合: {len(storage_results)}")
        
        context['task_instance'].xcom_push(key='storage_results', value=final_result)
        return final_result
        
    except Exception as e:
        error_result = {
            'status': 'failed',
            'batch_id': spotify_data.get('batch_id', 'unknown'),
            'error': str(e),
            'partial_results': storage_results
        }
        
        print(f"❌ 數據儲存失敗: {e}")
        context['task_instance'].xcom_push(key='storage_results', value=error_result)
        return error_result

def log_enhanced_summary(**context):
    """記錄完善版執行摘要"""
    execution_date = context['ds']
    
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_enhanced_spotify_data',
        key='spotify_data'
    ) or {}
    
    storage_results = context['task_instance'].xcom_pull(
        task_ids='store_enhanced_data',
        key='storage_results'
    ) or {}
    
    print("\n" + "=" * 80)
    print("🎵 完善版 Spotify 音樂追蹤執行報告")
    print("=" * 80)
    print(f"📅 執行日期: {execution_date}")
    print(f"🏷️ 批次 ID: {spotify_data.get('batch_id', 'unknown')}")
    print(f"🔧 方法: {spotify_data.get('method', 'unknown')}")
    print(f"⏱️ 收集時間: {spotify_data.get('collection_time', 0)} 秒")
    print("")
    
    # Spotify API 收集統計
    summary = spotify_data.get('summary', {})
    print("🎵 Spotify API 收集:")
    print(f"   📻 聽歌記錄: {summary.get('total_listening_records', 0)}")
    print(f"   🎵 新歌曲: {summary.get('new_tracks_collected', 0)}")
    print(f"   🎤 新藝術家: {summary.get('new_artists_collected', 0)}")
    print(f"   💿 新專輯: {summary.get('new_albums_collected', 0)}")
    print("")
    
    # MongoDB 儲存統計
    print("🍃 MongoDB 儲存:")
    storage_details = storage_results.get('storage_details', {})
    
    for collection, result in storage_details.items():
        status_icon = "✅" if result.get('status') == 'success' else "❌"
        inserted = result.get('inserted_count', 0)
        attempted = result.get('total_attempted', 0)
        print(f"   {status_icon} {collection}: {inserted}/{attempted}")
    
    print(f"   📊 總計: {storage_results.get('total_inserted', 0)}/{storage_results.get('total_attempted', 0)}")
    print("")
    
    # 成功率計算
    if spotify_data.get('status') == 'success' and storage_results.get('status') == 'success':
        success_rate = 100
        status_emoji = "🎉"
        status_msg = "完全成功"
    elif spotify_data.get('status') == 'success':
        success_rate = 75
        status_emoji = "⚠️"
        status_msg = "數據收集成功，儲存部分失敗"
    else:
        success_rate = 0
        status_emoji = "❌"
        status_msg = "執行失敗"
    
    print("📈 執行狀態:")
    print(f"   {status_emoji} 狀態: {status_msg}")
    print(f"   📊 成功率: {success_rate}%")
    print("")
    
    # 系統改進說明
    print("🚀 系統改進亮點:")
    print("   ✅ 智能去重 - 避免重複收集已存在的資料")
    print("   ✅ 批次 API - 大幅提升效率（50首歌曲/次）")
    print("   ✅ 完整結構 - 聽歌記錄 + 歌曲/藝術家/專輯詳細資訊")
    print("   ✅ 容錯設計 - 單項失敗不影響整體執行")
    print("   ✅ curl 方案 - 解決 Python requests 卡住問題")
    print("")
    
    # 資料品質說明
    if summary.get('total_listening_records', 0) > 0:
        print("📊 資料品質:")
        new_items = (summary.get('new_tracks_collected', 0) + 
                    summary.get('new_artists_collected', 0) + 
                    summary.get('new_albums_collected', 0))
        
        if new_items > 0:
            print(f"   🆕 新發現內容: {new_items} 項")
            print("   🔄 系統正在持續豐富音樂資料庫")
        else:
            print("   ♻️ 所有內容已存在，資料庫保持最新")
        
        print(f"   📈 資料涵蓋度持續提升")
    
    print("")
    print("✨ 下次執行時間: 2 小時後")
    print("💡 提示: 可在 MongoDB 查看詳細資料結構")
    print("=" * 80)
    
    return "✅ 完善版音樂追蹤執行完成"

# ============================================================================
# Task 定義
# ============================================================================

check_curl_task = PythonOperator(
    task_id='check_curl_availability',
    python_callable=check_curl_availability,
    dag=dag
)

fetch_enhanced_data_task = PythonOperator(
    task_id='fetch_enhanced_spotify_data',
    python_callable=fetch_spotify_data,
    dag=dag
)

store_enhanced_data_task = PythonOperator(
    task_id='store_enhanced_data',
    python_callable=store_enhanced_data,
    dag=dag
)

summary_task = PythonOperator(
    task_id='log_enhanced_summary',
    python_callable=log_enhanced_summary,
    dag=dag
)

# ============================================================================
# Task 依賴關係
# ============================================================================

check_curl_task >> fetch_enhanced_data_task >> store_enhanced_data_task >> summary_task