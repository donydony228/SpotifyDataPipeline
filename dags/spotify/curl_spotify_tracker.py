# dags/spotify/curl_spotify_tracker.py
# 使用 curl 替代 requests 的版本 - 解決 Python requests 卡住問題

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
    env_files = ['.env', 'airflow_home/.env', '../.env']
    
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
                    print(f"  Token: {self.access_token[:20]}***")
                    print(f"  有效期: {expires_in} 秒")
                    
                    return self.access_token
                else:
                    print(f"❌ 回應中沒有 access_token: {result.stdout}")
                    raise Exception(f"Invalid response: {result.stdout}")
            else:
                print(f"❌ curl 命令失敗:")
                print(f"  Return code: {result.returncode}")
                print(f"  Stdout: {result.stdout}")
                print(f"  Stderr: {result.stderr}")
                raise Exception(f"curl failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print("❌ curl 命令超時")
            raise Exception("curl command timeout")
        except json.JSONDecodeError as e:
            print(f"❌ JSON 解析失敗: {e}")
            print(f"  Raw output: {result.stdout}")
            raise Exception(f"JSON parse error: {e}")
        except Exception as e:
            print(f"❌ curl 執行異常: {e}")
            raise
    
    def get_recently_played(self, limit=20):
        """使用 curl 獲取最近播放記錄"""
        print(f"🎵 使用 curl 獲取最近 {limit} 首歌...")
        
        if not self.access_token:
            self.get_access_token()
        
        # 構建 curl 命令
        cmd = [
            'curl', '-s', '-X', 'GET',
            f'https://api.spotify.com/v1/me/player/recently-played?limit={limit}',
            '-H', f'Authorization: Bearer {self.access_token}',
            '--max-time', '30'
        ]
        
        print("📤 執行 curl API 請求...")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=35
            )
            
            if result.returncode == 0:
                try:
                    response_data = json.loads(result.stdout)
                    
                    if 'items' in response_data:
                        items = response_data['items']
                        print(f"✅ 成功獲取 {len(items)} 首歌曲")
                        return items
                    
                    elif 'error' in response_data:
                        error = response_data['error']
                        if error.get('status') == 401:
                            print("🔄 Token 可能過期，重新獲取...")
                            self.get_access_token()
                            
                            # 重試 - 更新 Authorization header
                            cmd[3] = f'Authorization: Bearer {self.access_token}'
                            
                            retry_result = subprocess.run(
                                cmd,
                                capture_output=True,
                                text=True,
                                timeout=35
                            )
                            
                            if retry_result.returncode == 0:
                                retry_data = json.loads(retry_result.stdout)
                                items = retry_data.get('items', [])
                                print(f"✅ 重試成功，獲取 {len(items)} 首歌曲")
                                return items
                        
                        print(f"❌ API 錯誤: {error}")
                        raise Exception(f"API error: {error}")
                    
                    else:
                        print(f"⚠️ 未預期的回應格式: {result.stdout[:100]}...")
                        return []
                        
                except json.JSONDecodeError as e:
                    # 檢查是否是 204 回應 (無內容)
                    if not result.stdout.strip():
                        print("⚠️ 空回應 (可能是 204 No Content)")
                        return []
                    else:
                        print(f"❌ JSON 解析失敗: {e}")
                        print(f"  Raw output: {result.stdout[:200]}...")
                        raise Exception(f"JSON parse error: {e}")
            
            else:
                print(f"❌ curl API 請求失敗:")
                print(f"  Return code: {result.returncode}")
                print(f"  Stderr: {result.stderr}")
                raise Exception(f"curl API failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print("❌ curl API 請求超時")
            raise Exception("curl API timeout")
        except Exception as e:
            print(f"❌ curl API 執行異常: {e}")
            raise

# ============================================================================
# DAG 配置
# ============================================================================

default_args = {
    'owner': 'curl-spotify',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=10)
}

dag = DAG(
    'curl_spotify_tracker',
    default_args=default_args,
    description='🔧 使用 curl 的 Spotify 音樂追蹤 (解決 requests 問題)',
    schedule='0 */2 * * *', # 每兩小時
    max_active_runs=1,
    catchup=False,
    tags=['spotify', 'curl', 'working-solution']
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

def fetch_curl_spotify_data(**context):
    """使用 curl 獲取 Spotify 資料"""
    execution_date = context['ds']
    batch_id = f"curl_spotify_{execution_date.replace('-', '')}"
    
    print(f"🎵 開始使用 curl 獲取 {execution_date} 的 Spotify 資料...")
    print(f"📦 批次 ID: {batch_id}")
    
    start_time = time.time()
    
    try:
        # 初始化 curl 客戶端
        client = CurlSpotifyClient()
        
        # 獲取播放記錄
        items = client.get_recently_played(limit=30)
        
        # 處理資料
        processed_tracks = []
        for item in items:
            track = item['track']
            processed_track = {
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_name': track['artists'][0]['name'],
                'album_name': track['album']['name'],
                'played_at': item['played_at'],
                'duration_ms': track['duration_ms'],
                'popularity': track.get('popularity', 0),
                'explicit': track.get('explicit', False),
                'batch_id': batch_id,
                'execution_date': execution_date,
                'collected_at': datetime.utcnow().isoformat(),
                'method': 'curl'  # 標記使用 curl 方法
            }
            processed_tracks.append(processed_track)
        
        collection_time = time.time() - start_time
        
        result = {
            'status': 'success',
            'method': 'curl',
            'batch_id': batch_id,
            'execution_date': execution_date,
            'total_tracks': len(processed_tracks),
            'collection_time': round(collection_time, 2),
            'tracks_data': processed_tracks
        }
        
        print(f"✅ curl 資料收集完成:")
        print(f"   播放記錄: {len(processed_tracks)}")
        print(f"   收集時間: {collection_time:.2f} 秒")
        print(f"   方法: curl")
        
        if processed_tracks:
            print("🎧 示例歌曲:")
            for i, track in enumerate(processed_tracks[:3]):
                print(f"   {i+1}. {track['track_name']} - {track['artist_name']}")
        
        # 傳遞給下一個 Task
        context['task_instance'].xcom_push(key='spotify_data', value=result)
        return result
        
    except Exception as e:
        print(f"❌ curl 資料收集失敗: {str(e)}")
        raise

def store_curl_data(**context):
    """儲存 curl 獲取的資料"""
    print("💾 開始儲存 curl 獲取的資料...")
    
    # 獲取資料
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_curl_spotify_data',
        key='spotify_data'
    )
    
    if not spotify_data or not spotify_data.get('tracks_data'):
        print("⚠️ 沒有資料需要儲存")
        return {"status": "no_data"}
    
    tracks_data = spotify_data['tracks_data']
    batch_id = spotify_data['batch_id']
    
    print(f"📦 準備儲存 {len(tracks_data)} 筆記錄...")
    print(f"🏷️ 批次 ID: {batch_id}")
    print(f"🔧 資料來源: {spotify_data.get('method', 'unknown')}")
    
    try:
        # 載入 MongoDB 憑證
        force_load_env_vars()
        
        mongodb_url = os.environ.get('MONGODB_ATLAS_URL')
        db_name = os.environ.get('MONGODB_ATLAS_DB_NAME', 'music_data')
        
        if not mongodb_url:
            print("⚠️ MongoDB URL 未設定，只在記憶體中處理")
            return {
                "status": "memory_only",
                "tracks_processed": len(tracks_data),
                "batch_id": batch_id,
                "method": "curl"
            }
        
        print(f"🔗 連接到 MongoDB: {db_name}")
        
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db['daily_listening_history']
        
        # 批次處理
        insert_count = 0
        update_count = 0
        
        for track in tracks_data:
            filter_query = {
                'track_id': track['track_id'],
                'played_at': track['played_at']
            }
            
            result = collection.replace_one(filter_query, track, upsert=True)
            
            if result.upserted_id:
                insert_count += 1
            elif result.modified_count > 0:
                update_count += 1
        
        storage_stats = {
            'status': 'success',
            'method': 'curl',
            'mongodb_inserted': insert_count,
            'mongodb_updated': update_count,
            'mongodb_total': insert_count + update_count,
            'batch_id': batch_id,
            'database': db_name
        }
        
        print(f"✅ MongoDB 儲存完成:")
        print(f"   新增: {insert_count}")
        print(f"   更新: {update_count}")
        print(f"   總計: {insert_count + update_count}")
        print(f"   方法: curl")
        
        client.close()
        return storage_stats
        
    except Exception as e:
        print(f"❌ 儲存失敗: {str(e)}")
        return {
            "status": "storage_failed",
            "error": str(e),
            "tracks_processed_in_memory": len(tracks_data),
            "method": "curl"
        }

def log_curl_summary(**context):
    """記錄 curl 版本執行摘要"""
    execution_date = context['ds']
    
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_curl_spotify_data',
        key='spotify_data'
    ) or {}
    
    storage_result = context['task_instance'].xcom_pull(
        task_ids='store_curl_data'
    ) or {}
    
    print("\n" + "=" * 70)
    print("📋 curl 版本 Spotify 音樂追蹤執行報告")
    print("=" * 70)
    print(f"📅 執行日期: {execution_date}")
    print(f"🏷️ 批次 ID: {spotify_data.get('batch_id', 'unknown')}")
    print(f"🔧 請求方法: {spotify_data.get('method', 'unknown')}")
    print("")
    print("🎵 Spotify API:")
    print(f"   獲取歌曲: {spotify_data.get('total_tracks', 0)}")
    print(f"   收集時間: {spotify_data.get('collection_time', 0)} 秒")
    print(f"   狀態: {spotify_data.get('status', 'unknown')}")
    print("")
    print("💾 資料儲存:")
    print(f"   狀態: {storage_result.get('status', 'unknown')}")
    print(f"   方法: {storage_result.get('method', 'unknown')}")
    if storage_result.get('mongodb_total'):
        print(f"   MongoDB 記錄: {storage_result.get('mongodb_total', 0)}")
    print("")
    print("🎉 curl 方法成功解決了 Python requests 卡住的問題!")
    print("=" * 70)
    
    return "✅ curl 版本音樂追蹤執行完成"

# ============================================================================
# Task 定義
# ============================================================================

check_curl_task = PythonOperator(
    task_id='check_curl_availability',
    python_callable=check_curl_availability,
    dag=dag
)

fetch_data_task = PythonOperator(
    task_id='fetch_curl_spotify_data',
    python_callable=fetch_curl_spotify_data,
    dag=dag
)

store_data_task = PythonOperator(
    task_id='store_curl_data',
    python_callable=store_curl_data,
    dag=dag
)

summary_task = PythonOperator(
    task_id='log_curl_summary',
    python_callable=log_curl_summary,
    dag=dag
)

# 線性執行
check_curl_task >> fetch_data_task >> store_data_task >> summary_task