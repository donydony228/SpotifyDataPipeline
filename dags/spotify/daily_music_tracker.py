# dags/spotify/daily_music_tracker.py
# 每日 Spotify 聽歌記錄追蹤 DAG

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import base64
import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
from dotenv import load_dotenv

# ============================================================================
# DAG 配置
# ============================================================================

default_args = {
    'owner': 'spotify-music-tracker',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

dag = DAG(
    'daily_spotify_music_tracker',
    default_args=default_args,
    description='🎵 每日 Spotify 聽歌記錄追蹤與儲存',
    schedule='@daily',  # 每天執行一次
    max_active_runs=1,  # 避免重疊執行
    catchup=False,      # 不回補歷史執行
    tags=['spotify', 'music', 'daily-tracking']
)

# ============================================================================
# Spotify API 客戶端類別
# ============================================================================

class SpotifyClient:
    """Spotify API 客戶端 - 基於成功的 Jupyter 測試"""
    
    def __init__(self):
        # 載入環境變數
        load_dotenv()
        
        self.client_id = os.getenv('SPOTIFY_CLIENT_ID', '2d2343762689494080664bd26ccc898f')
        self.client_secret = os.getenv('SPOTIFY_CLIENT_SECRET', '987c2fe892154b788df9790a04d84f6c')
        self.refresh_token = os.getenv('SPOTIFY_REFRESH_TOKEN', 'AQBk4fvESczjR30qGozNEDAe82YOJr_zJmR4Ga5_LTfTBH1xoFl1wAT4hIA9DieRyl1Vxg-Vlh9Vi2dqGf0h0iAPjIVkyPw6MjuvIAl6-02Qlh-6Bf55zKDGdhj8r7vp_F8')
        
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError("缺少必要的 Spotify API 憑證")
        
        self.access_token = None
    
    def get_access_token(self):
        """用 refresh_token 獲取新的 access_token"""
        print("🔑 正在獲取 Spotify Access Token...")
        
        # 編碼 credentials
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_base64 = base64.b64encode(auth_str.encode()).decode()
        
        # 請求新的 access token
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            headers={
                'Authorization': f'Basic {auth_base64}',
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            },
            timeout=30
        )
        
        if response.status_code == 200:
            tokens = response.json()
            self.access_token = tokens['access_token']
            print(f"✅ Access Token 獲取成功 (有效期: {tokens['expires_in']} 秒)")
            return self.access_token
        else:
            error_msg = f"獲取 Access Token 失敗: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)
    
    def get_recently_played(self, limit=50):
        """
        獲取最近播放的歌曲
        
        參數:
            limit: 要獲取的歌曲數量 (最多 50)
        
        返回:
            List[Dict] 包含播放記錄
        """
        print(f"🎵 正在獲取最近播放的 {limit} 首歌...")
        
        if not self.access_token:
            self.get_access_token()
        
        url = "https://api.spotify.com/v1/me/player/recently-played"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {"limit": limit}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            items = data['items']
            
            print(f"✅ 成功獲取 {len(items)} 首歌曲")
            return items
            
        elif response.status_code == 401:
            print("❌ Token 無效或過期，嘗試重新獲取...")
            self.get_access_token()
            # 重試一次
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                items = data['items']
                print(f"✅ 重試成功，獲取 {len(items)} 首歌曲")
                return items
            else:
                raise Exception(f"重試後仍然失敗: {response.status_code} - {response.text}")
        else:
            error_msg = f"請求失敗: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)

# ============================================================================
# Task 函數定義
# ============================================================================

def check_spotify_credentials(**context):
    """檢查 Spotify API 憑證是否設定正確"""
    print("🔍 檢查 Spotify API 憑證...")
    
    required_env_vars = {
        'SPOTIFY_CLIENT_ID': 'Spotify 客戶端 ID',
        'SPOTIFY_CLIENT_SECRET': 'Spotify 客戶端密鑰',
        'SPOTIFY_REFRESH_TOKEN': 'Spotify 刷新令牌'
    }
    
    missing_vars = []
    for var_name, description in required_env_vars.items():
        value = os.getenv(var_name)
        if value:
            masked = f"{value[:10]}***" if len(value) > 10 else "***"
            print(f"  ✅ {var_name}: {masked}")
        else:
            print(f"  ❌ {var_name}: 未設定 ({description})")
            missing_vars.append(var_name)
    
    if missing_vars:
        error_msg = f"缺少必要的環境變數: {missing_vars}"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    
    print("✅ 所有 Spotify 憑證都已正確設定")
    return "Credentials check passed"

def fetch_spotify_data(**context):
    """從 Spotify API 獲取聽歌記錄"""
    execution_date = context['ds']  # YYYY-MM-DD 格式
    batch_id = f"daily_spotify_{execution_date.replace('-', '')}"
    
    print(f"🎯 開始獲取 {execution_date} 的 Spotify 聽歌記錄...")
    print(f"📦 批次 ID: {batch_id}")
    
    try:
        # 初始化 Spotify 客戶端
        spotify = SpotifyClient()
        
        # 獲取最近播放的歌曲
        raw_items = spotify.get_recently_played(limit=50)
        
        if not raw_items:
            print("⚠️  沒有獲取到任何聽歌記錄")
            return {"status": "no_data", "message": "No listening history found"}
        
        # 整理資料格式 (與 Jupyter Notebook 中的邏輯一致)
        processed_tracks = []
        
        for item in raw_items:
            track = item['track']
            played_at = item['played_at']
            
            # 轉換時間格式
            played_time = datetime.fromisoformat(played_at.replace('Z', '+00:00'))
            
            # 建立符合 MongoDB schema 的格式
            track_record = {
                'track_id': track['id'],
                'played_at': played_time,
                'track_info': {
                    'name': track['name'],
                    'artists': [
                        {
                            'id': artist['id'],
                            'name': artist['name']
                        } for artist in track['artists']
                    ],
                    'album': {
                        'id': track['album']['id'],
                        'name': track['album']['name'],
                        'release_date': track['album'].get('release_date', ''),
                        'images': track['album'].get('images', [])
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
                'raw_api_response': item  # 保存完整的 API 回應
            }
            
            processed_tracks.append(track_record)
        
        # 統計結果
        result = {
            'status': 'success',
            'batch_id': batch_id,
            'total_tracks': len(processed_tracks),
            'tracks_data': processed_tracks,
            'collection_time': datetime.utcnow().isoformat()
        }
        
        print(f"✅ 成功處理 {len(processed_tracks)} 首歌曲")
        print(f"📊 資料時間範圍: {min(t['played_at'] for t in processed_tracks)} 到 {max(t['played_at'] for t in processed_tracks)}")
        
        # 儲存到 XCom 供下個 Task 使用
        context['task_instance'].xcom_push(key='spotify_data', value=result)
        
        return result
        
    except Exception as e:
        error_msg = f"獲取 Spotify 資料失敗: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)

def validate_music_data(**context):
    """驗證音樂資料品質"""
    print("🔍 開始驗證音樂資料品質...")
    
    # 從上一個 Task 獲取資料
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_spotify_data',
        key='spotify_data'
    )
    
    if not spotify_data or spotify_data['status'] != 'success':
        print("❌ 沒有有效的 Spotify 資料需要驗證")
        return {"status": "no_data", "message": "No valid data to validate"}
    
    tracks_data = spotify_data['tracks_data']
    
    print(f"📊 開始驗證 {len(tracks_data)} 筆音樂記錄...")
    
    validation_results = {
        'total_tracks': len(tracks_data),
        'valid_tracks': 0,
        'invalid_tracks': 0,
        'quality_issues': [],
        'validation_details': {}
    }
    
    valid_tracks = []
    
    for i, track in enumerate(tracks_data):
        track_issues = []
        
        # 必要欄位檢查
        required_fields = ['track_id', 'played_at', 'track_info']
        for field in required_fields:
            if not track.get(field):
                track_issues.append(f"缺少必要欄位: {field}")
        
        # track_info 詳細檢查
        if track.get('track_info'):
            track_info = track['track_info']
            required_track_info = ['name', 'artists', 'album']
            for field in required_track_info:
                if not track_info.get(field):
                    track_issues.append(f"缺少 track_info.{field}")
        
        # 資料型態檢查
        if track.get('track_info', {}).get('duration_ms'):
            if not isinstance(track['track_info']['duration_ms'], int):
                track_issues.append("duration_ms 應為整數")
        
        # 計算品質分數
        total_checks = 8  # 總檢查項目數
        passed_checks = total_checks - len(track_issues)
        quality_score = passed_checks / total_checks
        
        if track_issues:
            validation_results['invalid_tracks'] += 1
            validation_results['quality_issues'].append({
                'track_index': i,
                'track_id': track.get('track_id', 'unknown'),
                'track_name': track.get('track_info', {}).get('name', 'unknown'),
                'issues': track_issues,
                'quality_score': quality_score
            })
            print(f"⚠️  Track {i+1} 品質問題: {track_issues}")
        else:
            validation_results['valid_tracks'] += 1
            track['data_quality'] = {'score': quality_score, 'validated_at': datetime.utcnow()}
            valid_tracks.append(track)
    
    # 計算整體統計
    validation_results['validation_details'] = {
        'total_artists': len(set(
            artist['name'] 
            for track in valid_tracks 
            for artist in track.get('track_info', {}).get('artists', [])
        )),
        'total_albums': len(set(
            track.get('track_info', {}).get('album', {}).get('name', '')
            for track in valid_tracks
        )),
        'avg_duration_ms': sum(
            track.get('track_info', {}).get('duration_ms', 0)
            for track in valid_tracks
        ) / len(valid_tracks) if valid_tracks else 0,
        'explicit_count': sum(
            1 for track in valid_tracks
            if track.get('track_info', {}).get('explicit', False)
        )
    }
    
    print(f"✅ 資料驗證完成:")
    print(f"   有效記錄: {validation_results['valid_tracks']}")
    print(f"   無效記錄: {validation_results['invalid_tracks']}")
    print(f"   不重複藝術家: {validation_results['validation_details']['total_artists']}")
    print(f"   不重複專輯: {validation_results['validation_details']['total_albums']}")
    
    # 更新結果並傳遞給下個 Task
    validated_result = {
        'status': 'validated',
        'batch_id': spotify_data['batch_id'],
        'valid_tracks': valid_tracks,
        'validation_results': validation_results,
        'total_valid': len(valid_tracks)
    }
    
    context['task_instance'].xcom_push(key='validated_data', value=validated_result)
    
    return f"Validated {len(valid_tracks)} valid tracks"

def store_to_mongodb(**context):
    """儲存資料到 MongoDB Atlas"""
    print("🍃 開始儲存資料到 MongoDB Atlas...")
    
    # 從上一個 Task 獲取驗證後的資料
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_music_data',
        key='validated_data'
    )
    
    if not validated_data or not validated_data.get('valid_tracks'):
        print("⚠️  沒有有效資料需要儲存")
        return {"status": "no_data", "message": "No valid data to store"}
    
    valid_tracks = validated_data['valid_tracks']
    batch_id = validated_data['batch_id']
    
    print(f"📦 準備儲存 {len(valid_tracks)} 筆記錄到 MongoDB...")
    print(f"🏷️  批次 ID: {batch_id}")
    
    try:
        # 連接 MongoDB Atlas
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'music_data')
        
        if not mongodb_url:
            raise ValueError("MONGODB_ATLAS_URL 環境變數未設定")
        
        print(f"🔗 連接到 MongoDB: {db_name}")
        
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db['daily_listening_history']
        
        # 批次插入/更新資料 (使用 upsert 避免重複)
        inserted_count = 0
        updated_count = 0
        
        for track_record in valid_tracks:
            # 使用 track_id 和 played_at 作為唯一識別
            filter_condition = {
                'track_id': track_record['track_id'],
                'played_at': track_record['played_at']
            }
            
            # 執行 upsert
            result = collection.replace_one(
                filter_condition,
                track_record,
                upsert=True
            )
            
            if result.upserted_id:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
        
        client.close()
        
        print(f"✅ MongoDB 儲存完成:")
        print(f"   新增記錄: {inserted_count}")
        print(f"   更新記錄: {updated_count}")
        print(f"   總計處理: {inserted_count + updated_count}")
        
        # 記錄儲存統計
        storage_stats = {
            'status': 'success',
            'mongodb_inserted': inserted_count,
            'mongodb_updated': updated_count,
            'mongodb_total': inserted_count + updated_count,
            'batch_id': batch_id,
            'collection': 'daily_listening_history',
            'database': db_name
        }
        
        context['task_instance'].xcom_push(key='storage_stats', value=storage_stats)
        
        return f"Stored {inserted_count + updated_count} tracks to MongoDB"
        
    except Exception as e:
        error_msg = f"MongoDB 儲存失敗: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)

def log_execution_summary(**context):
    """記錄執行摘要和統計"""
    print("📊 生成執行摘要...")
    
    execution_date = context['ds']
    
    # 收集所有 Task 的執行結果
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_spotify_data',
        key='spotify_data'
    ) or {}
    
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_music_data',
        key='validated_data'
    ) or {}
    
    storage_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_mongodb',
        key='storage_stats'
    ) or {}
    
    # 編譯完整的執行報告
    execution_summary = {
        'dag_id': context['dag'].dag_id,
        'execution_date': execution_date,
        'batch_id': spotify_data.get('batch_id', 'unknown'),
        
        # Spotify API 統計
        'spotify_api': {
            'total_fetched': spotify_data.get('total_tracks', 0),
            'collection_time': spotify_data.get('collection_time'),
            'status': spotify_data.get('status', 'unknown')
        },
        
        # 資料驗證統計
        'data_validation': validated_data.get('validation_results', {}),
        
        # 儲存統計
        'storage': {
            'mongodb': storage_stats
        },
        
        # 執行時間
        'execution_info': {
            'start_time': context['task_instance'].start_date.isoformat() if context['task_instance'].start_date else None,
            'end_time': datetime.utcnow().isoformat()
        }
    }
    
    print("📋 每日音樂追蹤執行報告")
    print("=" * 60)
    print(f"📅 執行日期: {execution_summary['execution_date']}")
    print(f"🏷️  批次 ID: {execution_summary['batch_id']}")
    print("")
    print("🎵 Spotify API 統計:")
    print(f"   獲取歌曲: {execution_summary['spotify_api']['total_fetched']}")
    print(f"   API 狀態: {execution_summary['spotify_api']['status']}")
    print("")
    print("🔍 資料驗證:")
    validation = execution_summary['data_validation']
    print(f"   有效記錄: {validation.get('valid_tracks', 0)}")
    print(f"   無效記錄: {validation.get('invalid_tracks', 0)}")
    print(f"   品質問題: {len(validation.get('quality_issues', []))}")
    print("")
    print("🍃 MongoDB 儲存:")
    mongodb_stats = execution_summary['storage']['mongodb']
    print(f"   新增: {mongodb_stats.get('mongodb_inserted', 0)}")
    print(f"   更新: {mongodb_stats.get('mongodb_updated', 0)}")
    print(f"   總計: {mongodb_stats.get('mongodb_total', 0)}")
    print("")
    print("=" * 60)
    
    # 儲存執行摘要
    context['task_instance'].xcom_push(key='execution_summary', value=execution_summary)
    
    return "Execution summary logged successfully"

# ============================================================================
# Task 定義
# ============================================================================

# Task 1: 檢查 Spotify 憑證
check_credentials_task = PythonOperator(
    task_id='check_spotify_credentials',
    python_callable=check_spotify_credentials,
    dag=dag
)

# Task 2: 從 Spotify API 獲取資料
fetch_data_task = PythonOperator(
    task_id='fetch_spotify_data',
    python_callable=fetch_spotify_data,
    dag=dag
)

# Task 3: 驗證資料品質
validate_data_task = PythonOperator(
    task_id='validate_music_data',
    python_callable=validate_music_data,
    dag=dag
)

# Task 4: 儲存到 MongoDB
store_mongodb_task = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=store_to_mongodb,
    dag=dag
)

# Task 5: 記錄執行摘要
summary_task = PythonOperator(
    task_id='log_execution_summary',
    python_callable=log_execution_summary,
    dag=dag
)

# ============================================================================
# Task 依賴關係
# ============================================================================

# 線性執行流程
check_credentials_task >> fetch_data_task >> validate_data_task >> store_mongodb_task >> summary_task