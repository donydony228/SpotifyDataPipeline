from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

import sys
import os
sys.path.append('/Users/desmond/airflow')

from utils.database import MongoDBConnection, PostgreSQLConnection
from utils.config import load_config
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple
import pymongo
from datetime import timezone

# ============================================================================
# DAG 設定
# ============================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 10, 20),    description='MongoDB → PostgreSQL 每日 ETL Pipeline',
    schedule_interval='0 2 * * *',  # 每日凌晨 2:00
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'spotify', 'daily']
)

# ============================================================================
# 輔助函數
# ============================================================================

def get_last_sync_timestamp(**context) -> str:
    """取得上次同步的時間戳，作為增量同步的起點"""
    try:
        config = load_config()
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        # 從 ETL 紀錄表取得上次成功同步的時間
        query = """
        SELECT last_sync_timestamp 
        FROM dwh.etl_batch_log 
        WHERE job_name = 'daily_listening_sync' 
        AND status = 'SUCCESS'
        ORDER BY completed_at DESC 
        LIMIT 1
        """
        
        result = pg_conn.execute_query(query)
        
        if result and len(result) > 0:
            last_sync = result[0][0]
            logging.info(f"上次同步時間: {last_sync}")
            return last_sync.isoformat()
        else:
            # 第一次執行，從昨天開始
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            logging.info(f"首次執行，從昨天開始: {yesterday}")
            return yesterday.isoformat()
            
    except Exception as e:
        logging.warning(f"無法取得上次同步時間，使用昨天作為起點: {e}")
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.isoformat()

def sync_listening_to_raw_staging(**context) -> Dict:
    """Task 1: 同步聽歌記錄 MongoDB → Raw Staging"""
    try:
        config = load_config()
        
        # 取得上次同步時間點
        last_sync_timestamp = context['task_instance'].xcom_pull(
            task_ids='get_sync_watermark'
        )
        
        # 連接資料庫
        mongo_conn = MongoDBConnection(config['database']['mongodb'])
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        # 從 MongoDB 取得新資料
        collection = mongo_conn.client['music_data']['daily_listening_history']
        
        # 查詢條件: created_at > 上次同步時間
        query = {
            'created_at': {
                '$gt': datetime.fromisoformat(last_sync_timestamp.replace('Z', '+00:00'))
            }
        }
        
        # 取得新記錄
        new_records = list(collection.find(query).sort('created_at', 1))
        
        if not new_records:
            logging.info("沒有新的聽歌記錄需要同步")
            return {
                'status': 'SUCCESS',
                'records_processed': 0,
                'message': '沒有新資料'
            }
        
        logging.info(f"找到 {len(new_records)} 筆新記錄")
        
        # 準備插入 PostgreSQL 的資料
        insert_data = []
        batch_id = context['ds_nodash'] + '_listening_sync'
        
        for record in new_records:
            # 處理嵌套的 track_info
            track_info = record.get('track_info', {})
            
            row = {
                'track_id': record.get('track_id'),
                'played_at': record.get('played_at'),
                'track_name': track_info.get('name'),
                'artist_id': track_info.get('artist_id'),
                'artist_name': track_info.get('artist_name'),
                'album_id': track_info.get('album_id'),
                'album_name': track_info.get('album_name'),
                'duration_ms': track_info.get('duration_ms'),
                'explicit': track_info.get('explicit', False),
                'popularity': track_info.get('popularity'),
                'batch_id': batch_id
            }
            
            insert_data.append(row)
        
        # 批次插入 PostgreSQL
        df = pd.DataFrame(insert_data)
        
        # 使用 UPSERT 避免重複
        insert_query = """
        INSERT INTO raw_staging.spotify_listening_raw 
        (track_id, played_at, track_name, artist_id, artist_name, 
         album_id, album_name, duration_ms, explicit, popularity, batch_id)
        VALUES (%(track_id)s, %(played_at)s, %(track_name)s, %(artist_id)s, 
                %(artist_name)s, %(album_id)s, %(album_name)s, %(duration_ms)s, 
                %(explicit)s, %(popularity)s, %(batch_id)s)
        ON CONFLICT (track_id, played_at) DO NOTHING
        """
        
        success_count = 0
        for _, row in df.iterrows():
            try:
                pg_conn.execute_query(insert_query, row.to_dict())
                success_count += 1
            except Exception as e:
                logging.warning(f"插入記錄失敗 {row['track_id']}: {e}")
                continue
        
        logging.info(f"成功同步 {success_count}/{len(new_records)} 筆記錄")
        
        return {
            'status': 'SUCCESS',
            'records_processed': success_count,
            'total_records': len(new_records),
            'batch_id': batch_id,
            'last_sync_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logging.error(f"同步聽歌記錄失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e),
            'records_processed': 0
        }

def process_time_fields(**context) -> Dict:
    """Task 2: 時間處理 Raw → Clean Staging"""
    try:
        config = load_config()
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        # 取得今日需要處理的資料
        batch_id = context['ds_nodash'] + '_listening_sync'
        
        # SQL: 從 Raw 轉換到 Clean (時間解析)
        process_query = """
        INSERT INTO clean_staging.listening_cleaned 
        (raw_id, track_id, played_at, played_date, played_hour, 
         played_day_of_week, played_month, played_year, is_weekend)
        SELECT 
            r.id,
            r.track_id,
            r.played_at,
            r.played_at::date,
            EXTRACT(HOUR FROM r.played_at),
            EXTRACT(DOW FROM r.played_at),
            EXTRACT(MONTH FROM r.played_at),
            EXTRACT(YEAR FROM r.played_at),
            EXTRACT(DOW FROM r.played_at) IN (0, 6)
        FROM raw_staging.spotify_listening_raw r
        WHERE r.batch_id = %s
        AND NOT EXISTS (
            SELECT 1 FROM clean_staging.listening_cleaned c 
            WHERE c.raw_id = r.id
        )
        """
        
        result = pg_conn.execute_query(process_query, (batch_id,))
        processed_count = pg_conn.cursor.rowcount
        
        logging.info(f"完成時間處理，處理 {processed_count} 筆記錄")
        
        return {
            'status': 'SUCCESS',
            'records_processed': processed_count
        }
        
    except Exception as e:
        logging.error(f"時間處理失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def load_to_warehouse(**context) -> Dict:
    """Task 3: 載入 DWH Clean → Fact Table"""
    try:
        config = load_config()
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        batch_id = context['ds_nodash'] + '_listening_sync'
        
        # SQL: 載入到事實表
        load_query = """
        INSERT INTO dwh.fact_listening 
        (date_key, track_key, artist_key, album_key, played_at, 
         listening_minutes, batch_id)
        SELECT 
            d.date_key,
            COALESCE(t.track_key, -1),  -- -1 表示未知
            COALESCE(a.artist_key, -1),
            COALESCE(al.album_key, -1),
            c.played_at,
            ROUND(r.duration_ms / 60000.0, 2),  -- 轉換為分鐘
            %s
        FROM clean_staging.listening_cleaned c
        JOIN raw_staging.spotify_listening_raw r ON c.raw_id = r.id
        JOIN dwh.dim_dates d ON d.date_value = c.played_date
        LEFT JOIN dwh.dim_tracks t ON t.track_id = r.track_id
        LEFT JOIN dwh.dim_artists a ON a.artist_id = r.artist_id  
        LEFT JOIN dwh.dim_albums al ON al.album_id = r.album_id
        WHERE r.batch_id = %s
        AND NOT EXISTS (
            SELECT 1 FROM dwh.fact_listening f 
            WHERE f.played_at = c.played_at AND f.track_key = t.track_key
        )
        """
        
        result = pg_conn.execute_query(load_query, (batch_id, batch_id))
        loaded_count = pg_conn.cursor.rowcount
        
        logging.info(f"載入到 DWH，新增 {loaded_count} 筆事實記錄")
        
        return {
            'status': 'SUCCESS',
            'records_loaded': loaded_count
        }
        
    except Exception as e:
        logging.error(f"載入 DWH 失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def sync_tracks_to_dwh(**context) -> Dict:
    """Task 4a: 同步歌曲維度表"""
    try:
        config = load_config()
        mongo_conn = MongoDBConnection(config['database']['mongodb'])
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        # 從 MongoDB 取得今日有聽過但 DWH 中沒有的歌曲
        collection = mongo_conn.client['music_data']['track_details']
        
        # 取得需要同步的 track_ids (今日聽過的)
        batch_id = context['ds_nodash'] + '_listening_sync'
        
        get_track_ids_query = """
        SELECT DISTINCT r.track_id 
        FROM raw_staging.spotify_listening_raw r
        WHERE r.batch_id = %s
        AND r.track_id NOT IN (SELECT track_id FROM dwh.dim_tracks)
        """
        
        result = pg_conn.execute_query(get_track_ids_query, (batch_id,))
        track_ids = [row[0] for row in result] if result else []
        
        if not track_ids:
            logging.info("沒有新的歌曲需要同步到維度表")
            return {'status': 'SUCCESS', 'records_synced': 0}
        
        logging.info(f"需要同步 {len(track_ids)} 首新歌曲")
        
        # 從 MongoDB 取得歌曲詳細資訊
        tracks_data = list(collection.find({'track_id': {'$in': track_ids}}))
        
        sync_count = 0
        for track in tracks_data:
            try:
                insert_track_query = """
                INSERT INTO dwh.dim_tracks 
                (track_id, track_name, duration_minutes, explicit, 
                 popularity, first_heard, total_plays)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (track_id) DO UPDATE SET
                total_plays = dim_tracks.total_plays + 1,
                last_updated = CURRENT_TIMESTAMP
                """
                
                params = (
                    track.get('track_id'),
                    track.get('name'),
                    round(track.get('duration_ms', 0) / 60000.0, 2),
                    track.get('explicit', False),
                    track.get('popularity'),
                    datetime.now().date()
                )
                
                pg_conn.execute_query(insert_track_query, params)
                sync_count += 1
                
            except Exception as e:
                logging.warning(f"同步歌曲失敗 {track.get('track_id')}: {e}")
                continue
        
        logging.info(f"成功同步 {sync_count} 首歌曲到維度表")
        
        return {
            'status': 'SUCCESS',
            'records_synced': sync_count
        }
        
    except Exception as e:
        logging.error(f"同步歌曲維度表失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def sync_artists_to_dwh(**context) -> Dict:
    """Task 4b: 同步藝術家維度表"""
    try:
        config = load_config()
        mongo_conn = MongoDBConnection(config['database']['mongodb'])
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        collection = mongo_conn.client['music_data']['artist_profiles']
        batch_id = context['ds_nodash'] + '_listening_sync'
        
        # 取得需要同步的 artist_ids
        get_artist_ids_query = """
        SELECT DISTINCT r.artist_id 
        FROM raw_staging.spotify_listening_raw r
        WHERE r.batch_id = %s
        AND r.artist_id IS NOT NULL
        AND r.artist_id NOT IN (SELECT artist_id FROM dwh.dim_artists)
        """
        
        result = pg_conn.execute_query(get_artist_ids_query, (batch_id,))
        artist_ids = [row[0] for row in result] if result else []
        
        if not artist_ids:
            return {'status': 'SUCCESS', 'records_synced': 0}
        
        # 從 MongoDB 取得藝術家資訊
        artists_data = list(collection.find({'artist_id': {'$in': artist_ids}}))
        
        sync_count = 0
        for artist in artists_data:
            try:
                insert_artist_query = """
                INSERT INTO dwh.dim_artists 
                (artist_id, artist_name, genres, popularity, 
                 followers_count, first_discovered, total_plays)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (artist_id) DO UPDATE SET
                total_plays = dim_artists.total_plays + 1,
                last_updated = CURRENT_TIMESTAMP
                """
                
                params = (
                    artist.get('artist_id'),
                    artist.get('name'),
                    artist.get('genres', []),
                    artist.get('popularity'),
                    artist.get('followers'),
                    datetime.now().date()
                )
                
                pg_conn.execute_query(insert_artist_query, params)
                sync_count += 1
                
            except Exception as e:
                logging.warning(f"同步藝術家失敗 {artist.get('artist_id')}: {e}")
                continue
        
        return {
            'status': 'SUCCESS', 
            'records_synced': sync_count
        }
        
    except Exception as e:
        logging.error(f"同步藝術家維度表失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def sync_albums_to_dwh(**context) -> Dict:
    """Task 4c: 同步專輯維度表"""
    try:
        config = load_config()
        mongo_conn = MongoDBConnection(config['database']['mongodb'])
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        collection = mongo_conn.client['music_data']['album_catalog']
        batch_id = context['ds_nodash'] + '_listening_sync'
        
        # 取得需要同步的 album_ids
        get_album_ids_query = """
        SELECT DISTINCT r.album_id 
        FROM raw_staging.spotify_listening_raw r
        WHERE r.batch_id = %s
        AND r.album_id IS NOT NULL
        AND r.album_id NOT IN (SELECT album_id FROM dwh.dim_albums)
        """
        
        result = pg_conn.execute_query(get_album_ids_query, (batch_id,))
        album_ids = [row[0] for row in result] if result else []
        
        if not album_ids:
            return {'status': 'SUCCESS', 'records_synced': 0}
        
        # 從 MongoDB 取得專輯資訊
        albums_data = list(collection.find({'album_id': {'$in': album_ids}}))
        
        sync_count = 0
        for album in albums_data:
            try:
                insert_album_query = """
                INSERT INTO dwh.dim_albums 
                (album_id, album_name, album_type, release_date, 
                 total_tracks, first_heard, total_plays)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (album_id) DO UPDATE SET
                total_plays = dim_albums.total_plays + 1,
                last_updated = CURRENT_TIMESTAMP
                """
                
                # 處理 release_date
                release_date = album.get('release_date')
                if isinstance(release_date, str):
                    try:
                        release_date = datetime.strptime(release_date, '%Y-%m-%d').date()
                    except:
                        release_date = None
                
                params = (
                    album.get('album_id'),
                    album.get('name'),
                    album.get('album_type'),
                    release_date,
                    album.get('total_tracks'),
                    datetime.now().date()
                )
                
                pg_conn.execute_query(insert_album_query, params)
                sync_count += 1
                
            except Exception as e:
                logging.warning(f"同步專輯失敗 {album.get('album_id')}: {e}")
                continue
        
        return {
            'status': 'SUCCESS',
            'records_synced': sync_count
        }
        
    except Exception as e:
        logging.error(f"同步專輯維度表失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def update_daily_stats(**context) -> Dict:
    """Task 5: 更新每日聚合統計"""
    try:
        config = load_config()
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        # 計算今日統計
        today = context['ds']
        
        upsert_daily_stats_query = """
        INSERT INTO dwh.agg_daily_stats 
        (date_value, total_tracks, unique_tracks, total_minutes, 
         unique_artists, unique_albums, top_artist, top_track)
        SELECT 
            d.date_value,
            COUNT(f.listening_minutes) as total_tracks,
            COUNT(DISTINCT f.track_key) as unique_tracks,
            SUM(f.listening_minutes) as total_minutes,
            COUNT(DISTINCT f.artist_key) as unique_artists,
            COUNT(DISTINCT f.album_key) as unique_albums,
            (SELECT a.artist_name 
             FROM dwh.fact_listening f2
             JOIN dwh.dim_artists a ON f2.artist_key = a.artist_key
             WHERE f2.date_key = d.date_key
             GROUP BY a.artist_name
             ORDER BY COUNT(*) DESC LIMIT 1) as top_artist,
            (SELECT t.track_name
             FROM dwh.fact_listening f3  
             JOIN dwh.dim_tracks t ON f3.track_key = t.track_key
             WHERE f3.date_key = d.date_key
             GROUP BY t.track_name
             ORDER BY COUNT(*) DESC LIMIT 1) as top_track
        FROM dwh.dim_dates d
        LEFT JOIN dwh.fact_listening f ON d.date_key = f.date_key
        WHERE d.date_value = %s
        GROUP BY d.date_value, d.date_key
        ON CONFLICT (date_value) DO UPDATE SET
            total_tracks = EXCLUDED.total_tracks,
            unique_tracks = EXCLUDED.unique_tracks,
            total_minutes = EXCLUDED.total_minutes,
            unique_artists = EXCLUDED.unique_artists,
            unique_albums = EXCLUDED.unique_albums,
            top_artist = EXCLUDED.top_artist,
            top_track = EXCLUDED.top_track,
            updated_at = CURRENT_TIMESTAMP
        """
        
        pg_conn.execute_query(upsert_daily_stats_query, (today,))
        
        logging.info(f"每日統計已更新: {today}")
        
        return {
            'status': 'SUCCESS',
            'date_processed': today
        }
        
    except Exception as e:
        logging.error(f"更新每日統計失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def log_etl_batch(**context) -> Dict:
    """記錄 ETL 執行結果"""
    try:
        config = load_config()
        pg_conn = PostgreSQLConnection(config['database']['postgresql'])
        
        # 從前面的 task 收集結果
        sync_result = context['task_instance'].xcom_pull(task_ids='sync_listening_to_raw_staging')
        
        status = 'SUCCESS' if sync_result.get('status') == 'SUCCESS' else 'FAILED'
        records_processed = sync_result.get('records_processed', 0)
        last_sync_timestamp = sync_result.get('last_sync_timestamp')
        
        # 記錄到 ETL 日誌表
        log_query = """
        INSERT INTO dwh.etl_batch_log 
        (job_name, batch_id, status, records_processed, 
         last_sync_timestamp, started_at, completed_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = (
            'daily_listening_sync',
            context['ds_nodash'] + '_listening_sync',
            status,
            records_processed,
            last_sync_timestamp,
            context['task_instance'].start_date,
            datetime.now(),
            sync_result.get('error') if status == 'FAILED' else None
        )
        
        pg_conn.execute_query(log_query, params)
        
        logging.info(f"ETL 批次記錄已儲存: {status}")
        
        return {
            'status': 'SUCCESS',
            'batch_logged': True
        }
        
    except Exception as e:
        logging.error(f"記錄 ETL 批次失敗: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

# ============================================================================
# DAG Tasks 定義
# ============================================================================

# Task 0: 取得同步起始點
get_sync_watermark = PythonOperator(
    task_id='get_sync_watermark',
    python_callable=get_last_sync_timestamp,
    dag=dag
)

# Task 1: 同步聽歌記錄
sync_listening_task = PythonOperator(
    task_id='sync_listening_to_raw_staging',
    python_callable=sync_listening_to_raw_staging,
    dag=dag
)

# Task 2: 時間處理
process_time_task = PythonOperator(
    task_id='process_time_fields',
    python_callable=process_time_fields,
    dag=dag
)

# Task 3: 載入 DWH
load_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

# Task 4: 維度表同步 (平行執行)
with TaskGroup("sync_dimensions", dag=dag) as sync_dimensions_group:
    sync_tracks_task = PythonOperator(
        task_id='sync_tracks_to_dwh',
        python_callable=sync_tracks_to_dwh
    )
    
    sync_artists_task = PythonOperator(
        task_id='sync_artists_to_dwh',
        python_callable=sync_artists_to_dwh
    )
    
    sync_albums_task = PythonOperator(
        task_id='sync_albums_to_dwh',
        python_callable=sync_albums_to_dwh
    )

# Task 5: 更新聚合統計
update_stats_task = PythonOperator(
    task_id='update_daily_stats',
    python_callable=update_daily_stats,
    dag=dag
)

# Task 6: 記錄 ETL 結果
log_batch_task = PythonOperator(
    task_id='log_etl_batch',
    python_callable=log_etl_batch,
    dag=dag,
    trigger_rule='none_failed_or_skipped'  # 即使有部分失敗也要記錄
)

# ============================================================================
# 任務依賴關係
# ============================================================================

# 線性依賴 + 平行處理
get_sync_watermark >> sync_listening_task >> process_time_task

# 平行處理維度表同步
process_time_task >> sync_dimensions_group
process_time_task >> load_warehouse_task

# 等維度表和事實表都完成後，更新統計
[sync_dimensions_group, load_warehouse_task] >> update_stats_task

# 最後記錄執行結果
update_stats_task >> log_batch_task

# ============================================================================
# DAG 執行摘要
# ============================================================================
"""
執行流程:
1. get_sync_watermark: 取得上次同步時間點 
2. sync_listening_to_raw_staging: MongoDB → Raw Staging
3. process_time_fields: Raw → Clean Staging (時間解析)
4. sync_dimensions (平行):
   - sync_tracks_to_dwh: 同步新歌曲到維度表
   - sync_artists_to_dwh: 同步新藝術家到維度表  
   - sync_albums_to_dwh: 同步新專輯到維度表
5. load_to_warehouse: Clean → DWH Fact Table
6. update_daily_stats: 計算每日聚合統計
7. log_etl_batch: 記錄執行結果

容錯機制:
- 每個 task 都有獨立的錯誤處理
- 部分失敗不會阻止其他 task 執行
- 使用 watermark 機制追蹤進度
- 所有結果都記錄到 etl_batch_log 表

監控指標:
- 處理的記錄數量
- 同步的維度資料數量
- 執行時間和狀態
- 錯誤訊息記錄

執行時間: 每日凌晨 2:00
預期處理時間: < 5 分鐘 (每日 < 100 筆資料)
重試機制: 失敗後 5 分鐘重試，最多重試 2 次
"""