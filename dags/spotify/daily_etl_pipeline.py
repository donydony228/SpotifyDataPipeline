# Spotify ETL Pipeline
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from pymongo import MongoClient

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
import traceback

# Get the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from data_quality.dwh_fact_validator import validate_dwh_fact_listening_data

# ============================================================================
# DAG Settings
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
    start_date=datetime(2025, 10, 20),
    description='MongoDB to PostgreSQL Daily ETL Pipeline for Spotify Listening Data',
    schedule_interval='0 14 * * *',  # Daily at 14:00 UTC
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'spotify', 'daily', 'fixed']
)

# ============================================================================
# Supporting Functions
# ============================================================================

def get_last_sync_timestamp(**context) -> str:
    """Get the last sync timestamp for incremental sync"""
    try:
        config = load_config()
        pg_conn = PostgreSQLConnection()
        
        if not pg_conn.connect():
            raise Exception("PostgreSQL connection failed")

        # Get the last successful sync time from the ETL log table
        query = """
        SELECT batch_date
        FROM dwh.etl_batch_log 
        WHERE process_name = 'daily_listening_sync' 
        AND status = 'success'
        ORDER BY batch_date DESC 
        LIMIT 1
        """
        
        result = pg_conn.execute_query(query)
        pg_conn.close()
        
        if result and len(result) > 0:
            last_sync = result[0]['batch_date']  
            return last_sync.isoformat()
        else:
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            return yesterday.isoformat()
            
    except Exception as e:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.isoformat()


def get_db_connections():
    """Create and return database connections"""
    try:
        config = load_config()

        # MongoDB connection
        mongo_conn = MongoDBConnection()
        if not mongo_conn.connect():
            raise Exception("MongoDB connection failed")

        # PostgreSQL connection
        pg_conn = PostgreSQLConnection()
        if not pg_conn.connect():
            raise Exception("PostgreSQL connection failed")
        return pg_conn, mongo_conn
        
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise e

# ============================================================================
# Core ETL Functions (Fixed Version)
# ============================================================================

def sync_listening_to_raw_staging(**context):
    """Sync listening data from MongoDB to Raw Staging"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()
        
        # Get last sync timestamp from XCom
        watermark_result = context['task_instance'].xcom_pull(task_ids='get_sync_watermark')
        if watermark_result:
            last_sync_time = datetime.fromisoformat(watermark_result.replace('Z', '+00:00'))
        else:
            # Query directly from the database
            cursor.execute("""
                SELECT COALESCE(MAX(synced_at), '1970-01-01'::timestamp) 
                FROM raw_staging.spotify_listening_raw
            """)
            last_sync_time = cursor.fetchone()[0]

        logging.info(f"Last sync time: {last_sync_time}")

        # Query new records from MongoDB (using the corrected timestamp field)
        music_db = mongo_conn.client['music_data']
        collection = music_db['daily_listening_history']

        # Use the correct timestamp field
        query = {'batch_info.collected_at': {'$gt': last_sync_time}}
        new_records = list(collection.find(query))

        logging.info(f"Found {len(new_records)} new records")

        if not new_records:
            return {
                'status': 'SUCCESS', 
                'records_processed': 0,
                'message': 'No new records to sync'
            }

        # Batch insert into PostgreSQL (Fixed Version)
        insert_sql = """
        INSERT INTO raw_staging.spotify_listening_raw (
            track_id, played_at, track_name, artist_name, album_name,
            duration_ms, explicit, popularity, batch_id
        ) VALUES %s
        ON CONFLICT (track_id, played_at) DO NOTHING
        """
        
        records_data = []
        for record in new_records:
            try:
                # Parse nested structures
                track_info = record.get('track_info', {})
                batch_info = record.get('batch_info', {})

                # Process artist name (first artist)
                artists = track_info.get('artists', [])
                artist_name = artists[0].get('name') if artists else 'Unknown Artist'

                # Process album name
                album = track_info.get('album', {})
                album_name = album.get('name', 'Unknown Album')
                
                record_tuple = (
                    record.get('track_id'),
                    record.get('played_at'),
                    track_info.get('name'),
                    artist_name,
                    album_name,
                    track_info.get('duration_ms'),
                    track_info.get('explicit', False),
                    track_info.get('popularity', 0),
                    batch_info.get('batch_id')
                )
                records_data.append(record_tuple)
                
            except Exception as e:
                logging.warning(f"Record parsing failed: {e}")
                continue

        # Batch insert using execute_values for efficiency
        from psycopg2.extras import execute_values
        execute_values(cursor, insert_sql, records_data, page_size=100)
        
        inserted_count = cursor.rowcount
        pg_conn.connection.commit()

        # Update stats to XCom
        stats = {
            'records_found': len(new_records),
            'records_inserted': inserted_count,
            'last_sync_time': str(last_sync_time)
        }
        context['task_instance'].xcom_push(key='sync_stats', value=stats)

        # Update last sync timestamp
        if new_records:
            latest_time = max(r.get('batch_info', {}).get('collected_at') for r in new_records)
            logging.info(f"Sync completed: {inserted_count}/{len(new_records)} records inserted")
            return {
                'status': 'SUCCESS',
                'records_processed': inserted_count,
                'last_sync_timestamp': str(latest_time)
            }
        
    except Exception as e:
        logging.error(f"Sync failed: {e}")
        logging.error(traceback.format_exc())
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        return {'status': 'FAILED', 'error': str(e)}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

def process_time_fields(**context):
    """Process time fields and load into Clean Staging"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()

        # Process from raw_staging to clean_staging
        insert_sql = """
        INSERT INTO clean_staging.listening_cleaned (
            raw_id, track_id, played_at, played_date, played_hour, 
            played_day_of_week, time_period, is_weekend,
            track_name, artist_name, album_name, duration_minutes,
            data_quality_score, quality_flags
        )
        SELECT 
            r.id as raw_id,
            r.track_id,
            r.played_at,
            r.played_at::date as played_date,
            EXTRACT(hour FROM r.played_at) as played_hour,
            EXTRACT(dow FROM r.played_at) as played_day_of_week,
            CASE 
                WHEN EXTRACT(hour FROM r.played_at) BETWEEN 6 AND 11 THEN 'morning'
                WHEN EXTRACT(hour FROM r.played_at) BETWEEN 12 AND 17 THEN 'afternoon'
                WHEN EXTRACT(hour FROM r.played_at) BETWEEN 18 AND 23 THEN 'evening'
                ELSE 'night'
            END as time_period,
            EXTRACT(dow FROM r.played_at) IN (0, 6) as is_weekend,
            r.track_name,
            r.artist_name,
            r.album_name,
            ROUND(r.duration_ms / 60000.0, 2) as duration_minutes,
            CASE 
                WHEN r.track_name IS NULL OR r.artist_name IS NULL THEN 0.5
                WHEN r.duration_ms IS NULL OR r.duration_ms < 10000 THEN 0.7
                ELSE 1.0
            END as data_quality_score,
            CASE 
                WHEN r.track_name IS NULL THEN ARRAY['missing_track_name']
                WHEN r.artist_name IS NULL THEN ARRAY['missing_artist_name']
                WHEN r.duration_ms IS NULL THEN ARRAY['missing_duration']
                WHEN r.duration_ms < 10000 THEN ARRAY['short_duration']
                ELSE ARRAY[]::TEXT[]
            END as quality_flags
        FROM raw_staging.spotify_listening_raw r
        WHERE NOT EXISTS (
            SELECT 1 FROM clean_staging.listening_cleaned c
            WHERE c.track_id = r.track_id AND c.played_at = r.played_at
        )
        """
        
        cursor.execute(insert_sql)
        inserted_count = cursor.rowcount
        pg_conn.connection.commit()
        
        logging.info(f"Time field processing completed: {inserted_count} records inserted")
        return {'status': 'SUCCESS', 'records_processed': inserted_count}
        
    except Exception as e:
        logging.error(f"Time field processing failed: {e}")
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        return {'status': 'FAILED', 'error': str(e)}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

# ============================================================================
# Dimension Table Sync Functions
# ============================================================================

def sync_tracks_to_dwh(**context):
    """Sync tracks to dimension table"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()

        # Get unique tracks from clean_staging
        insert_sql = """
        INSERT INTO dwh.dim_tracks (track_id, track_name, duration_minutes, explicit, popularity, first_heard)
        SELECT DISTINCT 
            track_id,
            track_name,
            duration_minutes,
            FALSE as explicit, 
            0 as popularity, 
            MIN(played_date) as first_heard
        FROM clean_staging.listening_cleaned
        WHERE track_id NOT IN (
            SELECT track_id FROM dwh.dim_tracks
        )
        GROUP BY track_id, track_name, duration_minutes
        ON CONFLICT (track_id) DO UPDATE SET
            total_plays = dwh.dim_tracks.total_plays + 1,
            last_updated = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_sql)
        affected_count = cursor.rowcount
        pg_conn.connection.commit()

        logging.info(f"Sync completed: {affected_count} tracks inserted into dimension table")
        return {'status': 'SUCCESS', 'tracks_synced': affected_count}
        
    except Exception as e:
        logging.error(f"Sync failed: {e}")
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        return {'status': 'FAILED', 'error': str(e)}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

def sync_artists_to_dwh(**context):
    """Sync artists to dimension table"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()

        # Get unique artists from clean_staging
        insert_sql = """
        INSERT INTO dwh.dim_artists (artist_id, artist_name, first_discovered)
        SELECT DISTINCT 
            'artist_' || MD5(artist_name) as artist_id,
            artist_name,
            MIN(played_date) as first_discovered
        FROM clean_staging.listening_cleaned
        WHERE artist_name IS NOT NULL 
        AND artist_name NOT IN (
            SELECT artist_name FROM dwh.dim_artists
        )
        GROUP BY artist_name
        ON CONFLICT (artist_id) DO UPDATE SET
            total_plays = dwh.dim_artists.total_plays + 1,
            last_updated = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_sql)
        affected_count = cursor.rowcount
        pg_conn.connection.commit()

        logging.info(f"Sync completed: {affected_count} artists inserted into dimension table")
        return {'status': 'SUCCESS', 'artists_synced': affected_count}
        
    except Exception as e:
        logging.error(f"Sync failed: {e}")
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        return {'status': 'FAILED', 'error': str(e)}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

def sync_albums_to_dwh(**context):
    """Sync albums to dimension table"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()

        # Get unique albums from clean_staging
        insert_sql = """
        INSERT INTO dwh.dim_albums (album_id, album_name, first_heard)
        SELECT DISTINCT 
            'album_' || MD5(album_name) as album_id,
            album_name,
            MIN(played_date) as first_heard
        FROM clean_staging.listening_cleaned
        WHERE album_name IS NOT NULL 
        AND album_name NOT IN (
            SELECT album_name FROM dwh.dim_albums
        )
        GROUP BY album_name
        ON CONFLICT (album_id) DO UPDATE SET
            total_plays = dwh.dim_albums.total_plays + 1,
            last_updated = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_sql)
        affected_count = cursor.rowcount
        pg_conn.connection.commit()

        logging.info(f"Sync completed: {affected_count} albums inserted into dimension table")
        return {'status': 'SUCCESS', 'albums_synced': affected_count}
        
    except Exception as e:
        logging.error(f"Sync failed: {e}")
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        return {'status': 'FAILED', 'error': str(e)}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

# ============================================================================
# Load to Warehouse Function
# ============================================================================

def load_to_warehouse(**context):
    """Load to fact table - fixed version"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()
        
        insert_sql = """
        INSERT INTO dwh.fact_listening (
            date_key, track_key, artist_key, album_key,
            play_count, listening_minutes, hour_of_day, is_weekend, played_at
        )
        SELECT 
            d.date_key,
            t.track_key,
            a.artist_key,
            alb.album_key,
            1 as play_count,
            c.duration_minutes as listening_minutes,
            c.played_hour as hour_of_day,
            c.is_weekend,
            c.played_at
        FROM clean_staging.listening_cleaned c
        JOIN dwh.dim_dates d ON d.date_value = c.played_date
        JOIN dwh.dim_tracks t ON t.track_id = c.track_id
        LEFT JOIN dwh.dim_artists a ON a.artist_name = c.artist_name
        LEFT JOIN dwh.dim_albums alb ON alb.album_name = c.album_name
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.fact_listening f
            WHERE f.track_key = t.track_key 
            AND f.played_at = c.played_at
        )
        """
        
        logging.info("Conducting fact table load...")
        cursor.execute(insert_sql)
        inserted_count = cursor.rowcount
        pg_conn.connection.commit()

        logging.info(f"Sync completed: {inserted_count} records inserted into fact table")

        try:
            cursor.execute("SELECT COUNT(*) FROM dwh.fact_listening")
            result = cursor.fetchone()
            total_count = result[0] if result else 0
        except Exception as count_error:
            logging.warning(f"Count query failed: {count_error}")
            total_count = 0
        
        return {
            'status': 'SUCCESS',
            'records_inserted': inserted_count,
            'total_records': total_count
        }
        
    except Exception as e:
        error_msg = f"Fact table load failed: {e}"
        logging.error(f"{error_msg}")
        
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        
        return {'status': 'FAILED', 'error': error_msg}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

# ============================================================================
# Aggregation and Logging Functions
# ============================================================================

def update_daily_stats(**context):
    """Update daily aggregation statistics"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()
        
        today = datetime.now().date()
        
        # Calculate daily stats and upsert
        upsert_sql = """
        INSERT INTO dwh.agg_daily_stats (
            stats_date, total_tracks, unique_tracks, total_listening_minutes,
            unique_artists, unique_albums, morning_tracks, afternoon_tracks,
            evening_tracks, night_tracks, top_artist, top_track, top_album
        )
        SELECT 
            %s as stats_date,
            COUNT(*) as total_tracks,
            COUNT(DISTINCT f.track_key) as unique_tracks,
            SUM(f.listening_minutes) as total_listening_minutes,
            COUNT(DISTINCT f.artist_key) as unique_artists,
            COUNT(DISTINCT f.album_key) as unique_albums,
            COUNT(CASE WHEN f.hour_of_day BETWEEN 6 AND 11 THEN 1 END) as morning_tracks,
            COUNT(CASE WHEN f.hour_of_day BETWEEN 12 AND 17 THEN 1 END) as afternoon_tracks,
            COUNT(CASE WHEN f.hour_of_day BETWEEN 18 AND 23 THEN 1 END) as evening_tracks,
            COUNT(CASE WHEN f.hour_of_day BETWEEN 0 AND 5 THEN 1 END) as night_tracks,
            (SELECT a.artist_name FROM dwh.fact_listening f2 
             JOIN dwh.dim_artists a ON f2.artist_key = a.artist_key
             JOIN dwh.dim_dates d2 ON f2.date_key = d2.date_key
             WHERE d2.date_value = %s
             GROUP BY a.artist_name ORDER BY COUNT(*) DESC LIMIT 1) as top_artist,
            (SELECT t.track_name FROM dwh.fact_listening f2 
             JOIN dwh.dim_tracks t ON f2.track_key = t.track_key
             JOIN dwh.dim_dates d2 ON f2.date_key = d2.date_key
             WHERE d2.date_value = %s
             GROUP BY t.track_name ORDER BY COUNT(*) DESC LIMIT 1) as top_track,
            (SELECT alb.album_name FROM dwh.fact_listening f2 
             JOIN dwh.dim_albums alb ON f2.album_key = alb.album_key
             JOIN dwh.dim_dates d2 ON f2.date_key = d2.date_key
             WHERE d2.date_value = %s
             GROUP BY alb.album_name ORDER BY COUNT(*) DESC LIMIT 1) as top_album
        FROM dwh.fact_listening f
        JOIN dwh.dim_dates d ON f.date_key = d.date_key
        WHERE d.date_value = %s
        ON CONFLICT (stats_date) 
        DO UPDATE SET
            total_tracks = EXCLUDED.total_tracks,
            unique_tracks = EXCLUDED.unique_tracks,
            total_listening_minutes = EXCLUDED.total_listening_minutes,
            unique_artists = EXCLUDED.unique_artists,
            unique_albums = EXCLUDED.unique_albums,
            morning_tracks = EXCLUDED.morning_tracks,
            afternoon_tracks = EXCLUDED.afternoon_tracks,
            evening_tracks = EXCLUDED.evening_tracks,
            night_tracks = EXCLUDED.night_tracks,
            top_artist = EXCLUDED.top_artist,
            top_track = EXCLUDED.top_track,
            top_album = EXCLUDED.top_album,
            last_updated = CURRENT_TIMESTAMP
        """
        
        cursor.execute(upsert_sql, (today, today, today, today, today))
        pg_conn.connection.commit()

        logging.info(f"Daily stats updated successfully: {today}")
        return {'status': 'SUCCESS', 'date_processed': str(today)}
        
    except Exception as e:
        logging.error(f"Daily stats update failed: {e}")
        if 'pg_conn' in locals():
            pg_conn.connection.rollback()
        return {'status': 'FAILED', 'error': str(e)}
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

def log_etl_batch(**context):
    """Log ETL execution results to etl_batch_log"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        cursor = pg_conn.connection.cursor()

        # Collect results from previous tasks
        sync_result = context['task_instance'].xcom_pull(task_ids='sync_listening_to_raw_staging')
        time_result = context['task_instance'].xcom_pull(task_ids='process_time_fields')
        warehouse_result = context['task_instance'].xcom_pull(task_ids='load_to_warehouse')
        
        # Determine overall status
        all_results = [sync_result, time_result, warehouse_result]
        failed_tasks = [r for r in all_results if r and r.get('status') == 'FAILED']
        overall_status = 'failed' if failed_tasks else 'success' 
        
        total_processed = (sync_result.get('records_processed', 0) if sync_result else 0)

        # Provide all necessary fields for logging
        execution_date = context.get('ds')  # YYYY-MM-DD 
        batch_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
        
        # Insert log record
        log_query = """
        INSERT INTO dwh.etl_batch_log 
        (batch_id, batch_date, process_name, status, records_processed, 
         started_at, completed_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        error_msg = None
        if failed_tasks:
            error_msg = '; '.join([f"{r.get('error', 'Unknown error')}" for r in failed_tasks])
        
        params = (
            context['ds_nodash'] + '_listening_sync',  # batch_id
            batch_date,                                # batch_date
            'daily_listening_sync',                    # process_name
            overall_status,                            # status
            total_processed,                           # records_processed
            context['task_instance'].start_date,       # started_at
            datetime.now(),                            # completed_at
            error_msg                                  # error_message
        )
        
        cursor.execute(log_query, params)
        pg_conn.connection.commit()

        logging.info(f"ETL batch logged: {overall_status}")

        return {
            'status': 'SUCCESS',
            'batch_logged': True,
            'overall_status': overall_status
        }
        
    except Exception as e:
        logging.error(f"ETL batch logging failed: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

def generate_etl_summary(**context):
    """Generate detailed ETL execution summary report"""
    try:
        from datetime import timezone
        
        execution_start = context['task_instance'].start_date
        current_time = datetime.now(timezone.utc)
        
        # Make sure execution_start is timezone-aware
        if execution_start.tzinfo is None:
            execution_start = execution_start.replace(tzinfo=timezone.utc)
        
        execution_duration = current_time - execution_start
        
        # Collect results from previous tasks
        sync_result = context['task_instance'].xcom_pull(task_ids='sync_listening_to_raw_staging')
        time_result = context['task_instance'].xcom_pull(task_ids='process_time_fields')
        warehouse_result = context['task_instance'].xcom_pull(task_ids='load_to_warehouse')
        
        # Generate summary
        summary = {
            'execution_date': context['ds'],
            'total_execution_time': str(execution_duration),
            'sync_records': sync_result.get('records_processed', 0) if sync_result else 0,
            'time_records': time_result.get('records_processed', 0) if time_result else 0,
            'warehouse_records': warehouse_result.get('records_inserted', 0) if warehouse_result else 0,
            'overall_status': 'SUCCESS'
        }

        # Format output
        report_lines = [
            "Daily ETL Pipeline Execution Summary",
            "=" * 50,
            f"Execution Date: {summary['execution_date']}",
            f"⏱Total Execution Time: {summary['total_execution_time']}",
            f"Sync Records: {summary['sync_records']}",
            f"Time Records: {summary['time_records']} ",
            f"Warehouse Records: {summary['warehouse_records']} ",
            "ETL Pipeline Completed!"
        ]
        
        final_report = "\n".join(report_lines)
        logging.info(f"\n{final_report}")
        
        return {
            'status': 'SUCCESS',
            'summary': summary,
            'report': final_report
        }
        
    except Exception as e:
        logging.error(f"Failed to generate ETL summary: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def check_dwh_fact_data_quality(**context):
    """Check data quality of dwh.fact_listening"""
    try:
        pg_conn, mongo_conn = get_db_connections()
        
        query = "SELECT * FROM dwh.fact_listening LIMIT 200;"
        df = pg_conn.execute_query(query)
        
        # Transform to DataFrame
        df = pd.DataFrame(df)

        validation_results = validate_dwh_fact_listening_data(df)
        logging.info(f"Data Quality Validation Results: {validation_results}")
        
        return {
            'status': 'SUCCESS',
            'validation_results': validation_results
        }
        
    except Exception as e:
        logging.error(f"Data quality check failed: {e}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }
    
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'mongo_conn' in locals():
            mongo_conn.close()

# ============================================================================
# DAG Tasks Definition
# ============================================================================

# Task 0: Get last sync watermark
get_sync_watermark = PythonOperator(
    task_id='get_sync_watermark',
    python_callable=get_last_sync_timestamp,
    dag=dag
)

# Task 1: Sync listening records
sync_listening_task = PythonOperator(
    task_id='sync_listening_to_raw_staging',
    python_callable=sync_listening_to_raw_staging,
    retries=3, 
    dag=dag,
    owner='data-team'
)

# Task 2: Process time fields
process_time_task = PythonOperator(
    task_id='process_time_fields',
    python_callable=process_time_fields,
    retries=2,
    dag=dag,
    owner='data-team'
)

# Task 3: Load fact table
load_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    retries=2,
    dag=dag,
    owner='data-team'
)

# Task 4: Sync dimension tables (parallel execution) - using TaskGroup
with TaskGroup("sync_dimensions", dag=dag) as sync_dimensions_group:
    
    sync_tracks_task = PythonOperator(
        task_id='sync_tracks_to_dwh',
        python_callable=sync_tracks_to_dwh,
        retries=1,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  
    )
    
    sync_artists_task = PythonOperator(
        task_id='sync_artists_to_dwh',
        python_callable=sync_artists_to_dwh,
        retries=1,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    sync_albums_task = PythonOperator(
        task_id='sync_albums_to_dwh',
        python_callable=sync_albums_to_dwh,
        retries=1,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

# Task 5: Update daily stats
update_stats_task = PythonOperator(
    task_id='update_daily_stats',
    python_callable=update_daily_stats,
    retries=1,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  
    dag=dag,
    owner='data-team'
)

# Task 6: Log ETL results
log_batch_task = PythonOperator(
    task_id='log_etl_batch',
    python_callable=log_etl_batch,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,  
    owner='data-team'
)

# Task 7: Generate execution summary
summary_task = PythonOperator(
    task_id='generate_etl_summary',
    python_callable=generate_etl_summary,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,    
    owner='data-team'
)

# Task 8: Data Quality Check
data_quality_task = PythonOperator(
    task_id='check_dwh_fact_data_quality',
    python_callable=check_dwh_fact_data_quality,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,  
    owner='data-team'
)

# ============================================================================
# Task Dependencies
# ============================================================================

# Main linear flow
get_sync_watermark >> sync_listening_task >> process_time_task

# Split from process_time_task to load and dimensions
process_time_task >> [load_warehouse_task, sync_dimensions_group]

# Merge to update stats
[load_warehouse_task, sync_dimensions_group] >> update_stats_task

# Final logging and reporting
update_stats_task >> log_batch_task >> summary_task >> data_quality_task

# Dimension tables can run in parallel
[sync_tracks_task, sync_artists_task, sync_albums_task]

# ============================================================================
# DAG Completion Message
# ============================================================================

if __name__ == "__main__":
    print("Spotify ETL Pipeline - Loaded Successfully")
