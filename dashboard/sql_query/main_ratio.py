import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def hour_ratio_load_data(_db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the hour ratio visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH hourly_by_day AS (
                SELECT 
                    dd.day_name,
                    dd.day_of_week,
                    fl.hour_of_day,
                    COUNT(fl.listening_key) as total_plays,
                    SUM(fl.listening_minutes) as total_minutes
                FROM dwh.fact_listening fl
                JOIN dwh.dim_dates dd ON fl.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY dd.day_name, dd.day_of_week, fl.hour_of_day
            ),
            time_periods_by_day AS (
                SELECT 
                    day_name,
                    day_of_week,
                    hour_of_day,
                    total_plays,
                    total_minutes,
                    CASE 
                        WHEN hour_of_day BETWEEN 0 AND 5 THEN 'Midnight (00-05)'
                        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning (06-11)'
                        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
                        WHEN hour_of_day BETWEEN 18 AND 23 THEN 'Evening (18-23)'
                    END as time_period
                FROM hourly_by_day
            ),
            period_summary_by_day AS (
                SELECT 
                    day_name,
                    day_of_week,
                    time_period,
                    SUM(total_plays) as period_plays,
                    SUM(total_minutes) as period_minutes
                FROM time_periods_by_day
                GROUP BY day_name, day_of_week, time_period
            ),
            day_totals AS (
                SELECT 
                    day_name,
                    day_of_week,
                    SUM(period_minutes) as day_total_minutes
                FROM period_summary_by_day
                GROUP BY day_name, day_of_week
            )
            SELECT 
                psd.day_name,
                psd.day_of_week,
                psd.time_period,
                psd.period_minutes,
                dt.day_total_minutes,
                ROUND(
                    (psd.period_minutes::numeric / dt.day_total_minutes) * 100, 1
                ) as minutes_percentage,
                -- 為排序添加時段順序
                CASE psd.time_period
                    WHEN 'Midnight (00-05)' THEN 1
                    WHEN 'Morning (06-11)' THEN 2
                    WHEN 'Afternoon (12-17)' THEN 3
                    WHEN 'Evening (18-23)' THEN 4
                END as period_order
            FROM period_summary_by_day psd
            JOIN day_totals dt ON psd.day_name = dt.day_name
            ORDER BY 
                psd.day_of_week, 
                period_order;     
            """
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw

@st.cache_data(ttl=600) 
def radar_load_data(_db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the radar visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH behavior_analysis AS (
                SELECT 
                    CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                    
                    -- basic listening metrics
                    COUNT(fl.listening_key) as total_sessions,
                    SUM(fl.listening_minutes) as total_time,
                    AVG(fl.listening_minutes) as avg_session_length,
                    
                    -- Variety metrics 
                    COUNT(DISTINCT fl.track_key) as unique_tracks,
                    COUNT(DISTINCT fl.artist_key) as unique_artists,
                    COUNT(DISTINCT fl.album_key) as unique_albums,
                    
                    -- Repeat consumption
                    COUNT(fl.listening_key) / COUNT(DISTINCT fl.track_key) as repeat_ratio,
                    
                    -- Content popularity
                    AVG(dt.popularity) as avg_track_popularity,
                    STDDEV(dt.popularity) as track_popularity_variance,
                    
                    -- Explicit content
                    COUNT(CASE WHEN dt.explicit = true THEN 1 END) as explicit_count,
                    
                    -- Daytime listening
                    AVG(CASE WHEN fl.hour_of_day BETWEEN 6 AND 18 THEN 1 ELSE 0 END) * 100 as daytime_percentage,
                    
                    -- Active days
                    COUNT(DISTINCT dd.date_value) as active_days
                    
                FROM dwh.fact_listening fl
                JOIN dwh.dim_dates dd ON fl.date_key = dd.date_key
                JOIN dwh.dim_tracks dt ON fl.track_key = dt.track_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '60 days'
                    AND dd.is_holiday = false
                GROUP BY day_type
            )
            SELECT 
                day_type,
                total_sessions,
                total_time,
                ROUND(avg_session_length, 2) as avg_session_minutes,
                unique_tracks,
                unique_artists, 
                unique_albums,
                ROUND(repeat_ratio, 2) as track_repeat_ratio,
                ROUND(avg_track_popularity, 1) as avg_track_popularity,
                ROUND(track_popularity_variance, 1) as popularity_variance,
                explicit_count,
                ROUND(explicit_count::numeric / total_sessions * 100, 1) as explicit_percentage,
                ROUND(daytime_percentage, 1) as daytime_listening_percentage,
                active_days,
                ROUND(total_sessions::numeric / active_days, 1) as avg_sessions_per_day
            FROM behavior_analysis
            ORDER BY day_type;
            """
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw
