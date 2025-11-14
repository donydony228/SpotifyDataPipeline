import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def basic_loyal_load_data(_db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the basic loyalty visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = """
WITH artist_loyalty AS (
    SELECT 
        da.artist_name,
        da.artist_id,
        -- 播放統計
        COUNT(fl.listening_key) as total_plays,
        COUNT(DISTINCT fl.date_key) as active_days,
        -- 時間跨度
        MIN(dd.date_value) as first_listen_date,
        MAX(dd.date_value) as last_listen_date,
        MAX(dd.date_value) - MIN(dd.date_value) + 1 as total_span_days,
        -- 忠誠度計算
        ROUND(
            COUNT(DISTINCT fl.date_key)::numeric / 
            (MAX(dd.date_value) - MIN(dd.date_value) + 1), 3
        ) as loyalty_ratio,
        -- 平均播放
        ROUND(COUNT(fl.listening_key)::numeric / COUNT(DISTINCT fl.date_key), 2) as avg_plays_per_day
    FROM dwh.fact_listening fl
    JOIN dwh.dim_artists da ON fl.artist_key = da.artist_key
    JOIN dwh.dim_dates dd ON fl.date_key = dd.date_key
    WHERE dd.date_value >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY da.artist_name, da.artist_id
    HAVING COUNT(fl.listening_key) >= 3
)
SELECT 
    artist_name,
    total_plays,
    active_days,
    total_span_days,
    loyalty_ratio,
    avg_plays_per_day,
    CASE 
        WHEN loyalty_ratio >= 0.5 THEN 'High Loyalty'
        WHEN loyalty_ratio >= 0.3 THEN 'Medium Loyalty'
        ELSE 'Low Loyalty'
    END as loyalty_level
FROM artist_loyalty
WHERE total_span_days > 3
ORDER BY loyalty_ratio DESC, total_plays DESC
LIMIT 100;
"""
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw
