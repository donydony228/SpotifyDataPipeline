import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def track_sankey_load_data(_db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the track sankey visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = """WITH track_conversion_analysis AS (
        SELECT 
            dt.track_key,
            dt.track_name,
            dt.first_heard,
            
            COUNT(fl.listening_key) as actual_total_plays,
            
            COUNT(CASE WHEN fl.played_at::date = dt.first_heard THEN 1 END) as first_day_plays,
            COUNT(CASE WHEN fl.played_at::date BETWEEN dt.first_heard AND dt.first_heard + INTERVAL '7 days' THEN 1 END) as first_week_plays,
            COUNT(CASE WHEN fl.played_at::date BETWEEN dt.first_heard AND dt.first_heard + INTERVAL '30 days' THEN 1 END) as first_month_plays,
            
            MIN(fl.played_at::date) as actual_first_play,
            MAX(fl.played_at::date) as last_play
            
        FROM dwh.dim_tracks dt
        JOIN dwh.fact_listening fl ON dt.track_key = fl.track_key
        WHERE dt.first_heard IS NOT NULL
            AND dt.first_heard >= CURRENT_DATE - INTERVAL '6 months'
            AND fl.played_at IS NOT NULL
        GROUP BY dt.track_key, dt.track_name, dt.first_heard
        HAVING COUNT(fl.listening_key) > 0  
    )
    SELECT 
        track_name,
        first_heard,
        actual_total_plays,
        first_day_plays,
        first_week_plays,
        first_month_plays,
        
        CASE 
            WHEN actual_total_plays = 1 THEN 'One Hit Wonder'
            WHEN actual_total_plays BETWEEN 2 AND 5 THEN 'Casual Listen'
            WHEN actual_total_plays BETWEEN 6 AND 15 THEN 'Regular Play'
            WHEN actual_total_plays > 15 THEN 'Heavy Rotation'
        END as conversion_category,
        
        CASE 
            WHEN last_play - first_heard <= 3 THEN 'Quick Drop'
            WHEN last_play - first_heard <= 7 THEN 'Short Term'
            WHEN last_play - first_heard <= 14 THEN 'Medium Term'
            ELSE 'Long Term'
        END as lifecycle_stage
        
    FROM track_conversion_analysis
    ORDER BY actual_total_plays DESC;"""

    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw

