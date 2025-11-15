import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def band_violin_load_data(days_to_display: int, _db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the band distribution visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH params AS (
                SELECT {days_to_display} AS day_count 
            )
            SELECT
                f.listening_minutes,
                a.band
            FROM
                dwh.fact_listening AS f
            LEFT JOIN
                dwh.dim_artists AS a
                ON f.artist_key = a.artist_key
            WHERE
                f.played_at >= (CURRENT_DATE - (SELECT day_count FROM params) * INTERVAL '1 day') 
                AND f.played_at < CURRENT_DATE
            """
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw

@st.cache_data(ttl=600) 
def band_bar_load_data(days_to_display: int, _db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the band distribution visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH params AS (
                SELECT {days_to_display} AS day_count 
            )
            SELECT
                count(f.listening_minutes),
                -- a.gender,
                a.band
            FROM
                dwh.fact_listening AS f
            LEFT JOIN
                dwh.dim_artists AS a
                ON f.artist_key = a.artist_key
            WHERE
                f.played_at >= (CURRENT_DATE - (SELECT day_count FROM params) * INTERVAL '1 day') 
                AND f.played_at < CURRENT_DATE
            GROUP by
                a.band
            """
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw

