import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def treemap_artist_load_data(day_count: int, _db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the treemap visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH params AS (
                SELECT {day_count} AS day_count 
                ),
                date_range AS (
                    SELECT
                        (CURRENT_DATE - (p.day_count || ' days')::interval) AS start_date
                    FROM params AS p
                )
                SELECT
                    t.artist_name,
                    COUNT(t.artist_name) AS play_count
                FROM
                    dwh.fact_listening AS f 
                INNER JOIN 
                    dwh.dim_artists AS t 
                    ON f.artist_key = t.artist_key
                CROSS JOIN 
                    date_range AS dr
                WHERE 
                    f.played_at >= dr.start_date
                GROUP BY 
                    t.artist_name
                ORDER BY 
                    play_count DESC 
                LIMIT 10;
            """
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw