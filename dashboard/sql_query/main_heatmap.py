import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def heatmap_load_data(day_count: int, _db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the heatmap visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH params AS (
                SELECT {day_count} AS day_count 
            ),
            date_hour_series AS (
                SELECT
                    generate_series(
                        (CURRENT_DATE - (p.day_count || ' days')::interval)::timestamp,
                        (CURRENT_DATE - '1 day'::interval + '23 hours 59 minutes 59 seconds'::interval)::timestamp,
                        '1 hour'::interval
                    ) AS played_time_series
                FROM params AS p
            )
            SELECT
                DATE(s.played_time_series) AS played_date,
                EXTRACT(HOUR FROM s.played_time_series) AS played_hour,
                COALESCE(SUM(f.listening_minutes), 0.0) AS total_listening_minutes
            FROM
                date_hour_series AS s
            LEFT JOIN
                dwh.fact_listening AS f
                ON date_trunc('hour', f.played_at) = s.played_time_series
            GROUP BY
                played_date,
                played_hour
            ORDER BY
                played_date,
                played_hour;
            """
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    # Renaming columns for clarity
    df_raw.rename(columns={
        'played_date': 'Date',
        'played_hour': 'Hour',
        'total_listening_minutes': 'Intensity'
    }, inplace=True)
    
    # Convert data types
    df_raw['Date'] = pd.to_datetime(df_raw['Date']).dt.strftime('%Y-%m-%d') 
    df_raw['Hour'] = df_raw['Hour'].astype(int)
    df_raw['Intensity'] = pd.to_numeric(df_raw['Intensity'], errors='coerce').fillna(0)

    return df_raw
