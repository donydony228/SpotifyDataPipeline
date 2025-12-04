import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

@st.cache_data(ttl=600) 
def bar_quality_load_data(day_count: int, _db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the bar chart visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH params AS (
                SELECT {day_count} AS day_count 
            ),
            date_series AS (
                SELECT
                    generate_series(
                        (CURRENT_DATE - (p.day_count || ' days')::interval),
                        (CURRENT_DATE - '1 day'::interval),
                        '1 day'::interval
                    )::date AS played_date -- 這裡直接轉成 date 格式比較乾淨
                FROM params AS p
            )
            SELECT
                s.played_date,
                COALESCE(avg(f.success_rate_percent), 0.0) AS avg_success_rate
            FROM
                date_series AS s
            LEFT JOIN
                dwh.v_data_lineage_full as f
                ON DATE(f.created_at) = s.played_date -- 修改 JOIN 條件，只比對日期
            GROUP BY
                s.played_date
            ORDER BY
                s.played_date;"""
    
    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")
    
    return df_raw

@st.cache_data(ttl=600)
def bar_number_load_data(day_count: int, _db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the bar chart visualization.
    """
    
    # SQL query with dynamic day_count parameter
    query = f"""
            WITH params AS (
                SELECT {day_count} AS day_count 
            ),
            date_series AS (
                SELECT
                    generate_series(
                        (CURRENT_DATE - (p.day_count || ' days')::interval),
                        (CURRENT_DATE - '1 day'::interval),
                        '1 day'::interval
                    )::date AS played_date -- 這裡直接轉成 date 格式比較乾淨
                FROM params AS p
            )
            SELECT
                s.played_date,
                COALESCE(sum(f.records_successful), 0.0) AS successful_records
            FROM
                date_series AS s
            LEFT JOIN
                dwh.v_data_lineage_full as f
                ON DATE(f.created_at) = s.played_date -- 修改 JOIN 條件，只比對日期
            GROUP BY
                s.played_date
            ORDER BY
                s.played_date;"""

    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw

@st.cache_data(ttl=600)
def networks_load_data(_db_manager: SupabaseManager) -> pd.DataFrame:
    """
    Initialize and load data for the network graph visualization.
    """
    
    query = """
            SELECT 
                source_node,
                target_node,
                edge_type,
                relationship_count,
                last_execution,
                avg_records_processed
            FROM dwh.v_lineage_graph
            ORDER BY last_execution DESC;
            """

    df_raw = pd.DataFrame() 
    try:
        # Query execution
        df_raw = _db_manager.execute_query(query)
    except Exception as e:
        st.error(f"ERROR: Database query execution failed: {e}")

    return df_raw
