import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

st.set_page_config(layout="wide")
st.title("Spotify Recap Dashboard")

# Check and initialize the database manager
try:
    db = SupabaseManager()
except NameError:
    st.error("ERROR: SupabaseManager is not defined. Please check the database_manager module.")
    st.stop()
except Exception as e:
    st.error(f"ERROR: Database initialization failed: {e}")
    st.stop()


# ----------------------------------------------------
# Heatmap
# ----------------------------------------------------
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

# Interactive slider for days back
days_to_display = st.slider(
    'Time Range (Days Back):', 
    min_value=7, 
    max_value=365, 
    value=30, 
    step=7
)

# Load data
df_source = heatmap_load_data(days_to_display, db)

if df_source.empty:
    st.stop()

# Create heatmap
st.subheader("Heatmap of Listening Time")

try:
    # Use Plotly for heatmap visualization
    import plotly.express as px
    import plotly.graph_objects as go
    
    pivot_data = df_source.pivot(index='Hour', columns='Date', values='Intensity')
    
    fig = px.imshow(
        pivot_data,
        labels=dict(x="Date", y="Hour", color="Listening Minutes"),
        title=f'Heatmap of Listening Time for the Past {days_to_display} Days',
        color_continuous_scale='blues',
        aspect="auto"
    )
    
    # Customize layout
    fig.update_layout(
        height=600,
        yaxis=dict(
            tickmode='array',
            tickvals=list(range(0, 24, 4)),
            ticktext=[f"{i}:00" for i in range(0, 24, 4)],
            autorange='reversed'  
        ),
        xaxis=dict(
            tickangle=-45
        ),
        font=dict(size=12)
    )
    
    st.plotly_chart(fig, use_container_width=True)

except ImportError:
    st.error("Necessary library 'plotly' is not installed. Please install it using the following command:")
    st.code("pip install plotly")
except Exception as e:
    st.error(f"ERROR: Failed to create heatmap: {e}")

# Listening Summary Metrics
st.markdown("---")
st.subheader("Listening Summary")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_minutes = df_source['Intensity'].sum()
    st.metric("Total Listening Time", f"{total_minutes:.0f} minutes")

with col2:
    avg_daily = total_minutes / days_to_display
    st.metric("Average Daily", f"{avg_daily:.1f} minutes")

with col3:
    max_hour = df_source.loc[df_source['Intensity'].idxmax(), 'Hour'] if not df_source.empty else 0
    st.metric("Most Active Hour", f"{max_hour}:00")

with col4:
    active_days = len(df_source[df_source['Intensity'] > 0]['Date'].unique())
    st.metric("Active Days", f"{active_days} days")