import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import squarify
import matplotlib.colors as mcolors

from database_manager import SupabaseManager
from sql_query.album_treemap import treemap_album_load_data


# Initialize database manager
db = SupabaseManager()

st.set_page_config(layout="wide")
st.title("Fun Facts about Albums I Like")

col1, col2, col3 = st.columns(3)

with col1:
    # New Albums Discovered Today
    result = db.execute_query("select count(album_key) from dwh.dim_albums as a group by a.first_heard order by a.first_heard desc;")
    delta = result['count'][0] - result['count'][1]
    st.metric("New Albums Discovered Today", result['count'][0], border =True, delta= f"{delta}")
with col2:
    # New Albums Discovered This Week
    result = db.execute_query("SELECT (a.first_heard - DATE '2000-01-01') / 7 AS week_group_id, MIN(a.first_heard) AS week_start_date, COUNT(a.album_key) AS album_count FROM dwh.dim_albums AS a WHERE a.first_heard >= CURRENT_DATE - INTERVAL '90 days' GROUP BY 1 ORDER BY week_start_date DESC;")
    delta = result['album_count'][0] - result['album_count'][1]
    st.metric("New Albums Discovered This Week", result['album_count'][0], border =True, delta= f"{delta}")
with col3:
    # New Albums Discovered This Month
    result = db.execute_query("SELECT DATE_TRUNC('month', a.first_heard) AS month_start_date, COUNT(a.album_key) AS album_count FROM dwh.dim_albums AS a WHERE a.first_heard >= CURRENT_DATE - INTERVAL '365 days' GROUP BY 1 ORDER BY month_start_date DESC;")
    delta = result['album_count'][0] - result['album_count'][1]
    st.metric("New Albums Discovered This Month", result['album_count'][0], border =True, delta= f"{delta}")

st.markdown("---")

# Interactive slider for days back
days_to_display = st.slider(
    'Time Range (Days Back):', 
    min_value=1, 
    max_value=60, 
    value=14, 
    step=1
)

# ----------------------------------------------------
# Top Songs Treemap
# ----------------------------------------------------
def generate_blue_colors(values):
    """Generate a list of blue shades based on values."""
    normalized = (values - values.min()) / (values.max() - values.min())
    colors = []
    for val in normalized:
        blue_intensity = 0.3 + (2 * val) 
        color = f'rgba(10, 54, 105, {blue_intensity})' 
        colors.append(color)
    return colors

album_treemap = treemap_album_load_data(days_to_display, db)

col1, col2 = st.columns([2, 1])
with col1:
    # Create treemap
    # st.subheader(f"Top Albums for the Past {days_to_display} Days")

    manual_colors = generate_blue_colors(album_treemap['play_count'])

    fig = go.Figure(go.Treemap(
        labels=album_treemap['album_name'],
        parents=[""] * len(album_treemap),
        values=album_treemap['play_count'],
        textinfo="label+value",
        marker=dict(
            colors=manual_colors, 
            line=dict(width=2, color='white')
        )
    ))

    fig.update_layout(
        height=600,
        font=dict(size=20),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        coloraxis=dict(colorscale='Blues', showscale=False),
        title=f"Top Tracks for the Past {days_to_display} Days"
    )

    st.plotly_chart(fig, width='stretch')
with col2:
    # result = db.execute_query("select d.artist_name, count(f.listening_key) from dwh.fact_listening as f left join dwh.dim_artists as d on f.artist_key = d.artist_key where f.created_at > current_date - 7 group by d.artist_name order by count desc limit 1")
    st.header(" ")
    st.header(" ")
    st.metric(f"Most Listened Album in the Past {days_to_display} Days", album_treemap['album_name'][0])
    st.header(" ")
    st.metric("Plays", album_treemap['play_count'][0])

st.markdown("---")
