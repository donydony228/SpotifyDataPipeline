import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
# import matplotlib.pyplot as plt
# import squarify
# import matplotlib.colors as mcolors

from database_manager import SupabaseManager
from sql_query.track_treemap import treemap_track_load_data
from sql_query.track_sankey import track_sankey_load_data


# Initialize database manager
db = SupabaseManager()

st.set_page_config(layout="wide")
st.title("Fun Facts about Songs I Like")

col1, col2, col3 = st.columns(3)

with col1:
    # New Songs Discovered Today
    result = db.execute_query("select count(artist_key) from dwh.dim_artists as a group by a.first_discovered order by a.first_discovered desc;")
    delta = result['count'][0] - result['count'][1]
    st.metric("New Songs Discovered Today", result['count'][0], border =True, delta= f"{delta}")
with col2:
    # New Songs Discovered This Week
    result = db.execute_query("SELECT (a.first_heard - DATE '2000-01-01') / 7 AS week_group_id, MIN(a.first_heard) AS week_start_date, COUNT(a.track_key) AS track_count FROM dwh.dim_tracks AS a WHERE a.first_heard >= CURRENT_DATE - INTERVAL '90 days' GROUP BY 1 ORDER BY week_start_date DESC;")
    delta = result['track_count'][0] - result['track_count'][1]
    st.metric("New Songs Discovered This Week", result['track_count'][0], border =True, delta= f"{delta}")
with col3:
    # New Songs Discovered This Month
    result = db.execute_query("SELECT DATE_TRUNC('month', a.first_heard) AS month_start_date, COUNT(a.track_key) AS track_count FROM dwh.dim_tracks AS a WHERE a.first_heard >= CURRENT_DATE - INTERVAL '365 days' GROUP BY 1 ORDER BY month_start_date DESC;")
    delta = result['track_count'][0] - result['track_count'][1]
    st.metric("New Songs Discovered This Month", result['track_count'][0], border =True, delta= f"{delta}")

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

track_treemap = treemap_track_load_data(days_to_display, db)

col1, col2 = st.columns([2, 1])
with col1:
    # Create treemap
    # st.subheader(f"Top Tracks for the Past {days_to_display} Days")

    manual_colors = generate_blue_colors(track_treemap['play_count'])

    fig = go.Figure(go.Treemap(
        labels=track_treemap['track_name'],
        parents=[""] * len(track_treemap),
        values=track_treemap['play_count'],
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
    st.metric(f"Most Listened Track in the Past {days_to_display} Days", track_treemap['track_name'][0])
    st.header(" ")
    st.metric("Plays", track_treemap['play_count'][0])

st.markdown("---")

# ----------------------------------------------------
# Track Sankey Diagram
# ----------------------------------------------------
# st.subheader("Track Listening Behavior Sankey Diagram")
sankey_data = track_sankey_load_data(db)    

nodes = [
        # Initial Node
        "First Listen",
        
        # Transition Categories
        "One Hit Wonder", "Casual Listen", "Regular Play", "Heavy Rotation",

        # Lifecycle Stages
        "Quick Drop", "Short Term", "Medium Term", "Long Term"
    ]
    
# 2. Define node colors
node_colors = [
    "#263647",  # First Listen - 深藍灰
    "#E74C3C",  # One Hit Wonder - 紅色
    "#F39C12",  # Casual Listen - 橙色  
    "#3498DB",  # Regular Play - 藍色
    "#27AE60",  # Heavy Rotation - 綠色
    "#1ABC9C",  # Quick Drop - 青色
    "#16A085",  # Short Term - 深青色
    "#48C9B0",  # Medium Term - 淺青色
    "#76D7C4"   # Long Term - 很淺青色
]
# 3. Define links
source_nodes = []
target_nodes = []
values = []
link_colors = []

# First Listen -> Transition Categories
conversion_summary = sankey_data.groupby('conversion_category')['actual_total_plays'].sum().reset_index()
for _, row in conversion_summary.iterrows():
    source_nodes.append(0)  # First Listen
    target_nodes.append(nodes.index(row['conversion_category']))
    values.append(row['actual_total_plays'])
    link_colors.append("rgba(0, 0, 80, 0.3)")  

# Transition Categories -> Lifecycle Stages
lifecycle_flow = sankey_data.groupby(['conversion_category', 'lifecycle_stage'])['actual_total_plays'].sum().reset_index()

for _, row in lifecycle_flow.iterrows():
    source_idx = nodes.index(row['conversion_category'])
    target_idx = nodes.index(row['lifecycle_stage'])
    source_nodes.append(source_idx)
    target_nodes.append(target_idx)
    values.append(row['actual_total_plays'])

    if row['conversion_category'] == 'Heavy Rotation':
        link_colors.append("rgba(39, 174, 96, 0.4)")   # 綠色
    elif row['conversion_category'] == 'Regular Play':
        link_colors.append("rgba(52, 152, 219, 0.4)")  # 藍色
    elif row['conversion_category'] == 'Casual Listen':
        link_colors.append("rgba(243, 156, 18, 0.4)")  # 橙色
    else:  # One Hit Wonder
        link_colors.append("rgba(231, 76, 60, 0.4)")   # 紅色

# 4. Create Sankey Diagram
fig_sankey = go.Figure(data=[go.Sankey(
    node=dict(
        pad=20,
        thickness=25,
        line=dict(color="white", width=2),
        label=nodes,
        color=node_colors,
        hovertemplate='<b>%{label}</b><br>Number of tracks: %{value}<extra></extra>'
    ),
    link=dict(
        source=source_nodes,
        target=target_nodes,
        value=values,
        color=link_colors,
        hovertemplate='From %{source.label}<br>To %{target.label}<br>Number of tracks: %{value}<extra></extra>'
    )
)])

fig_sankey.update_layout(
    title=dict(
        text="Track Listening Behavior Sankey Diagram",
        # x=0.5,
        font=dict(size=18, color='#2C3E50'),
    ),
    font_size=12,
    height=600,
    width=1000,
    paper_bgcolor='rgba(0,0,0,0)', #透明背景
    plot_bgcolor='rgba(255,255,255,0)' #透明背景
)
st.plotly_chart(fig_sankey, use_container_width=True)

# Add some definitions
st.markdown("""One Hit Wonder: Tracks played only once after discovery.  
Casual Listen: Tracks played 2-5 times after discovery.  
Regular Play: Tracks played 6-15 times after discovery.  
Heavy Rotation: Tracks played more than 15 times after discovery.
            
Quick Drop: Tracks with last play within 3 days of first heard.  
Short Term: Tracks with last play within 7 days of first heard.  
Medium Term: Tracks with last play within 14 days of first heard.  
Long Term: Tracks with last play beyond 14 days of first heard.""")
st.markdown("---")