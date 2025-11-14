# streamlit run dashboard/app.py
import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import squarify
import matplotlib.colors as mcolors

from database_manager import SupabaseManager
from sql_query.main_heatmap import heatmap_load_data
from sql_query.main_ratio import hour_ratio_load_data, radar_load_data

# Initialize database manager
db = SupabaseManager()

st.set_page_config(layout="wide")
st.markdown("""
<style>
.block-container {
    padding-top: 0rem;
    padding-right: 2rem;
    padding-left: 2rem;
    padding-bottom: 0rem;
}

.st-emotion-cache-1cypcd9 {
    padding-top: 1rem;
}

.st-emotion-cache-1y4pm5c { 
    padding-top: 0rem; 
}
</style>
""", unsafe_allow_html=True)
# -----------------------------------------------

# Image Display
st.image("dashboard/IMG_2417.jpg", width='stretch')
st.title("Spotify Recap Dashboard")

# Interactive slider for days back
days_to_display = st.slider(
    'Time Range (Days Back):', 
    min_value=7, 
    max_value=365, 
    value=30, 
    step=7
)

# ----------------------------------------------------
# Heatmap
# ----------------------------------------------------
# Load data
df_source = heatmap_load_data(days_to_display, db)

if df_source.empty:
    st.stop()

# Create heatmap
st.subheader(f"Listening Time for the Past {days_to_display} Days")

# Use Plotly for heatmap visualization
pivot_data = df_source.pivot(index='Hour', columns='Date', values='Intensity')

fig = px.imshow(
    pivot_data,
    labels=dict(x="Date", y="Hour", color="Minutes"),
    color_continuous_scale='blues',
    aspect="auto"
)

# Customize layout
fig.update_layout(
    height=400,
    yaxis=dict(
        tickmode='array',
        tickvals=list(range(0, 24, 4)),
        ticktext=[f"{i}:00" for i in range(0, 24, 4)],
        autorange='reversed'  
    ),
    xaxis=dict(
        # tickangle=-45
    ),
    font=dict(size=12)
)

st.plotly_chart(fig, width='stretch')
st.markdown("---")

# ----------------------------------------------------
# Listening Summary Metrics
# ----------------------------------------------------
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

st.markdown("---")

# ----------------------------------------------------
# Hourly Listening Ratio by Day of Week
# ----------------------------------------------------
# Load data 
df = hour_ratio_load_data(db)

if df.empty:
    st.stop()
st.subheader(f"Hourly Listening Ratio by Day of Week")

days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
periods = ['Midnight (00-05)', 'Morning (06-11)', 'Afternoon (12-17)', 'Evening (18-23)']
colors = ["#B4C3D0", "#5E8098", "#3C6784", "#06263B"]

day_mapping = {
    '週一': 'Monday', '週二': 'Tuesday', '週三': 'Wednesday', 
    '週四': 'Thursday', '週五': 'Friday', '週六': 'Saturday', '週日': 'Sunday',
    'Monday': 'Monday', 'Tuesday': 'Tuesday', 'Wednesday': 'Wednesday',
    'Thursday': 'Thursday', 'Friday': 'Friday', 'Saturday': 'Saturday', 'Sunday': 'Sunday'
}

df_processed = df.copy()
df_processed['day_name'] = df_processed['day_name'].str.strip()
df_processed['day_name_en'] = df_processed['day_name'].map(day_mapping)

pivot_df = df_processed.pivot_table(
    index='day_name_en', 
    columns='time_period', 
    values='minutes_percentage',
    aggfunc='first'
)

pivot_df = pivot_df.reindex(days_order, fill_value=0)

for period in periods:
    if period not in pivot_df.columns:
        pivot_df[period] = 0

pivot_df = pivot_df[periods]

fig = go.Figure()
fig.update_traces(marker_line_width=0)
for i, period in enumerate(periods):
    values = pivot_df[period].fillna(0)
    
    fig.add_trace(go.Bar(
        name=period,
        y=pivot_df.index,  
        x=values,          
        orientation='h',
        marker_color=colors[i],
        text=[f'{val:.1f}%' if val > 5 else '' for val in values],
        textposition='inside',
        textfont=dict(color='white', size=12),
        hovertemplate=f'<b>{period}</b><br>' +
                        'Weekday: %{y}<br>' +
                        'Percentage: %{x:.1f}%<br>' +
                        '<extra></extra>'
    ))

# 更新布局
fig.update_layout(
    barmode='stack',
    xaxis=dict(
        title='Time Percentage (%)',
        range=[0, 100],
        ticksuffix='%',
        showgrid=True,
        gridcolor='#E8E8E8'
    ),
    yaxis=dict(
        title='Weekday',
        categoryorder='array',
        categoryarray=days_order[::-1]  
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        bgcolor='rgba(255,255,255,0.8)'
    ),
    height=500,
    width=800,
    plot_bgcolor='white',
    paper_bgcolor='white'
)
st.plotly_chart(fig, width='stretch')
st.markdown("---")

# ----------------------------------------------------
# Radar Chart for Listening Habits
# ----------------------------------------------------
st.subheader("Listening Habits Radar Chart")
col1, col2 = st.columns([2, 1])
# Load data
df_behavior = radar_load_data(db)

# Prepare radar chart data
categories = ['Listening Time', 'Music Diversity', 'Track Repeat Ratio', 'Daytime Listening', 'Activity Level', 'Conversation Length']

def normalize_score(value, max_val, reverse=False):
    score = (value / max_val) * 100
    return 100 - score if reverse else score

# 工作日數據
workday = df_behavior[df_behavior['day_type'] == 'Weekday'].iloc[0]
weekend = df_behavior[df_behavior['day_type'] == 'Weekend'].iloc[0]

# 計算最大值用於標準化
max_vals = {
    'total_time': max(workday['total_time'], weekend['total_time']),
    'unique_tracks': max(workday['unique_tracks'], weekend['unique_tracks']),
    'track_repeat_ratio': max(workday['track_repeat_ratio'], weekend['track_repeat_ratio']),
    # 'avg_track_popularity': 100,  # 流行度最大值
    # 'explicit_percentage': 100,  # 百分比最大值
    'daytime_listening_percentage': 100,
    'avg_sessions_per_day': max(workday['avg_sessions_per_day'], weekend['avg_sessions_per_day']),
    'avg_session_minutes': max(workday['avg_session_minutes'], weekend['avg_session_minutes'])
}

workday_scores = [
    normalize_score(workday['total_time'], max_vals['total_time']),
    normalize_score(workday['unique_tracks'], max_vals['unique_tracks']), 
    normalize_score(workday['track_repeat_ratio'], max_vals['track_repeat_ratio']),
    # normalize_score(workday['avg_track_popularity'], max_vals['avg_track_popularity']),
    # normalize_score(workday['explicit_percentage'], max_vals['explicit_percentage']),
    normalize_score(workday['daytime_listening_percentage'], max_vals['daytime_listening_percentage']),
    normalize_score(workday['avg_sessions_per_day'], max_vals['avg_sessions_per_day']),
    normalize_score(workday['avg_session_minutes'], max_vals['avg_session_minutes'])
]

weekend_scores = [
    normalize_score(weekend['total_time'], max_vals['total_time']),
    normalize_score(weekend['unique_tracks'], max_vals['unique_tracks']),
    normalize_score(weekend['track_repeat_ratio'], max_vals['track_repeat_ratio']),
    # normalize_score(weekend['avg_track_popularity'], max_vals['avg_track_popularity']),
    # normalize_score(weekend['explicit_percentage'], max_vals['explicit_percentage']),
    normalize_score(weekend['daytime_listening_percentage'], max_vals['daytime_listening_percentage']),
    normalize_score(weekend['avg_sessions_per_day'], max_vals['avg_sessions_per_day']),
    normalize_score(weekend['avg_session_minutes'], max_vals['avg_session_minutes'])
]

with col1:
    fig = go.Figure()

    # 工作日雷達
    fig.add_trace(go.Scatterpolar(
        r=workday_scores,
        theta=categories,
        fill='toself',
        name='Weekday',
        line=dict(color="#154D72", width=2),
        fillcolor='rgba(52, 152, 219, 0.3)'
    ))

    # 週末雷達
    fig.add_trace(go.Scatterpolar(
        r=weekend_scores,
        theta=categories,
        fill='toself', 
        name='Weekend',
        line=dict(color="#9F2F22", width=2),
        fillcolor='rgba(231, 76, 60, 0.3)'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                ticksuffix='%'
            )
        ),
        legend=dict(x=0.8, y=0.95),
        height=600
    )

    st.plotly_chart(fig, width='stretch')
with col2:
    st.subheader(" ")
    st.subheader(" ")
    st.markdown("""
        ***Weekday Pattern (Blue Area)***

        - High Music Diversity - Explores different tracks during work
        - High Track Repeat Ratio - Tends to replay favorite songs
        - Extended Listening Time - Music as constant companion

        ***Weekend Pattern (Red Area)***

        - Focused Sessions - Longer, more intentional listening periods
        - Higher Activity Level - More frequent music engagement
        - Less Daytime Listening - Shifts toward evening/night sessions

        ***Insights***
        - Weekdays: Music as background - seeking variety with familiar comfort
        - Weekends: Music as experience - deeper, more concentrated enjoyment""")
# End of the dashboard code
st.markdown("---")
st.markdown("© Created by Desmond Peng")