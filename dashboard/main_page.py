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
from heatmap import heatmap_load_data
from treemap_track import treemap_track_load_data
from treemap_artist import treemap_artist_load_data

def generate_blue_colors(values):
    """Generate a list of blue shades based on values."""
    normalized = (values - values.min()) / (values.max() - values.min())
    colors = []
    for val in normalized:
        blue_intensity = 0.3 + (2 * val) 
        color = f'rgba(10, 54, 105, {blue_intensity})' 
        colors.append(color)
    return colors

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

# Check and initialize the database manager
try:
    db = SupabaseManager()
except NameError:
    st.error("ERROR: SupabaseManager is not defined. Please check the database_manager module.")
    st.stop()
except Exception as e:
    st.error(f"ERROR: Database initialization failed: {e}")
    st.stop()

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
st.subheader(f"Heatmap of Listening Time for the Past {days_to_display} Days")

# Use Plotly for heatmap visualization
pivot_data = df_source.pivot(index='Hour', columns='Date', values='Intensity')

fig = px.imshow(
    pivot_data,
    labels=dict(x="Date", y="Hour", color="Minutes"),
    # title=f'Heatmap of Listening Time for the Past {days_to_display} Days',
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
# Treemap
# ----------------------------------------------------
col1, col2 = st.columns(2)

# Load data
track_treemap = treemap_track_load_data(days_to_display, db)
artist_treemap = treemap_artist_load_data(days_to_display, db)

with col1:
    # Create treemap
    st.subheader(f"Treemap of Top Artists for the Past {days_to_display} Days")

    manual_colors = generate_blue_colors(track_treemap['play_count'])

    fig = go.Figure(go.Treemap(
        labels=artist_treemap['artist_name'],
        parents=[""] * len(artist_treemap),
        values=artist_treemap['play_count'],
        textinfo="label+value",
        marker=dict(
            colors=manual_colors, 
            line=dict(width=2, color='white')
        )
    ))

    fig.update_layout(
        height=600,
        # title=dict(
        #     text=f"Treemap of Top Artists for the Past {days_to_display} Days", 
        #     font=dict(size=30, color='black', family='Arial')  
        # ),
        font=dict(size=20),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        coloraxis=dict(colorscale='Blues', showscale=False)
    )

    st.plotly_chart(fig, width='stretch')

with col2:
    # Create treemap
    st.subheader(f"Treemap of Top Tracks for the Past {days_to_display} Days")

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
        coloraxis=dict(colorscale='Blues', showscale=False)
    )

    st.plotly_chart(fig, width='stretch')

# ----------------------------------------------------
# Listening Summary Metrics
# ----------------------------------------------------
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