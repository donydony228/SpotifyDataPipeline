import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from database_manager import SupabaseManager
from sql_query.artist_gender import gender_violin_load_data, gender_bar_load_data, gender_bar_by_date
from sql_query.artist_band import band_violin_load_data, band_bar_load_data
from sql_query.artist_loyal import basic_loyal_load_data
from sql_query.artist_treemap import treemap_artist_load_data
# Initialize database manager
db = SupabaseManager()

st.set_page_config(layout="wide")
st.title("Fun Facts about Artists I Like")

col1, col2, col3 = st.columns(3)

with col1:
    # New Artists Discovered Today
    result = db.execute_query("select count(artist_key) from dwh.dim_artists as a group by a.first_discovered order by a.first_discovered desc;")
    delta = result['count'][0] - result['count'][1]
    st.metric("New Artists Discovered Today", result['count'][0], border =True, delta= f"{delta}")

with col2:
    # New Artists Discovered This Week
    result = db.execute_query("SELECT (a.first_discovered - DATE '2000-01-01') / 7 AS week_group_id, MIN(a.first_discovered) AS week_start_date, COUNT(a.artist_key) AS artist_count FROM dwh.dim_artists AS a WHERE a.first_discovered >= CURRENT_DATE - INTERVAL '90 days' GROUP BY 1 ORDER BY week_start_date DESC;")
    delta = result['artist_count'][0] - result['artist_count'][1]
    st.metric("New Artists Discovered This Week", result['artist_count'][0], border =True, delta= f"{delta}")

with col3:
    # New Artists Discovered This Month
    result = db.execute_query("SELECT DATE_TRUNC('month', a.first_discovered) AS month_start_date, COUNT(a.artist_key) AS artist_count FROM dwh.dim_artists AS a WHERE a.first_discovered >= CURRENT_DATE - INTERVAL '365 days' GROUP BY 1 ORDER BY month_start_date DESC;")
    delta = result['artist_count'][0] - result['artist_count'][1]
    st.metric("New Artists Discovered This Month", result['artist_count'][0], border =True, delta= f"{delta}")

st.markdown("---")

# Interactive slider for days back
days_to_display = st.slider(
    'Time Range (Days Back):', 
    min_value=1, 
    max_value=60, 
    value=30, 
    step=1
)

# ----------------------------------------------------
# Top Artists Treemap
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
col1, col2 = st.columns([2, 1])

# Load data
artist_treemap = treemap_artist_load_data(days_to_display, db)

with col1:
    # Create treemap
    # st.subheader(f"Top Artists for the Past {days_to_display} Days")

    manual_colors = generate_blue_colors(artist_treemap['play_count'])

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
        font=dict(size=20),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        coloraxis=dict(colorscale='Blues', showscale=False),
        title=f"Top Artists for the Past {days_to_display} Days"
    )

    st.plotly_chart(fig, width='stretch')
with col2:
    # result = db.execute_query("select d.artist_name, count(f.listening_key) from dwh.fact_listening as f left join dwh.dim_artists as d on f.artist_key = d.artist_key where f.created_at > current_date - 7 group by d.artist_name order by count desc limit 1")
    st.header(" ")
    st.header(" ")
    st.metric(f"Most Listened Artist in the Past {days_to_display} Days", artist_treemap['artist_name'][0])
    st.header(" ")
    st.metric("Plays", artist_treemap['play_count'][0])

st.markdown("---")

# ----------------------------------------------------
# The gender of artist
# ----------------------------------------------------
# Load data
df_source = gender_violin_load_data(days_to_display,db)
df_source_bar = gender_bar_load_data(days_to_display,db)
df_source_by_date = gender_bar_by_date(db)

df_source_by_date["first_discovered"] = pd.to_datetime(df_source_by_date["first_discovered"]).dt.date

fig = px.line(df_source_by_date, x="first_discovered", y="count", color="gender",
              title="New Artists Discovered Over Time by Gender", markers=True)
fig.update_layout(xaxis_title="Date Discovered", yaxis_title="Number of New Artists Discovered")
st.plotly_chart(fig, width='stretch')

st.markdown("---")

col1, col2= st.columns(2)

if df_source.empty:
    st.stop()

with col1:
    fig = px.violin(df_source, y="listening_minutes", x="gender", color="gender", box=True, points="all",
            hover_data=df_source.columns)
    fig.update_layout(title="Distribution of Listening Minutes by Artist Gender")
    st.plotly_chart(fig, width='stretch')

with col2:
    fig = px.bar(df_source_bar, x="gender", y="count", color="gender", barmode="group",
                 hover_data=df_source_bar.columns)
    fig.update_layout(title="Total Listening Times by Artist Gender")
    st.plotly_chart(fig, width='stretch')

m = df_source_bar[df_source_bar['gender']=='M']['count'].sum()
f = df_source_bar[df_source_bar['gender']=='F']['count'].sum()
if m > f:
    # M is the larger number, F is the base (denominator)
    percentage = (m - f) / f
    st.markdown(f"In the past **{days_to_display}** days, the number of male artists I've listened to is **{percentage:.2%} more than** female artists.")
elif f > m:
    # F is the larger number, M is the base (denominator)
    percentage = (f - m) / m
    st.markdown(f"In the past **{days_to_display}** days, the number of female artists I've listened to is **{percentage:.2%} more than** male artists.")
else: # m == f
    st.markdown(f"In the past **{days_to_display}** days, the number of male and female artists listened to is **equal**.")

st.markdown("---")

# ----------------------------------------------------
# Band or not
# ----------------------------------------------------
# Load data
col1, col2= st.columns(2)
df_source = band_violin_load_data(days_to_display,db)
df_source_bar = band_bar_load_data(days_to_display,db)

if df_source.empty:
    st.stop()

with col1:
    fig = px.violin(df_source, y="listening_minutes", x="band", color="band", box=True, points="all",
            hover_data=df_source.columns)
    fig.update_layout(title="Distribution of Listening Minutes by Band or Solo Artist")
    st.plotly_chart(fig, width='stretch')

with col2:
    fig = px.bar(df_source_bar, x="band", y="count", color="band", barmode="group",
                hover_data=df_source_bar.columns)
    fig.update_layout(title="Total Listening Times by Band or Solo Artist")
    st.plotly_chart(fig, width='stretch')

band = df_source_bar[df_source_bar['band']]['count'].sum()
solo = df_source_bar[~df_source_bar['band']]['count'].sum()
if band > solo:
    # band is the larger number, solo is the base (denominator)
    percentage = (band - solo) / solo
    st.markdown(f"In the past **{days_to_display}** days, the number of bands I've listened to is **{percentage:.2%} more than** solo artists.")
elif solo > band:       
    # solo is the larger number, band is the base (denominator)
    percentage = (solo - band) / band
    st.markdown(f"In the past **{days_to_display}** days, the number of solo artists I've listened to is **{percentage:.2%} more than** bands.")
else: # band == solo
    st.markdown(f"{band, solo}In the past **{days_to_display}** days, the number of bands and solo artists listened to is **equal**.")

# ----------------------------------------------------
# Artist Loyalty
# ----------------------------------------------------
st.markdown("---")
# st.header("Artist Loyalty Analysis")    
# Load data
df = basic_loyal_load_data(db)
fig_scatter = px.scatter(
    df, 
    x='total_plays', 
    y='loyalty_ratio',
    color='loyalty_level',
    size='active_days',
    hover_data=['artist_name', 'avg_plays_per_day'],
    title="Loyalty Ratio* vs Total Plays of Artists"
)
fig_scatter.update_layout(xaxis_title="Total Plays", yaxis_title="Loyalty Ratio*")
st.plotly_chart(fig_scatter, width='stretch')
st.markdown("*Loyalty Ratio = Active Days / Total Span Days")