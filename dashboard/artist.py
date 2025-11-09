import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import squarify
import matplotlib.colors as mcolors

from database_manager import SupabaseManager
from sql_query.artist_gender import gender_violin_load_data, gender_bar_load_data

# Initialize database manager
db = SupabaseManager()

st.set_page_config(layout="wide")
st.title("Fun Facts about Artists I Like")
# Interactive slider for days back
days_to_display = st.slider(
    'Time Range (Days Back):', 
    min_value=1, 
    max_value=60, 
    value=30, 
    step=1
)
# ----------------------------------------------------
# The gender of artist
# ----------------------------------------------------
# Load data
col1, col2= st.columns(2)
df_source = gender_violin_load_data(days_to_display,db)
df_source_bar = gender_bar_load_data(days_to_display,db)

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
    st.markdown(f"In the past {days_to_display} days, the number of male artists I've listened to is {(m-f)/m:.2%} more than female artists.")
else:
    st.markdown(f"In the past {days_to_display} days, the number of female artists I've listened to is {(f-m)/f:.2%} more than male artists.")  
st.markdown("---")