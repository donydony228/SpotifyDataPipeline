#!/usr/bin/env python3
"""
📊 圖表元件
處理各種圖表的產生和顯示
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from config import config

def render_charts_section(db, sidebar_state):
    """渲染圖表區域"""
    
    # 取得資料
    weekly_data = db.get_weekly_trends()
    top_artists = db.get_top_artists()
    
    if weekly_data is None or weekly_data.empty:
        st.error("❌ 無法取得圖表資料")
        return
    
    # 根據圖表類型決定顯示的圖表
    chart_type = sidebar_state.get('chart_type', '標準')
    
    if chart_type == "簡化":
        render_simple_charts(weekly_data, top_artists)
    elif chart_type == "詳細":
        render_detailed_charts(weekly_data, top_artists)
    else:
        render_standard_charts(weekly_data, top_artists)

def render_standard_charts(weekly_data, top_artists):
    """渲染標準圖表"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 本週趨勢")
        
        # 週趨勢圖
        fig = create_weekly_trend_chart(weekly_data)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### 🎤 最愛藝人")
        
        # 藝人排行圖
        fig = create_top_artists_chart(top_artists)
        st.plotly_chart(fig, use_container_width=True)

def render_simple_charts(weekly_data, top_artists):
    """渲染簡化圖表"""
    
    st.markdown("#### 📊 快速概覽")
    
    # 只顯示週趨勢的簡化版本
    fig = px.line(
        weekly_data,
        x='date',
        y='tracks_count',
        title="本週聽歌數量趨勢",
        markers=True
    )
    
    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=40, b=0),
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 簡單的藝人列表
    st.markdown("#### 🎤 TOP 5 藝人")
    for i, row in top_artists.iterrows():
        st.write(f"{i+1}. **{row['artist']}** - {row['play_count']} 首")

def render_detailed_charts(weekly_data, top_artists):
    """渲染詳細圖表"""
    
    # 週趨勢 (雙軸)
    st.markdown("#### 📈 詳細週趨勢分析")
    fig = create_detailed_weekly_chart(weekly_data)
    st.plotly_chart(fig, use_container_width=True)
    
    # 三欄佈局
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 🎤 藝人排行")
        fig = create_top_artists_chart(top_artists)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### 📊 聽歌分布")
        # 模擬一個圓餅圖
        fig = create_mock_pie_chart()
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        st.markdown("#### ⏰ 時間分析")
        # 模擬時間分析圖
        fig = create_mock_time_chart()
        st.plotly_chart(fig, use_container_width=True)

def create_weekly_trend_chart(weekly_data):
    """建立週趨勢圖表"""
    
    fig = px.line(
        weekly_data,
        x='date',
        y='tracks_count',
        title="本週聽歌趨勢",
        markers=True,
        color_discrete_sequence=config.CHART_COLOR_SEQUENCE
    )
    
    fig.update_layout(
        height=config.CHART_HEIGHT,
        hovermode='x unified',
        margin=dict(l=0, r=0, t=40, b=0),
        showlegend=False
    )
    
    fig.update_traces(
        line=dict(width=3),
        marker=dict(size=8)
    )
    
    return fig

def create_detailed_weekly_chart(weekly_data):
    """建立詳細週趨勢圖表 (雙軸)"""
    
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        subplot_titles=()
    )
    
    # 歌曲數量
    fig.add_trace(
        go.Scatter(
            x=weekly_data['date'],
            y=weekly_data['tracks_count'],
            mode='lines+markers',
            name='歌曲數量',
            line=dict(color=config.CHART_COLOR_SEQUENCE[0], width=3),
            marker=dict(size=8)
        ),
        secondary_y=False,
    )
    
    # 聆聽時間
    fig.add_trace(
        go.Scatter(
            x=weekly_data['date'],
            y=weekly_data['total_minutes'],
            mode='lines+markers',
            name='聆聽時間(分鐘)',
            line=dict(color=config.CHART_COLOR_SEQUENCE[1], width=3),
            marker=dict(size=8)
        ),
        secondary_y=True,
    )
    
    fig.update_xaxes(title_text="日期")
    fig.update_yaxes(title_text="歌曲數量", secondary_y=False)
    fig.update_yaxes(title_text="聆聽時間 (分鐘)", secondary_y=True)
    
    fig.update_layout(
        height=config.CHART_HEIGHT,
        hovermode='x unified',
        showlegend=True,
        margin=dict(l=0, r=0, t=30, b=0)
    )
    
    return fig

def create_top_artists_chart(top_artists):
    """建立最愛藝人圖表"""
    
    fig = px.bar(
        top_artists,
        x='play_count',
        y='artist',
        orientation='h',
        title="",
        color='play_count',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        height=config.CHART_HEIGHT,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'},
        margin=dict(l=0, r=0, t=30, b=0),
        coloraxis_showscale=False
    )
    
    fig.update_traces(
        texttemplate='%{x}首',
        textposition='outside'
    )
    
    return fig

def create_mock_pie_chart():
    """建立模擬圓餅圖"""
    
    data = {
        'genre': ['Pop', 'Rock', 'Hip-Hop', 'Electronic', 'R&B'],
        'count': [30, 25, 20, 15, 10]
    }
    
    fig = px.pie(
        values=data['count'],
        names=data['genre'],
        title="",
        color_discrete_sequence=config.CHART_COLOR_SEQUENCE
    )
    
    fig.update_layout(
        height=config.CHART_HEIGHT - 50,
        margin=dict(l=0, r=0, t=30, b=0),
        showlegend=True
    )
    
    return fig

def create_mock_time_chart():
    """建立模擬時間分析圖"""
    
    import random
    
    hours = list(range(24))
    intensity = [random.randint(0, 20) if h in range(7, 23) else random.randint(0, 5) for h in hours]
    
    fig = go.Figure(data=go.Scatter(
        x=hours,
        y=intensity,
        mode='lines+markers',
        fill='tonexty',
        line=dict(color=config.CHART_COLOR_SEQUENCE[2], width=3),
        marker=dict(size=6),
        name='聽歌活躍度'
    ))
    
    fig.update_layout(
        height=config.CHART_HEIGHT - 50,
        xaxis_title="小時",
        yaxis_title="活躍度",
        margin=dict(l=0, r=0, t=30, b=0),
        showlegend=False
    )
    
    return fig