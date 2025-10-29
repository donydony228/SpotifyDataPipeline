#!/usr/bin/env python3
"""
🎵 Spotify 音樂分析 Dashboard
一個即時的音樂聽歌習慣分析儀表板

作者: Your Name
建立日期: 2024/10/28
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
import time

# 設定頁面配置
st.set_page_config(
    page_title="🎵 我的音樂分析",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定義 CSS 樣式
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 0.25rem solid #1f77b4;
    }
    .stApp > header {
        background-color: transparent;
    }
    .stApp {
        margin-top: -80px;
    }
    h1 {
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .last-updated {
        text-align: right;
        color: #666;
        font-size: 0.8rem;
        margin-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# 資料庫連線設定
@st.cache_resource
def init_connection():
    """初始化資料庫連線"""
    try:
        # 這裡需要你的 Supabase 連線字串
        # 請在環境變數中設定 SUPABASE_DB_URL
        db_url = os.getenv('SUPABASE_DB_URL', 'postgresql://user:password@host:port/database')
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        st.error(f"資料庫連線失敗: {e}")
        return None

# 模擬資料 (當資料庫還沒有真實資料時使用)
@st.cache_data(ttl=300)  # 快取5分鐘
def get_mock_data():
    """產生模擬的音樂資料"""
    import random
    
    # 模擬今日統計
    today_stats = {
        'total_tracks': random.randint(30, 80),
        'total_minutes': random.randint(120, 300),
        'unique_artists': random.randint(15, 30),
        'unique_genres': random.randint(5, 12)
    }
    
    # 模擬本週趨勢
    dates = pd.date_range(end=datetime.now(), periods=7)
    weekly_data = pd.DataFrame({
        'date': dates,
        'tracks_count': [random.randint(20, 60) for _ in range(7)],
        'total_minutes': [random.randint(80, 200) for _ in range(7)]
    })
    
    # 模擬最愛藝人
    artists = ['Taylor Swift', '周杰倫', 'Post Malone', 'Ed Sheeran', '鄧紫棋', 
               'Olivia Rodrigo', '林俊傑', 'Billie Eilish', '蔡依林', 'The Weeknd']
    top_artists = pd.DataFrame({
        'artist': random.sample(artists, 5),
        'play_count': sorted([random.randint(5, 25) for _ in range(5)], reverse=True)
    })
    
    # 模擬音樂風格分布
    genres = ['Pop', 'Rock', 'Hip-Hop', 'Electronic', 'R&B', 'Country']
    genre_data = pd.DataFrame({
        'genre': genres,
        'percentage': [random.randint(10, 30) for _ in range(len(genres))]
    })
    # 標準化百分比
    genre_data['percentage'] = (genre_data['percentage'] / genre_data['percentage'].sum() * 100).round(1)
    
    # 模擬聽歌時間分析
    hours = list(range(24))
    hour_data = pd.DataFrame({
        'hour': hours,
        'intensity': [random.randint(0, 20) if h in range(7, 23) else random.randint(0, 5) for h in hours]
    })
    
    return today_stats, weekly_data, top_artists, genre_data, hour_data

# 真實資料查詢函數
@st.cache_data(ttl=300)
def get_real_data():
    """從資料庫獲取真實資料"""
    engine = init_connection()
    if engine is None:
        return get_mock_data()
    
    try:
        # 今日統計
        today_query = """
        SELECT 
            COUNT(*) as total_tracks,
            SUM(listening_minutes) as total_minutes,
            COUNT(DISTINCT artist_key) as unique_artists
        FROM dwh.fact_listening f
        JOIN dwh.dim_dates d ON f.date_key = d.date_key
        WHERE d.date_value = CURRENT_DATE
        """
        
        # 本週趨勢
        weekly_query = """
        SELECT 
            d.date_value as date,
            COUNT(*) as tracks_count,
            SUM(f.listening_minutes) as total_minutes
        FROM dwh.fact_listening f
        JOIN dwh.dim_dates d ON f.date_key = d.date_key
        WHERE d.date_value >= CURRENT_DATE - INTERVAL '6 days'
        GROUP BY d.date_value
        ORDER BY d.date_value
        """
        
        # 最愛藝人
        artists_query = """
        SELECT 
            a.artist_name as artist,
            COUNT(*) as play_count
        FROM dwh.fact_listening f
        JOIN dwh.dim_artists a ON f.artist_key = a.artist_key
        JOIN dwh.dim_dates d ON f.date_key = d.date_key
        WHERE d.date_value >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY a.artist_name
        ORDER BY play_count DESC
        LIMIT 5
        """
        
        today_stats_df = pd.read_sql(today_query, engine)
        weekly_data = pd.read_sql(weekly_query, engine)
        top_artists = pd.read_sql(artists_query, engine)
        
        # 轉換今日統計為字典
        if not today_stats_df.empty:
            today_stats = today_stats_df.iloc[0].to_dict()
            today_stats['unique_genres'] = 8  # 暫時固定值
        else:
            return get_mock_data()
        
        # 如果沒有資料，使用模擬資料
        if weekly_data.empty or top_artists.empty:
            return get_mock_data()
        
        # 模擬其他資料 (音樂風格和聽歌時間)
        _, _, _, genre_data, hour_data = get_mock_data()
        
        return today_stats, weekly_data, top_artists, genre_data, hour_data
        
    except Exception as e:
        st.warning(f"資料庫查詢失敗，使用模擬資料: {e}")
        return get_mock_data()

# 主要 Dashboard 函數
def main():
    """主要 dashboard 介面"""
    
    # 標題區域
    st.markdown("# 🎵 我的音樂分析 Dashboard")
    st.markdown("---")
    
    # 側邊欄設定
    with st.sidebar:
        st.markdown("## ⚙️ 設定")
        
        # 資料模式切換
        data_mode = st.radio(
            "資料來源",
            ["🔄 即時資料", "🎭 模擬資料"],
            help="選擇使用即時資料庫資料或模擬資料"
        )
        
        # 自動重新整理
        auto_refresh = st.checkbox("🔄 自動重新整理 (30秒)", value=False)
        
        if st.button("🔄 手動重新整理"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("### 📊 功能說明")
        st.markdown("- 📈 即時聽歌統計")
        st.markdown("- 🎵 音樂風格分析") 
        st.markdown("- 🎤 最愛藝人排行")
        st.markdown("- ⏰ 聽歌時間模式")
    
    # 獲取資料
    if data_mode == "🎭 模擬資料":
        today_stats, weekly_data, top_artists, genre_data, hour_data = get_mock_data()
    else:
        today_stats, weekly_data, top_artists, genre_data, hour_data = get_real_data()
    
    # 今日概覽區域
    st.markdown("## 📊 今日概覽")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🎧 已聽歌曲",
            value=f"{today_stats['total_tracks']} 首",
            delta=f"+{today_stats['total_tracks'] - 35} (vs 昨日)"
        )
    
    with col2:
        hours = today_stats['total_minutes'] // 60
        minutes = today_stats['total_minutes'] % 60
        st.metric(
            label="⏱️ 聆聽時間",
            value=f"{hours}h {minutes}m",
            delta=f"+{today_stats['total_minutes'] - 180}m (vs 昨日)"
        )
    
    with col3:
        st.metric(
            label="🎤 不同藝人",
            value=f"{today_stats['unique_artists']} 位",
            delta=f"+{today_stats['unique_artists'] - 20} (vs 昨日)"
        )
    
    with col4:
        st.metric(
            label="🎵 音樂風格",
            value=f"{today_stats.get('unique_genres', 8)} 種",
            delta="+2 (vs 昨日)"
        )
    
    st.markdown("---")
    
    # 圖表區域
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 本週聽歌趨勢")
        
        # 建立雙軸圖表
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
                line=dict(color='#1f77b4', width=3),
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
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=8)
            ),
            secondary_y=True,
        )
        
        fig.update_xaxes(title_text="日期")
        fig.update_yaxes(title_text="歌曲數量", secondary_y=False)
        fig.update_yaxes(title_text="聆聽時間 (分鐘)", secondary_y=True)
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            showlegend=True,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎤 最愛藝人 TOP 5")
        
        fig = px.bar(
            top_artists,
            x='play_count',
            y='artist',
            orientation='h',
            color='play_count',
            color_continuous_scale='Blues',
            title=""
        )
        
        fig.update_layout(
            height=400,
            showlegend=False,
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=0, r=0, t=30, b=0),
            coloraxis_showscale=False
        )
        
        fig.update_traces(
            texttemplate='%{x}首',
            textposition='outside'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # 第二排圖表
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🎵 音樂風格分布")
        
        fig = px.pie(
            genre_data,
            values='percentage',
            names='genre',
            title="",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>佔比: %{percent}<br>播放次數: %{value}%<extra></extra>'
        )
        
        fig.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=30, b=0),
            showlegend=True,
            legend=dict(orientation="v", yanchor="middle", y=0.5)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### ⏰ 聽歌時間分析")
        
        # 創建一個更有趣的時間熱力圖
        fig = go.Figure(data=go.Scatter(
            x=hour_data['hour'],
            y=hour_data['intensity'],
            mode='lines+markers',
            fill='tonexty',
            line=dict(color='rgba(31, 119, 180, 0.8)', width=3),
            marker=dict(size=8, color='rgba(31, 119, 180, 1)'),
            name='聽歌活躍度'
        ))
        
        # 添加時段標記
        time_periods = [
            (6, 12, "🌅 早晨", "rgba(255, 193, 7, 0.2)"),
            (12, 18, "☀️ 下午", "rgba(255, 152, 0, 0.2)"),
            (18, 22, "🌆 傍晚", "rgba(156, 39, 176, 0.2)"),
            (22, 24, "🌙 夜晚", "rgba(63, 81, 181, 0.2)")
        ]
        
        for start, end, label, color in time_periods:
            fig.add_vrect(
                x0=start, x1=end,
                fillcolor=color,
                opacity=0.3,
                layer="below",
                line_width=0,
            )
        
        fig.update_layout(
            height=400,
            xaxis_title="小時",
            yaxis_title="聽歌活躍度",
            hovermode='x',
            margin=dict(l=0, r=0, t=30, b=0),
            xaxis=dict(tickmode='linear', tick0=0, dtick=4)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # 頁尾資訊
    current_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    st.markdown(f"""
    <div class="last-updated">
        📱 最後更新: {current_time} | 
        📊 資料來源: {'PostgreSQL (Supabase)' if data_mode == '🔄 即時資料' else '模擬資料'} | 
        🎵 音樂分析系統 v1.0
    </div>
    """, unsafe_allow_html=True)
    
    # 自動重新整理功能
    if auto_refresh:
        time.sleep(30)
        st.rerun()

# 執行主程式
if __name__ == "__main__":
    main()