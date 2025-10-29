#!/usr/bin/env python3
"""
📊 統計卡片元件
顯示各種統計數據
"""

import streamlit as st

def render_metrics_section(db, sidebar_state):
    """渲染統計數據區域"""
    
    # 取得資料
    daily_stats = db.get_daily_stats()
    
    if daily_stats is None:
        st.error("❌ 無法取得統計資料")
        return
    
    # 顯示統計卡片
    st.markdown("### 📈 今日統計")
    
    # 建立四列
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🎧 聽歌數量",
            value=f"{daily_stats.get('total_tracks', 0)} 首",
            delta=f"+{daily_stats.get('total_tracks', 0) - 35}",
            help="今日總共聽了多少首歌"
        )
    
    with col2:
        total_minutes = daily_stats.get('total_minutes', 0)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        st.metric(
            label="⏱️ 聆聽時間",
            value=f"{hours}h {minutes}m",
            delta=f"+{total_minutes - 180}m",
            help="今日總聆聽時長"
        )
    
    with col3:
        st.metric(
            label="🎤 不同藝人",
            value=f"{daily_stats.get('unique_artists', 0)} 位",
            delta=f"+{daily_stats.get('unique_artists', 0) - 20}",
            help="今日聽了多少位不同藝人"
        )
    
    with col4:
        st.metric(
            label="🎵 不同歌曲",
            value=f"{daily_stats.get('unique_tracks', 0)} 首",
            delta=f"+{daily_stats.get('unique_tracks', 0) - 30}",
            help="今日聽了多少首不同歌曲"
        )
    
    # 額外資訊 (如果需要)
    if sidebar_state.get('chart_type') == "詳細":
        st.markdown("---")
        st.markdown("### 📊 詳細統計")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 計算一些衍生指標
            avg_track_length = total_minutes / max(daily_stats.get('total_tracks', 1), 1)
            st.info(f"📏 平均歌曲長度: {avg_track_length:.1f} 分鐘")
            
            repeat_rate = (1 - daily_stats.get('unique_tracks', 0) / max(daily_stats.get('total_tracks', 1), 1)) * 100
            st.info(f"🔄 重複播放率: {repeat_rate:.1f}%")
        
        with col2:
            diversity_score = daily_stats.get('unique_artists', 0) / max(daily_stats.get('unique_tracks', 1), 1) * 100
            st.info(f"🎨 音樂多樣性: {diversity_score:.1f}%")
            
            activity_level = "低" if total_minutes < 120 else "中" if total_minutes < 240 else "高"
            st.info(f"⚡ 活躍程度: {activity_level}")

def render_custom_metric(title, value, delta=None, help_text=None):
    """渲染自定義統計卡片"""
    
    delta_html = ""
    if delta is not None:
        delta_color = "green" if delta >= 0 else "red"
        delta_symbol = "+" if delta >= 0 else ""
        delta_html = f"""
        <div style="color: {delta_color}; font-size: 0.8rem;">
            {delta_symbol}{delta}
        </div>
        """
    
    help_html = ""
    if help_text:
        help_html = f'<div style="font-size: 0.7rem; color: #666;">{help_text}</div>'
    
    card_html = f"""
    <div class="metric-card">
        <div style="font-size: 0.9rem; color: #666; margin-bottom: 0.5rem;">
            {title}
        </div>
        <div style="font-size: 1.5rem; font-weight: bold; margin-bottom: 0.25rem;">
            {value}
        </div>
        {delta_html}
        {help_html}
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)

def render_summary_box(title, items):
    """渲染摘要框"""
    
    items_html = ""
    for item in items:
        items_html += f"<li>{item}</li>"
    
    summary_html = f"""
    <div style="
        background-color: #f8f9fa; 
        border: 1px solid #dee2e6; 
        border-radius: 0.5rem; 
        padding: 1rem; 
        margin: 1rem 0;
    ">
        <h4 style="margin: 0 0 0.5rem 0; color: #495057;">{title}</h4>
        <ul style="margin: 0; padding-left: 1.2rem;">
            {items_html}
        </ul>
    </div>
    """
    
    st.markdown(summary_html, unsafe_allow_html=True)