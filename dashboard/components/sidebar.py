#!/usr/bin/env python3
"""
🎛️ 側邊欄元件
處理所有控制面板邏輯
"""

import streamlit as st
from datetime import date, timedelta

def render_sidebar():
    """渲染側邊欄並返回用戶選擇的狀態"""
    
    with st.sidebar:
        st.markdown("## ⚙️ 控制面板")
        
        # 資料模式選擇
        data_mode = st.radio(
            "資料來源",
            ["🔄 即時資料", "🎭 模擬資料"],
            help="選擇使用即時資料庫資料或模擬資料",
            index=1  # 預設選擇模擬資料
        )
        
        st.markdown("---")
        
        # 時間範圍設定
        st.markdown("### 📅 時間範圍")
        
        date_range = st.selectbox(
            "選擇範圍",
            ["今天", "本週", "本月", "自訂範圍"],
            index=1  # 預設選擇本週
        )
        
        # 如果選擇自訂範圍
        start_date = None
        end_date = None
        
        if date_range == "自訂範圍":
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "開始日期",
                    value=date.today() - timedelta(days=7)
                )
            with col2:
                end_date = st.date_input(
                    "結束日期",
                    value=date.today()
                )
        
        st.markdown("---")
        
        # 顯示選項
        st.markdown("### 🎨 顯示選項")
        
        show_metrics = st.checkbox("顯示統計數據", value=True)
        show_charts = st.checkbox("顯示圖表", value=True)
        
        # 圖表選項
        if show_charts:
            chart_type = st.selectbox(
                "圖表類型",
                ["標準", "簡化", "詳細"],
                help="選擇圖表的詳細程度"
            )
        else:
            chart_type = "標準"
        
        st.markdown("---")
        
        # 系統操作
        st.markdown("### 🔧 系統操作")
        
        if st.button("🔄 重新整理資料"):
            st.cache_data.clear()
            st.success("✅ 資料已重新整理")
            st.rerun()
        
        # 自動重新整理
        auto_refresh = st.checkbox(
            "⏰ 自動重新整理",
            help="每30秒自動更新資料",
            value=False
        )
        
        if auto_refresh:
            st.info("🔄 自動重新整理已啟用")
        
        st.markdown("---")
        
        # 幫助資訊
        with st.expander("💡 使用說明"):
            st.markdown("""
            **資料來源**
            - 🔄 即時資料: 從你的資料庫讀取
            - 🎭 模擬資料: 用於測試和展示
            
            **時間範圍**
            - 選擇要分析的時間區間
            - 自訂範圍可設定特定日期
            
            **顯示選項**
            - 可以選擇顯示或隱藏特定區塊
            - 圖表類型影響顯示的詳細程度
            """)
        
        # 系統資訊
        st.markdown("---")
        st.markdown("### 📊 系統資訊")
        st.caption(f"資料模式: {data_mode}")
        st.caption(f"時間範圍: {date_range}")
        if data_mode == "🎭 模擬資料":
            st.caption("⚠️ 當前使用模擬資料")
    
    # 返回側邊欄狀態
    return {
        'data_mode': data_mode,
        'date_range': date_range,
        'start_date': start_date,
        'end_date': end_date,
        'show_metrics': show_metrics,
        'show_charts': show_charts,
        'chart_type': chart_type,
        'auto_refresh': auto_refresh
    }