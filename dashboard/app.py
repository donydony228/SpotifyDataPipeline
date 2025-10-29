#!/usr/bin/env python3
"""
🎵 音樂分析 Dashboard - 主程式
一個空白的 Streamlit dashboard，讓你自己建立內容

運行方式: streamlit run app.py
"""

import streamlit as st
from config import DashboardConfig
from database import DatabaseManager
from components.metrics import render_metrics_section
from components.charts import render_charts_section

# 初始化設定
config = DashboardConfig()

# 設定頁面配置
st.set_page_config(
    page_title=config.PAGE_TITLE,
    page_icon=config.PAGE_ICON,
    layout=config.LAYOUT,
    initial_sidebar_state="collapsed"  # 收起側邊欄
)

# 套用自定義 CSS
st.markdown(config.get_custom_css(), unsafe_allow_html=True)

def main():
    """主要 Dashboard 函數"""
    
    # 頁面標題
    st.markdown(f"# {config.PAGE_TITLE}")
    st.markdown("---")
    
    # 初始化資料庫
    db = DatabaseManager()
    
    # 簡單的控制選項 (在主區域，不是側邊欄)
    with st.expander("⚙️ 顯示選項", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            show_metrics = st.checkbox("顯示統計數據", value=True, key="show_metrics_main")
        
        with col2:
            show_charts = st.checkbox("顯示圖表", value=True, key="show_charts_main")
        
        with col3:
            if st.button("🔄 重新整理資料", key="refresh_button"):
                st.cache_data.clear()
                st.rerun()
    
    # 建立一個簡單的狀態物件 (替代原來的 sidebar_state)
    display_state = {
        'data_mode': '🎭 模擬資料',
        'date_range': '本週',
        'show_metrics': show_metrics,
        'show_charts': show_charts,
        'chart_type': '標準',
        'auto_refresh': False
    }
    
    # 檢查資料庫連線
    if not db.test_connection():
        st.error("❌ 無法連接資料庫，請檢查設定")
        st.info("💡 你可以在 config.py 中設定連線參數")
        return
    
    # 主要內容區域
    if show_metrics:
        st.markdown("## 📊 資料概覽")
        render_metrics_section(db, display_state)
        st.markdown("---")
    
    # 圖表區域
    if show_charts:
        st.markdown("## 📈 圖表分析")
        render_charts_section(db, display_state)
        st.markdown("---")
    
    # 頁尾
    st.markdown(
        f"<div style='text-align: center; color: #666; font-size: 0.8rem;'>"
        f"🎵 音樂分析系統 | 最後更新: {config.get_current_time()} | 資料模式: {display_state['data_mode']}"
        f"</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()