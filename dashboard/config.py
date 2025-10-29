#!/usr/bin/env python3
"""
🎵 Dashboard 設定檔
管理 Dashboard 的各種設定參數
"""

import os
from datetime import datetime
from dotenv import load_dotenv

# 載入環境變數
load_dotenv('.env.dashboard')

class DashboardConfig:
    """Dashboard 設定類別"""
    
    # 基本設定
    PAGE_TITLE = "🎵 我的音樂分析"
    PAGE_ICON = "🎵"
    LAYOUT = "wide"
    
    # 顏色主題
    PRIMARY_COLOR = "#1f77b4"
    BACKGROUND_COLOR = "#ffffff"
    SECONDARY_BG_COLOR = "#f0f2f6"
    TEXT_COLOR = "#262730"
    
    # 資料庫設定
    SUPABASE_URL = os.getenv('SUPABASE_DB_URL', '')
    MONGODB_URL = os.getenv('MONGODB_ATLAS_URL', '')
    
    # Dashboard 設定
    AUTO_REFRESH_INTERVAL = int(os.getenv('AUTO_REFRESH_INTERVAL', 30))
    CACHE_TTL = 300  # 5分鐘快取
    DEFAULT_DATE_RANGE = 7  # 預設顯示7天資料
    
    # 圖表設定
    CHART_HEIGHT = 400
    CHART_COLOR_SEQUENCE = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", 
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
    ]
    
    def get_custom_css(self):
        """取得自定義 CSS 樣式"""
        return f"""
        <style>
            /* 主要樣式 */
            .stApp {{
                color: {self.TEXT_COLOR};
            }}
            
            /* 隱藏側邊欄 */
            .css-1d391kg {{
                display: none;
            }}
            
            /* 標題樣式 */
            h1 {{
                color: {self.PRIMARY_COLOR};
                text-align: center;
                margin-bottom: 2rem;
            }}
            
            h2 {{
                color: {self.PRIMARY_COLOR};
                border-bottom: 2px solid {self.PRIMARY_COLOR};
                padding-bottom: 0.5rem;
            }}
            
            /* 統計卡片樣式 */
            .metric-card {{
                background-color: {self.SECONDARY_BG_COLOR};
                padding: 1rem;
                border-radius: 0.5rem;
                border-left: 0.25rem solid {self.PRIMARY_COLOR};
                margin: 0.5rem 0;
            }}
            
            /* 隱藏 Streamlit 標記 */
            #MainMenu {{visibility: hidden;}}
            footer {{visibility: hidden;}}
            header {{visibility: hidden;}}
            
            /* 控制面板樣式 */
            .streamlit-expanderHeader {{
                background-color: {self.SECONDARY_BG_COLOR};
                border-radius: 0.5rem;
            }}
            
            /* 響應式設計 */
            @media (max-width: 768px) {{
                .stApp {{
                    padding: 1rem 0.5rem;
                }}
                
                h1 {{
                    font-size: 1.5rem;
                }}
            }}
            
            /* 美化按鈕 */
            .stButton button {{
                background-color: {self.PRIMARY_COLOR};
                color: white;
                border: none;
                border-radius: 0.5rem;
                padding: 0.5rem 1rem;
                transition: all 0.3s ease;
            }}
            
            .stButton button:hover {{
                background-color: #0d5aa7;
                transform: translateY(-2px);
            }}
        </style>
        """
    
    def get_current_time(self):
        """取得當前時間字串"""
        return datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    
    def is_demo_mode(self):
        """檢查是否為 Demo 模式"""
        return not bool(self.SUPABASE_URL)
    
    def get_db_config(self):
        """取得資料庫設定"""
        return {
            'supabase_url': self.SUPABASE_URL,
            'mongodb_url': self.MONGODB_URL,
            'is_demo': self.is_demo_mode()
        }
    
    def get_chart_config(self):
        """取得圖表設定"""
        return {
            'height': self.CHART_HEIGHT,
            'color_sequence': self.CHART_COLOR_SEQUENCE,
            'theme': {
                'base': 'light',
                'primaryColor': self.PRIMARY_COLOR,
                'backgroundColor': self.BACKGROUND_COLOR,
                'secondaryBackgroundColor': self.SECONDARY_BG_COLOR,
                'textColor': self.TEXT_COLOR
            }
        }

# 全域設定實例
config = DashboardConfig()