#!/usr/bin/env python3
"""
🗄️ 資料庫管理器
處理所有資料庫連線和查詢邏輯
"""

import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import random
from datetime import datetime, timedelta
from config import config

class DatabaseManager:
    """資料庫管理類別"""
    
    def __init__(self):
        self.supabase_url = config.SUPABASE_URL
        self.is_demo = config.is_demo_mode()
        self._engine = None
    
    @st.cache_resource
    def get_connection(_self):
        """取得資料庫連線 (快取)"""
        if _self.is_demo:
            return None
        
        try:
            _self._engine = create_engine(_self.supabase_url)
            return _self._engine
        except Exception as e:
            st.error(f"資料庫連線失敗: {e}")
            return None
    
    def test_connection(self):
        """測試資料庫連線"""
        if self.is_demo:
            st.info("🎭 目前使用模擬資料模式")
            return True
        
        try:
            engine = self.get_connection()
            if engine is None:
                return False
            
            # 簡單查詢測試
            with engine.connect() as conn:
                result = conn.execute("SELECT 1")
                result.fetchone()
            
            st.success("✅ 資料庫連線成功")
            return True
            
        except Exception as e:
            st.error(f"❌ 資料庫連線失敗: {e}")
            return False
    
    @st.cache_data(ttl=config.CACHE_TTL)
    def get_daily_stats(_self):
        """取得每日統計資料"""
        if _self.is_demo:
            return _self._generate_mock_daily_stats()
        
        try:
            engine = _self.get_connection()
            query = """
            SELECT 
                COUNT(*) as total_tracks,
                SUM(listening_minutes) as total_minutes,
                COUNT(DISTINCT artist_key) as unique_artists,
                COUNT(DISTINCT track_key) as unique_tracks
            FROM dwh.fact_listening f
            JOIN dwh.dim_dates d ON f.date_key = d.date_key
            WHERE d.date_value = CURRENT_DATE
            """
            
            result = pd.read_sql(query, engine)
            return result.iloc[0].to_dict() if not result.empty else _self._generate_mock_daily_stats()
            
        except Exception as e:
            st.warning(f"查詢失敗，使用模擬資料: {e}")
            return _self._generate_mock_daily_stats()
    
    @st.cache_data(ttl=config.CACHE_TTL)
    def get_weekly_trends(_self):
        """取得週趨勢資料"""
        if _self.is_demo:
            return _self._generate_mock_weekly_data()
        
        try:
            engine = _self.get_connection()
            query = """
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
            
            result = pd.read_sql(query, engine)
            return result if not result.empty else _self._generate_mock_weekly_data()
            
        except Exception as e:
            st.warning(f"查詢失敗，使用模擬資料: {e}")
            return _self._generate_mock_weekly_data()
    
    def _generate_mock_daily_stats(self):
        """產生模擬每日統計"""
        return {
            'total_tracks': random.randint(30, 80),
            'total_minutes': random.randint(120, 300),
            'unique_artists': random.randint(15, 30),
            'unique_tracks': random.randint(25, 60)
        }
    
    def _generate_mock_weekly_data(self):
        """產生模擬週資料"""
        dates = pd.date_range(end=datetime.now(), periods=7)
        return pd.DataFrame({
            'date': dates,
            'tracks_count': [random.randint(20, 60) for _ in range(7)],
            'total_minutes': [random.randint(80, 200) for _ in range(7)]
        })
    
    @st.cache_data(ttl=config.CACHE_TTL)
    def get_top_artists(_self, limit=5):
        """取得最愛藝人"""
        if _self.is_demo:
            artists = ['Taylor Swift', '周杰倫', 'Post Malone', 'Ed Sheeran', '鄧紫棋', 
                      'Olivia Rodrigo', '林俊傑', 'Billie Eilish', '蔡依林', 'The Weeknd']
            return pd.DataFrame({
                'artist': random.sample(artists, limit),
                'play_count': sorted([random.randint(5, 25) for _ in range(limit)], reverse=True)
            })
        
        try:
            engine = _self.get_connection()
            query = f"""
            SELECT 
                a.artist_name as artist,
                COUNT(*) as play_count
            FROM dwh.fact_listening f
            JOIN dwh.dim_artists a ON f.artist_key = a.artist_key
            JOIN dwh.dim_dates d ON f.date_key = d.date_key
            WHERE d.date_value >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY a.artist_name
            ORDER BY play_count DESC
            LIMIT {limit}
            """
            
            result = pd.read_sql(query, engine)
            return result if not result.empty else _self.get_top_artists(limit)  # 遞迴回到模擬資料
            
        except Exception as e:
            st.warning(f"查詢失敗，使用模擬資料: {e}")
            return _self.get_top_artists(limit)  # 遞迴回到模擬資料
    
    def clear_cache(self):
        """清除所有快取"""
        st.cache_data.clear()
        st.success("✅ 快取已清除")