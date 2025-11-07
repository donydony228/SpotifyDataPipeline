#!/usr/bin/env python3
"""
🎵 Supabase 資料庫連線管理器 - 簡化版本
使用純 psycopg2，避免 SQLAlchemy 相容性問題
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from dotenv import load_dotenv
import warnings
import numpy as np

# 載入環境變數
load_dotenv()

class SupabaseManager:
    """
    Supabase PostgreSQL 資料庫管理器 - 簡化版
    """
    
    def __init__(self):
        """初始化資料庫連線參數"""
        self.connection_string = os.getenv('SUPABASE_DB_URL')
        
        if not self.connection_string:
            self.db_config = {
                'host': os.getenv('SUPABASE_HOST', 'localhost'),
                'port': os.getenv('SUPABASE_PORT', '5432'),
                'database': os.getenv('SUPABASE_DATABASE', 'postgres'),
                'user': os.getenv('SUPABASE_USER', 'postgres'),
                'password': os.getenv('SUPABASE_PASSWORD', '')
            }
        
        self.connection = None
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """設定日誌記錄器"""
        logger = logging.getLogger('SupabaseManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def connect(self):
        """建立資料庫連線"""
        try:
            if self.connection_string:
                self.connection = psycopg2.connect(
                    self.connection_string,
                    cursor_factory=RealDictCursor
                )
            else:
                self.connection = psycopg2.connect(
                    **self.db_config,
                    cursor_factory=RealDictCursor
                )
            
            # self.logger.info("✅ 成功連接到 Supabase")
            return True
            
        except Exception as e:
            # self.logger.error(f"❌ 連接資料庫失敗: {e}")
            return False
    
    def disconnect(self):
        """關閉資料庫連線"""
        if self.connection:
            self.connection.close()
            self.connection = None
            # self.logger.info("🔌 已關閉資料庫連線")
    
    def test_connection(self):
        """測試資料庫連線"""
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                
                if result and result['test'] == 1:
                    # self.logger.info("✅ 資料庫連線測試成功")
                    return True
                else:
                    # self.logger.error("❌ 資料庫連線測試失敗")
                    return False
                    
        except Exception as e:
            self.logger.error(f"❌ 連線測試錯誤: {e}")
            return False
        finally:
            self.disconnect()
    
    # def execute_query(self, query, params=None):
    #     """
    #     執行 SQL 查詢並回傳 DataFrame
    #     """
    #     if not self.connect():
    #         return pd.DataFrame()
        
    #     try:
    #         # 抑制 pandas 警告
    #         with warnings.catch_warnings():
    #             warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
    #             df = pd.read_sql_query(query, self.connection, params=params)
                
    #         self.logger.info(f"✅ 查詢成功，回傳 {len(df)} 筆資料")
    #         return df
            
    #     except Exception as e:
    #         self.logger.error(f"❌ 查詢執行失敗: {e}")
    #         return pd.DataFrame()
    #     finally:
    #         self.disconnect()
    
    def execute_query(self, query, params=None):
        if not self.connect():
            return pd.DataFrame()
        
        try:
            # print(f"執行的查詢: {query}")
            
            # 先用 cursor 直接查詢，確認原始結果
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # print(f"原始查詢結果: {results}")
                # print(f"欄位名稱: {columns}")
                
                # 手動建立 DataFrame
                df = pd.DataFrame(results, columns=columns)
            
            # print(f"DataFrame 結果: {df}")
            # print(f"資料類型: {df.dtypes}")
            
            # self.logger.info(f"✅ 查詢成功，回傳 {len(df)} 筆資料")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 查詢執行失敗: {e}")
            return pd.DataFrame()
        finally:
            self.disconnect()

    # def get_hourly_listening_data(self, days_back=30):
    #     """獲取每小時聆聽統計資料"""
        
    #     # 最簡單的查詢 - 先試 raw_staging
    #     query = """
    #     SELECT 
    #         EXTRACT(HOUR FROM played_at) as hour,
    #         EXTRACT(DOW FROM played_at) as day_of_week,
    #         COUNT(*) as listening_count,
    #         SUM(COALESCE(duration_ms, 0)) / 60000.0 as total_minutes
    #     FROM raw_staging.spotify_listening_raw 
    #     WHERE played_at >= CURRENT_DATE - INTERVAL '%s days'
    #     GROUP BY 1, 2
    #     ORDER BY 2, 1
    #     """
        
    #     df = self.execute_query(query, (days_back,))
        
    #     if df.empty:
    #         self.logger.warning("⚠️ 沒有找到聆聽資料，回傳模擬資料")
    #         return self._generate_mock_hourly_data()
        
    #     # 檢查資料內容（調試用）
    #     self.logger.info(f"查詢回傳的欄位: {list(df.columns)}")
    #     if not df.empty:
    #         self.logger.info(f"第一筆資料: {df.iloc[0].to_dict()}")
        
    #     # 檢查是否回傳的是欄位名稱而非數據
    #     if 'hour' in df.values or df.iloc[0]['hour'] == 'hour':
    #         self.logger.warning("⚠️ 查詢回傳欄位名稱，使用模擬資料")
    #         return self._generate_mock_hourly_data()
        
    #     try:
    #         # 確保資料類型正確
    #         df['hour'] = pd.to_numeric(df['hour'], errors='coerce').astype(int)
    #         df['day_of_week'] = pd.to_numeric(df['day_of_week'], errors='coerce').astype(int)
    #         df['listening_count'] = pd.to_numeric(df['listening_count'], errors='coerce').astype(int)
    #         df['total_minutes'] = pd.to_numeric(df['total_minutes'], errors='coerce').astype(float)
            
    #         # 移除任何無效資料
    #         df = df.dropna()
            
    #         if df.empty:
    #             self.logger.warning("⚠️ 資料轉換後為空，使用模擬資料")
    #             return self._generate_mock_hourly_data()
                
    #     except Exception as e:
    #         self.logger.error(f"❌ 資料類型轉換失敗: {e}")
    #         return self._generate_mock_hourly_data()
        
    #     return df
    
    # def get_recent_stats(self, days=7):
    #     """獲取最近幾天的聆聽統計"""
        
    #     query = """
    #     SELECT 
    #         COUNT(*) as total_tracks,
    #         COUNT(DISTINCT track_id) as unique_tracks,
    #         COUNT(DISTINCT artist_name) as unique_artists,
    #         AVG(EXTRACT(HOUR FROM played_at)) as avg_hour
    #     FROM raw_staging.spotify_listening_raw 
    #     WHERE played_at >= CURRENT_DATE - INTERVAL '%s days'
    #     """
        
    #     df = self.execute_query(query, (days,))
        
    #     if df.empty or df.iloc[0]['total_tracks'] == 0:
    #         return {
    #             'total_tracks': 156,
    #             'unique_tracks': 89,
    #             'unique_artists': 34,
    #             'avg_hour': 15.3,
    #             'is_mock_data': True
    #         }
        
    #     # 檢查是否回傳的是欄位名稱
    #     if df.iloc[0]['total_tracks'] == 'total_tracks':
    #         self.logger.warning("⚠️ 統計查詢回傳欄位名稱，使用模擬資料")
    #         return {
    #             'total_tracks': 156,
    #             'unique_tracks': 89,
    #             'unique_artists': 34,
    #             'avg_hour': 15.3,
    #             'is_mock_data': True
    #         }
        
    #     stats = df.iloc[0].to_dict()
        
    #     try:
    #         stats['total_tracks'] = int(pd.to_numeric(stats['total_tracks'], errors='coerce') or 0)
    #         stats['unique_tracks'] = int(pd.to_numeric(stats['unique_tracks'], errors='coerce') or 0)
    #         stats['unique_artists'] = int(pd.to_numeric(stats['unique_artists'], errors='coerce') or 0)
    #         stats['avg_hour'] = float(pd.to_numeric(stats['avg_hour'], errors='coerce') or 12.0)
    #         stats['is_mock_data'] = False
    #     except Exception as e:
    #         self.logger.error(f"❌ 統計資料轉換失敗: {e}")
    #         return {
    #             'total_tracks': 156,
    #             'unique_tracks': 89,
    #             'unique_artists': 34,
    #             'avg_hour': 15.3,
    #             'is_mock_data': True
    #         }
        
    #     return stats
    
    # def _generate_mock_hourly_data(self):
    #     """生成模擬資料"""
    #     np.random.seed(42)
    #     data = []
        
    #     for day in range(7):
    #         for hour in range(24):
    #             if day in [0, 6]:  # 週末
    #                 base_prob = 0.7
    #             else:  # 平日
    #                 base_prob = 0.5
                
    #             if 6 <= hour <= 9:
    #                 time_multiplier = 0.8
    #             elif 10 <= hour <= 17:
    #                 time_multiplier = 1.2
    #             elif 18 <= hour <= 23:
    #                 time_multiplier = 1.8
    #             else:
    #                 time_multiplier = 0.3
                
    #             prob = base_prob * time_multiplier
    #             listening_count = int(np.random.poisson(prob * 10))
    #             total_minutes = listening_count * 3.5
                
    #             data.append({
    #                 'hour': hour,
    #                 'day_of_week': day,
    #                 'listening_count': listening_count,
    #                 'total_minutes': total_minutes
    #             })
        
    #     return pd.DataFrame(data)

# 測試功能
# if __name__ == "__main__":
#     db = SupabaseManager()
    
#     # 測試連線
#     if db.test_connection():
#         print("資料庫連線成功！")
        
#         # 測試資料獲取
#         query = """
#             select track_name, COUNT(track_name) as Freq
#             from dwh.fact_listening as f
#             left join dwh.dim_tracks as t
#             on f.track_key = t.track_key
#             -- where f.played_at > "20251025"
#             group by track_name
#             order by COUNT(track_name) desc
#             """
#         df = db.execute_query(query)
#         print("查詢結果：")
#         print(df)
        
#     else:
#         print("資料庫連線失敗")