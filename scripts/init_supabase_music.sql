-- ============================================================================
-- PostgreSQL (Supabase) - 音樂分析資料庫設計 (全新專案版本)
-- 目標: 結構化儲存,方便分析與查詢
-- 適用: 全新的 Supabase 專案
-- ============================================================================

-- ============================================================================
-- 階段 0: 建立 Schema (確保存在)
-- ============================================================================

-- 建立必要的 Schema
CREATE SCHEMA IF NOT EXISTS raw_staging;
CREATE SCHEMA IF NOT EXISTS clean_staging;
CREATE SCHEMA IF NOT EXISTS business_staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- 設定 Schema 註解
COMMENT ON SCHEMA raw_staging IS '原始資料暫存區 - Spotify API 直接資料';
COMMENT ON SCHEMA clean_staging IS '清洗資料暫存區 - 標準化與驗證';
COMMENT ON SCHEMA business_staging IS '業務資料暫存區 - 業務邏輯處理';
COMMENT ON SCHEMA dwh IS '資料倉儲 - Star Schema 分析結構';

-- ============================================================================
-- 階段 1: Raw Staging - 每日聽歌記錄
-- ============================================================================

CREATE TABLE raw_staging.spotify_listening_history (
    -- 主鍵
    id BIGSERIAL PRIMARY KEY,
    
    -- Spotify 識別碼
    track_id VARCHAR(100) NOT NULL,
    
    -- 播放時間 (最重要!)
    played_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 歌曲基本資訊
    track_name TEXT NOT NULL,
    artist_names TEXT NOT NULL,  -- 多個藝術家用逗號分隔
    album_name TEXT,
    
    -- 歌曲屬性
    duration_ms INTEGER,
    explicit BOOLEAN DEFAULT FALSE,
    popularity INTEGER,
    
    -- 批次追蹤
    batch_id VARCHAR(50) NOT NULL,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 完整的 JSON 資料 (備份)
    raw_json JSONB,
    
    -- 建立時間
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 避免重複: 同一首歌在同一時間只記錄一次
    CONSTRAINT unique_play UNIQUE (track_id, played_at)
);

-- 索引優化
CREATE INDEX idx_spotify_played_at ON raw_staging.spotify_listening_history(played_at DESC);
CREATE INDEX idx_spotify_track_id ON raw_staging.spotify_listening_history(track_id);
CREATE INDEX idx_spotify_batch_id ON raw_staging.spotify_listening_history(batch_id);
CREATE INDEX idx_spotify_artist ON raw_staging.spotify_listening_history USING gin(to_tsvector('english', artist_names));

-- 註解
COMMENT ON TABLE raw_staging.spotify_listening_history IS '每日聽歌記錄 - 從 Spotify Recently Played API 獲取';
COMMENT ON COLUMN raw_staging.spotify_listening_history.played_at IS '播放時間 - 帶時區';
COMMENT ON COLUMN raw_staging.spotify_listening_history.track_id IS 'Spotify Track ID';
COMMENT ON COLUMN raw_staging.spotify_listening_history.raw_json IS '完整的 Spotify API 回應資料';

-- ============================================================================
-- 階段 2: Clean Staging - 清洗與標準化
-- ============================================================================

CREATE TABLE clean_staging.listening_cleaned (
    id BIGSERIAL PRIMARY KEY,
    
    -- 關聯到原始資料
    raw_id BIGINT REFERENCES raw_staging.spotify_listening_history(id),
    
    -- 清洗後的資料
    track_id VARCHAR(100) NOT NULL,
    played_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 標準化的時間欄位 (方便分析)
    played_date DATE NOT NULL,
    played_hour INTEGER NOT NULL CHECK (played_hour >= 0 AND played_hour <= 23),
    played_day_of_week INTEGER NOT NULL CHECK (played_day_of_week >= 0 AND played_day_of_week <= 6),
    -- 0 = Monday, 6 = Sunday
    
    -- 歌曲資訊
    track_name TEXT NOT NULL,
    primary_artist VARCHAR(200),  -- 主要藝術家
    album_name TEXT,
    
    -- 分類標記
    is_explicit BOOLEAN DEFAULT FALSE,
    is_weekend BOOLEAN DEFAULT FALSE,
    time_of_day VARCHAR(20),  -- morning, afternoon, evening, night
    
    -- 資料品質
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    quality_flags TEXT[],  -- 資料品質標記陣列
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_clean_played_date ON clean_staging.listening_cleaned(played_date DESC);
CREATE INDEX idx_clean_track_id ON clean_staging.listening_cleaned(track_id);
CREATE INDEX idx_clean_time_of_day ON clean_staging.listening_cleaned(time_of_day);

COMMENT ON TABLE clean_staging.listening_cleaned IS '清洗後的聽歌記錄 - 標準化時間與分類';
COMMENT ON COLUMN clean_staging.listening_cleaned.played_day_of_week IS '星期幾 (0=週一, 6=週日)';
COMMENT ON COLUMN clean_staging.listening_cleaned.time_of_day IS '時段分類: morning/afternoon/evening/night';

-- ============================================================================
-- 階段 3: Business Staging - 業務邏輯處理
-- ============================================================================

CREATE TABLE business_staging.listening_final (
    id BIGSERIAL PRIMARY KEY,
    
    -- 關聯
    clean_id BIGINT REFERENCES clean_staging.listening_cleaned(id),
    
    -- 核心欄位
    track_id VARCHAR(100) NOT NULL,
    played_at TIMESTAMP WITH TIME ZONE NOT NULL,
    played_date DATE NOT NULL,
    
    -- 歌曲資訊
    track_name TEXT NOT NULL,
    artist_name VARCHAR(200),
    album_name TEXT,
    
    -- 時間分析欄位
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    time_period VARCHAR(20),  -- 時段標記
    
    -- 統計標記
    is_repeat_listen BOOLEAN DEFAULT FALSE,  -- 是否重複聽
    play_sequence_today INTEGER,  -- 當天第幾首歌
    daily_play_count INTEGER DEFAULT 1,  -- 今日播放次數
    
    -- 準備載入 DWH
    ready_for_dwh BOOLEAN DEFAULT FALSE,
    dwh_loaded_at TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_business_played_date ON business_staging.listening_final(played_date DESC);
CREATE INDEX idx_business_ready_dwh ON business_staging.listening_final(ready_for_dwh);
CREATE INDEX idx_business_track_date ON business_staging.listening_final(track_id, played_date);

COMMENT ON TABLE business_staging.listening_final IS '業務處理後的聽歌記錄 - 準備進入資料倉儲';

-- ============================================================================
-- 階段 4: Data Warehouse - Star Schema (分析專用)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 維度表 1: 日期維度 (時間序列分析必備)
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.dim_dates (
    date_key INTEGER PRIMARY KEY,  -- 格式: 20250115
    full_date DATE NOT NULL UNIQUE,
    
    -- 年份資訊
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    
    -- 日期資訊
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    
    -- 中文資訊 (可選)
    month_name_zh VARCHAR(10),
    day_name_zh VARCHAR(10),
    
    -- 標記
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    season VARCHAR(10), -- Spring, Summer, Autumn, Winter
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_dates_year_month ON dwh.dim_dates(year, month);
CREATE INDEX idx_dim_dates_quarter ON dwh.dim_dates(year, quarter);

COMMENT ON TABLE dwh.dim_dates IS '日期維度表 - 支援時間序列分析';
COMMENT ON COLUMN dwh.dim_dates.date_key IS '日期鍵值 (YYYYMMDD 格式)';

-- ----------------------------------------------------------------------------
-- 維度表 2: 歌曲維度
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.dim_tracks (
    track_key SERIAL PRIMARY KEY,
    track_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- 基本資訊
    track_name TEXT NOT NULL,
    duration_ms INTEGER,
    explicit BOOLEAN DEFAULT FALSE,
    popularity INTEGER,
    
    -- 音訊特徵 (未來從 Audio Features API 獲取)
    danceability DECIMAL(4,3),
    energy DECIMAL(4,3),
    valence DECIMAL(4,3),  -- 音樂快樂程度
    tempo DECIMAL(6,2),
    loudness DECIMAL(6,3),
    speechiness DECIMAL(4,3),
    acousticness DECIMAL(4,3),
    instrumentalness DECIMAL(4,3),
    liveness DECIMAL(4,3),
    
    -- 分類
    key INTEGER,  -- 音調
    mode INTEGER, -- 0 = minor, 1 = major
    time_signature INTEGER,
    
    -- 元資料
    first_seen DATE NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_plays INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_tracks_name ON dwh.dim_tracks USING gin(to_tsvector('english', track_name));
CREATE INDEX idx_dim_tracks_popularity ON dwh.dim_tracks(popularity DESC);
CREATE INDEX idx_dim_tracks_valence ON dwh.dim_tracks(valence);

COMMENT ON TABLE dwh.dim_tracks IS '歌曲維度表 - 包含音訊特徵';
COMMENT ON COLUMN dwh.dim_tracks.valence IS '音樂情緒指標 - 0(悲傷) 到 1(快樂)';
COMMENT ON COLUMN dwh.dim_tracks.energy IS '音樂能量指標 - 0(低能量) 到 1(高能量)';

-- ----------------------------------------------------------------------------
-- 維度表 3: 藝術家維度
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.dim_artists (
    artist_key SERIAL PRIMARY KEY,
    artist_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- 基本資訊
    artist_name VARCHAR(200) NOT NULL,
    genres TEXT[],  -- PostgreSQL 陣列
    popularity INTEGER,
    followers_count INTEGER,
    
    -- 元資料
    first_seen DATE NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_plays INTEGER DEFAULT 0,
    unique_tracks INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_artists_name ON dwh.dim_artists USING gin(to_tsvector('english', artist_name));
CREATE INDEX idx_dim_artists_popularity ON dwh.dim_artists(popularity DESC);
CREATE INDEX idx_dim_artists_genres ON dwh.dim_artists USING gin(genres);

COMMENT ON TABLE dwh.dim_artists IS '藝術家維度表';

-- ----------------------------------------------------------------------------
-- 維度表 4: 專輯維度
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.dim_albums (
    album_key SERIAL PRIMARY KEY,
    album_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- 基本資訊
    album_name TEXT NOT NULL,
    release_date DATE,
    total_tracks INTEGER,
    album_type VARCHAR(20), -- album, single, compilation
    
    -- 元資料
    first_seen DATE NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_plays INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_albums_name ON dwh.dim_albums USING gin(to_tsvector('english', album_name));
CREATE INDEX idx_dim_albums_release_date ON dwh.dim_albums(release_date DESC);

COMMENT ON TABLE dwh.dim_albums IS '專輯維度表';

-- ----------------------------------------------------------------------------
-- 事實表: 聽歌記錄事實表
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.fact_listening (
    listening_key BIGSERIAL PRIMARY KEY,
    
    -- 維度外鍵
    date_key INTEGER REFERENCES dwh.dim_dates(date_key),
    track_key INTEGER REFERENCES dwh.dim_tracks(track_key),
    artist_key INTEGER REFERENCES dwh.dim_artists(artist_key),
    album_key INTEGER REFERENCES dwh.dim_albums(album_key),
    
    -- 播放時間
    played_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 時間細節
    hour_of_day INTEGER,
    minute_of_hour INTEGER,
    
    -- 度量值
    duration_played_ms INTEGER,  -- 實際播放時長 (未來可能獲取)
    completion_rate DECIMAL(4,3), -- 播放完成度 (0-1)
    
    -- 標記
    is_explicit BOOLEAN,
    is_repeat_today BOOLEAN,
    is_skip BOOLEAN DEFAULT FALSE, -- 是否跳過
    
    -- 批次追蹤
    batch_id VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 優化查詢的索引
CREATE INDEX idx_fact_listening_date ON dwh.fact_listening(date_key);
CREATE INDEX idx_fact_listening_track ON dwh.fact_listening(track_key);
CREATE INDEX idx_fact_listening_artist ON dwh.fact_listening(artist_key);
CREATE INDEX idx_fact_listening_played_at ON dwh.fact_listening(played_at DESC);
CREATE INDEX idx_fact_listening_hour ON dwh.fact_listening(hour_of_day);

COMMENT ON TABLE dwh.fact_listening IS '聽歌記錄事實表 - Star Schema 核心';

-- ============================================================================
-- 階段 5: 聚合表 (預計算,加速查詢)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 每日統計表
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.agg_daily_stats (
    stat_date DATE PRIMARY KEY,
    
    -- 基本統計
    total_plays INTEGER NOT NULL DEFAULT 0,
    unique_tracks INTEGER NOT NULL DEFAULT 0,
    unique_artists INTEGER NOT NULL DEFAULT 0,
    unique_albums INTEGER NOT NULL DEFAULT 0,
    
    -- 時間分布
    morning_plays INTEGER DEFAULT 0,    -- 6-12
    afternoon_plays INTEGER DEFAULT 0,  -- 12-18
    evening_plays INTEGER DEFAULT 0,    -- 18-24
    night_plays INTEGER DEFAULT 0,      -- 0-6
    
    -- 音樂特徵平均值
    avg_energy DECIMAL(4,3),
    avg_valence DECIMAL(4,3),
    avg_danceability DECIMAL(4,3),
    avg_tempo DECIMAL(6,2),
    
    -- 最愛歌曲 (當天)
    top_track_id VARCHAR(100),
    top_track_name TEXT,
    top_track_plays INTEGER,
    
    -- 最愛藝術家 (當天)
    top_artist_id VARCHAR(100),
    top_artist_name VARCHAR(200),
    top_artist_plays INTEGER,
    
    -- 更新時間
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_daily_stats_date ON dwh.agg_daily_stats(stat_date DESC);

COMMENT ON TABLE dwh.agg_daily_stats IS '每日聽歌統計 - 預計算表';

-- ----------------------------------------------------------------------------
-- 每週統計表
-- ----------------------------------------------------------------------------

CREATE TABLE dwh.agg_weekly_stats (
    week_start_date DATE PRIMARY KEY,
    week_number INTEGER NOT NULL,
    year INTEGER NOT NULL,
    
    -- 基本統計
    total_plays INTEGER NOT NULL DEFAULT 0,
    unique_tracks INTEGER NOT NULL DEFAULT 0,
    unique_artists INTEGER NOT NULL DEFAULT 0,
    
    -- 平均值
    avg_daily_plays DECIMAL(6,2),
    avg_energy DECIMAL(4,3),
    avg_valence DECIMAL(4,3),
    
    -- 聽歌時段偏好
    preferred_time_slot VARCHAR(20), -- morning/afternoon/evening/night
    weekend_vs_weekday_ratio DECIMAL(4,2),
    
    -- Top 5 歌曲與藝術家 (JSON 格式)
    top_tracks JSONB,  -- [{track_id, track_name, plays, artist_name}]
    top_artists JSONB, -- [{artist_id, artist_name, plays, track_count}]
    
    -- 音樂風格分析
    dominant_mood VARCHAR(20), -- happy, sad, energetic, calm
    genre_distribution JSONB,
    
    -- 更新時間
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_weekly_stats_year ON dwh.agg_weekly_stats(year DESC, week_number DESC);

COMMENT ON TABLE dwh.agg_weekly_stats IS '每週聽歌統計 - 用於週報生成';

-- ============================================================================
-- 階段 6: 系統管理表
-- ============================================================================

CREATE TABLE dwh.etl_batch_log (
    batch_id VARCHAR(50) PRIMARY KEY,
    execution_date DATE NOT NULL,
    
    -- 執行狀態
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'partial')),
    
    -- 統計
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    
    -- API 呼叫統計
    api_calls_made INTEGER DEFAULT 0,
    api_rate_limit_hits INTEGER DEFAULT 0,
    
    -- 時間
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    
    -- 錯誤訊息
    error_message TEXT,
    error_details JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_execution_date ON dwh.etl_batch_log(execution_date DESC);
CREATE INDEX idx_etl_status ON dwh.etl_batch_log(status);

COMMENT ON TABLE dwh.etl_batch_log IS 'ETL 批次執行記錄 - 監控資料載入流程';

-- ============================================================================
-- 階段 7: 預設資料與初始化
-- ============================================================================

-- 建立日期維度資料 (未來一年)
INSERT INTO dwh.dim_dates (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, week_of_year, is_weekend,
    month_name_zh, day_name_zh, season
)
SELECT
    TO_CHAR(d::date, 'YYYYMMDD')::INTEGER as date_key,
    d::date as full_date,
    EXTRACT(YEAR FROM d) as year,
    EXTRACT(QUARTER FROM d) as quarter,
    EXTRACT(MONTH FROM d) as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(DAY FROM d) as day,
    EXTRACT(DOW FROM d) as day_of_week, -- 0=Sunday, 6=Saturday
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(WEEK FROM d) as week_of_year,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN '一月' WHEN 2 THEN '二月' WHEN 3 THEN '三月'
        WHEN 4 THEN '四月' WHEN 5 THEN '五月' WHEN 6 THEN '六月'
        WHEN 7 THEN '七月' WHEN 8 THEN '八月' WHEN 9 THEN '九月'
        WHEN 10 THEN '十月' WHEN 11 THEN '十一月' WHEN 12 THEN '十二月'
    END as month_name_zh,
    CASE EXTRACT(DOW FROM d)
        WHEN 0 THEN '週日' WHEN 1 THEN '週一' WHEN 2 THEN '週二'
        WHEN 3 THEN '週三' WHEN 4 THEN '週四' WHEN 5 THEN '週五'
        WHEN 6 THEN '週六'
    END as day_name_zh,
    CASE 
        WHEN EXTRACT(MONTH FROM d) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6,7,8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (9,10,11) THEN 'Autumn'
        ELSE 'Winter'
    END as season
FROM generate_series(
    CURRENT_DATE - INTERVAL '30 days',
    CURRENT_DATE + INTERVAL '365 days',
    '1 day'::interval
) AS d
ON CONFLICT (full_date) DO NOTHING;

-- 插入測試資料: 一筆聽歌記錄
INSERT INTO raw_staging.spotify_listening_history (
    track_id, played_at, track_name, artist_names, album_name,
    duration_ms, explicit, popularity, batch_id, raw_json
)
VALUES (
    'test_track_init_001',
    CURRENT_TIMESTAMP,
    'Shape of You',
    'Ed Sheeran',
    '÷ (Deluxe)',
    233713,
    FALSE,
    94,
    'init_batch_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD'),
    jsonb_build_object(
        'test', '完整的 Spotify API 回應會存在這裡',
        'created_by', 'database_initialization',
        'timestamp', CURRENT_TIMESTAMP
    )
)
ON CONFLICT (track_id, played_at) DO NOTHING;

-- ============================================================================
-- 實用查詢 VIEW (方便日常使用)
-- ============================================================================

-- 今日聽歌總覽
CREATE OR REPLACE VIEW dwh.v_today_listening AS
SELECT 
    COUNT(*) as total_plays,
    COUNT(DISTINCT track_id) as unique_tracks,
    COUNT(DISTINCT SPLIT_PART(artist_names, ',', 1)) as unique_artists,
    MIN(played_at) as first_play,
    MAX(played_at) as last_play
FROM raw_staging.spotify_listening_history
WHERE played_at::date = CURRENT_DATE;

-- 本週統計概覽
CREATE OR REPLACE VIEW dwh.v_weekly_overview AS
SELECT 
    DATE_TRUNC('week', played_at::date) as week_start,
    COUNT(*) as total_plays,
    COUNT(DISTINCT track_id) as unique_tracks,
    COUNT(DISTINCT SPLIT_PART(artist_names, ',', 1)) as unique_artists,
    ROUND(AVG(duration_ms)/1000.0, 1) as avg_duration_seconds
FROM raw_staging.spotify_listening_history
WHERE played_at >= DATE_TRUNC('week', CURRENT_DATE)
GROUP BY DATE_TRUNC('week', played_at::date)
ORDER BY week_start DESC;

COMMENT ON VIEW dwh.v_today_listening IS '今日聽歌統計概覽';
COMMENT ON VIEW dwh.v_weekly_overview IS '週度聽歌統計概覽';

-- ============================================================================
-- 權限設定 (可選,根據需要調整)
-- ============================================================================

-- 如果有多個使用者,可以設定權限
-- GRANT USAGE ON SCHEMA raw_staging TO airflow_user;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA raw_staging TO airflow_user;

-- ============================================================================
-- 結束統計與訊息
-- ============================================================================

-- 統計新建立的物件
DO $$
DECLARE
    table_count INTEGER;
    index_count INTEGER;
    view_count INTEGER;
    schema_count INTEGER;
BEGIN
    -- 統計表格數量
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema IN ('raw_staging', 'clean_staging', 'business_staging', 'dwh');
    
    -- 統計索引數量
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes
    WHERE schemaname IN ('raw_staging', 'clean_staging', 'business_staging', 'dwh');
    
    -- 統計 View 數量
    SELECT COUNT(*) INTO view_count
    FROM information_schema.views
    WHERE table_schema = 'dwh';
    
    -- 統計 Schema 數量
    SELECT COUNT(*) INTO schema_count
    FROM information_schema.schemata
    WHERE schema_name IN ('raw_staging', 'clean_staging', 'business_staging', 'dwh');
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '🎉 PostgreSQL 音樂資料庫初始化完成!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '📊 建立統計:';
    RAISE NOTICE '  🗄️  Schema: % 個', schema_count;
    RAISE NOTICE '  📋 Table: % 個', table_count;
    RAISE NOTICE '  🔍 Index: % 個', index_count;
    RAISE NOTICE '  👁️  View: % 個', view_count;
    RAISE NOTICE '';
    RAISE NOTICE '📋 資料庫結構:';
    RAISE NOTICE '';
    RAISE NOTICE '  1️⃣  Raw Staging (原始資料):';
    RAISE NOTICE '     └─ spotify_listening_history';
    RAISE NOTICE '';
    RAISE NOTICE '  2️⃣  Clean Staging (清洗資料):';
    RAISE NOTICE '     └─ listening_cleaned';
    RAISE NOTICE '';
    RAISE NOTICE '  3️⃣  Business Staging (業務處理):';
    RAISE NOTICE '     └─ listening_final';
    RAISE NOTICE '';
    RAISE NOTICE '  4️⃣  Data Warehouse (Star Schema):';
    RAISE NOTICE '     ├─ dim_dates (日期維度)';
    RAISE NOTICE '     ├─ dim_tracks (歌曲維度)';
    RAISE NOTICE '     ├─ dim_artists (藝術家維度)';
    RAISE NOTICE '     ├─ dim_albums (專輯維度)';
    RAISE NOTICE '     └─ fact_listening (聽歌事實表)';
    RAISE NOTICE '';
    RAISE NOTICE '  5️⃣  聚合表 (預計算):';
    RAISE NOTICE '     ├─ agg_daily_stats';
    RAISE NOTICE '     └─ agg_weekly_stats';
    RAISE NOTICE '';
    RAISE NOTICE '  6️⃣  系統管理:';
    RAISE NOTICE '     └─ etl_batch_log';
    RAISE NOTICE '';
    RAISE NOTICE '  7️⃣  便民 Views:';
    RAISE NOTICE '     ├─ v_today_listening';
    RAISE NOTICE '     └─ v_weekly_overview';
    RAISE NOTICE '';
    RAISE NOTICE '💡 下一步建議:';
    RAISE NOTICE '  1. 測試基本查詢功能';
    RAISE NOTICE '  2. 建立 Spotify API 客戶端';
    RAISE NOTICE '  3. 建立第一個 Airflow DAG';
    RAISE NOTICE '  4. 測試完整的 ETL 流程';
    RAISE NOTICE '';
    RAISE NOTICE '✅ 資料庫已準備就緒!';
    RAISE NOTICE '';
END $$;

-- 顯示各 Schema 的表格統計
SELECT 
    schemaname AS "Schema",
    COUNT(*) AS "Tables"
FROM pg_tables
WHERE schemaname IN ('raw_staging', 'clean_staging', 'business_staging', 'dwh')
GROUP BY schemaname
ORDER BY 
    CASE schemaname 
        WHEN 'raw_staging' THEN 1
        WHEN 'clean_staging' THEN 2  
        WHEN 'business_staging' THEN 3
        WHEN 'dwh' THEN 4
    END;