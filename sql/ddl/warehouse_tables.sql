-- 美國求職市場資料工程專案 - PostgreSQL Schema
-- 架構：Raw Staging → Clean Staging → Business Staging → Data Warehouse

-- ============================================================================
-- Schema 建立
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw_staging;
CREATE SCHEMA IF NOT EXISTS clean_staging;
CREATE SCHEMA IF NOT EXISTS business_staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- ============================================================================
-- Raw Staging Tables - 各來源原始資料
-- ============================================================================

-- LinkedIn 原始資料表
CREATE TABLE raw_staging.linkedin_jobs_raw (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- 來源資訊
    source_job_id VARCHAR(100),           -- LinkedIn 的職缺 ID
    source_url TEXT,                      -- 原始職缺連結
    
    -- 基本職缺資訊
    job_title TEXT,                       -- 職位名稱
    company_name TEXT,                    -- 公司名稱
    location_raw TEXT,                    -- 原始地點資訊
    job_description TEXT,                 -- 職位描述
    employment_type TEXT,                 -- 工作型態 (full-time, part-time, contract)
    work_arrangement TEXT,                -- 工作安排 (remote, hybrid, onsite)
    
    -- 行業與技能 (原始格式)
    industry_raw TEXT,                    -- 原始行業資訊
    skills_raw TEXT,                      -- 原始技能要求文字
    requirements_raw TEXT,                -- 原始職位要求
    
    -- 時間資訊
    posted_date DATE,                     -- 職缺發布日期
    scraped_at TIMESTAMP NOT NULL,       -- 爬取時間
    
    -- 完整原始資料
    raw_json JSONB,                       -- 完整的原始 JSON 資料
    
    -- 批次控制
    batch_id UUID NOT NULL,
    data_quality_flags TEXT[],            -- 資料品質標記
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 避免重複
    UNIQUE(source_job_id, batch_id)
);

-- Indeed 原始資料表
CREATE TABLE raw_staging.indeed_jobs_raw (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_job_id VARCHAR(100),
    source_url TEXT,
    job_title TEXT,
    company_name TEXT,
    location_raw TEXT,
    job_description TEXT,
    employment_type TEXT,
    work_arrangement TEXT,
    industry_raw TEXT,
    skills_raw TEXT,
    requirements_raw TEXT,
    posted_date DATE,
    scraped_at TIMESTAMP NOT NULL,
    raw_json JSONB,
    batch_id UUID NOT NULL,
    data_quality_flags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_job_id, batch_id)
);

-- Glassdoor 原始資料表
CREATE TABLE raw_staging.glassdoor_jobs_raw (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_job_id VARCHAR(100),
    source_url TEXT,
    job_title TEXT,
    company_name TEXT,
    location_raw TEXT,
    job_description TEXT,
    employment_type TEXT,
    work_arrangement TEXT,
    industry_raw TEXT,
    skills_raw TEXT,
    requirements_raw TEXT,
    posted_date DATE,
    scraped_at TIMESTAMP NOT NULL,
    raw_json JSONB,
    batch_id UUID NOT NULL,
    data_quality_flags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_job_id, batch_id)
);

-- ============================================================================
-- Clean Staging - 統一清洗資料
-- ============================================================================

CREATE TABLE clean_staging.jobs_unified (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- 標準化基本資訊
    job_title_clean VARCHAR(300) NOT NULL,
    company_name_clean VARCHAR(200) NOT NULL,
    
    -- 地理資訊標準化
    city VARCHAR(100),
    state VARCHAR(50),
    state_abbreviation VARCHAR(2),        -- CA, NY, TX 等
    country VARCHAR(50) DEFAULT 'USA',
    location_type VARCHAR(20),            -- remote, hybrid, onsite
    
    -- 職位分類標準化
    employment_type_clean VARCHAR(50),    -- full-time, part-time, contract, internship
    job_level VARCHAR(50),                -- entry, mid, senior, executive
    job_category VARCHAR(100),            -- Software Engineering, Data Science, etc.
    
    -- 行業標準化
    industry_category VARCHAR(100),       -- Technology, Healthcare, Finance, etc.
    
    -- 職位內容
    job_description_clean TEXT,
    requirements_clean TEXT,
    
    -- 技能提取 (從描述中自動提取)
    extracted_skills TEXT[],              -- [Python, SQL, AWS, Docker]
    skill_categories TEXT[],              -- [programming, database, cloud, tools]
    
    -- 來源追蹤
    source_platform VARCHAR(50) NOT NULL, -- linkedin, indeed, glassdoor
    original_raw_id UUID NOT NULL,        -- 對應到 raw staging 的 ID
    source_url TEXT,
    posted_date DATE,
    scraped_at TIMESTAMP NOT NULL,
    
    -- 資料品質
    data_quality_score DECIMAL(3,2),      -- 0.00 到 1.00
    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_group_id UUID,               -- 重複職缺的群組 ID
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Business Staging - 業務規則處理
-- ============================================================================

CREATE TABLE business_staging.jobs_final (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- 繼承 clean_staging 的所有欄位
    job_title_clean VARCHAR(300) NOT NULL,
    company_name_clean VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    state_abbreviation VARCHAR(2),
    country VARCHAR(50) DEFAULT 'USA',
    location_type VARCHAR(20),
    employment_type_clean VARCHAR(50),
    job_level VARCHAR(50),
    job_category VARCHAR(100),
    industry_category VARCHAR(100),
    job_description_clean TEXT,
    requirements_clean TEXT,
    extracted_skills TEXT[],
    skill_categories TEXT[],
    source_platform VARCHAR(50) NOT NULL,
    original_raw_id UUID NOT NULL,
    source_url TEXT,
    posted_date DATE,
    scraped_at TIMESTAMP NOT NULL,
    data_quality_score DECIMAL(3,2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_group_id UUID,
    
    -- 業務邏輯處理後的欄位
    job_title_standardized VARCHAR(200),  -- 標準化職位名稱
    company_id VARCHAR(100),              -- 統一的公司識別碼
    location_id VARCHAR(100),             -- 統一的地點識別碼
    
    -- 分析用標記
    is_tech_role BOOLEAN DEFAULT FALSE,   -- 是否為技術職位
    is_senior_role BOOLEAN DEFAULT FALSE, -- 是否為資深職位
    is_popular_company BOOLEAN DEFAULT FALSE, -- 是否為熱門公司
    
    -- 準備載入 DWH
    ready_for_dwh BOOLEAN DEFAULT FALSE,
    dwh_load_date TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Data Warehouse - Star Schema 維度表
-- ============================================================================

-- 公司維度表
CREATE TABLE dwh.dim_companies (
    company_key SERIAL PRIMARY KEY,
    company_id VARCHAR(100) NOT NULL UNIQUE,
    company_name VARCHAR(200) NOT NULL,
    industry_category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 地點維度表
CREATE TABLE dwh.dim_locations (
    location_key SERIAL PRIMARY KEY,
    location_id VARCHAR(100) NOT NULL UNIQUE,
    city VARCHAR(100),
    state VARCHAR(50),
    state_abbreviation VARCHAR(2),
    country VARCHAR(50) DEFAULT 'USA',
    region VARCHAR(50),                   -- West, East, Midwest, South
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 職位角色維度表
CREATE TABLE dwh.dim_job_roles (
    role_key SERIAL PRIMARY KEY,
    job_category VARCHAR(100) NOT NULL,   -- Software Engineering
    job_subcategory VARCHAR(100),         -- Backend Development
    job_level VARCHAR(50),                -- Senior
    employment_type VARCHAR(50),          -- Full-time
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_category, job_subcategory, job_level, employment_type)
);

-- 技能維度表
CREATE TABLE dwh.dim_skills (
    skill_key SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) NOT NULL UNIQUE,
    skill_category VARCHAR(50),           -- programming, database, cloud
    skill_type VARCHAR(30),               -- technical, soft, certification
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 日期維度表
CREATE TABLE dwh.dim_dates (
    date_key INTEGER PRIMARY KEY,         -- 20241225
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100)
);

-- ============================================================================
-- Data Warehouse - 事實表
-- ============================================================================

-- 職缺事實表
CREATE TABLE dwh.fact_jobs (
    job_key BIGSERIAL PRIMARY KEY,
    
    -- 維度鍵
    company_key INTEGER REFERENCES dwh.dim_companies(company_key),
    location_key INTEGER REFERENCES dwh.dim_locations(location_key),
    role_key INTEGER REFERENCES dwh.dim_job_roles(role_key),
    posted_date_key INTEGER REFERENCES dwh.dim_dates(date_key),
    scraped_date_key INTEGER REFERENCES dwh.dim_dates(date_key),
    
    -- 自然鍵
    job_id VARCHAR(100) NOT NULL,
    source_platform VARCHAR(50) NOT NULL,
    
    -- 職缺詳情
    job_title VARCHAR(300) NOT NULL,
    source_url TEXT,
    
    -- 工作模式
    is_remote BOOLEAN DEFAULT FALSE,
    is_hybrid BOOLEAN DEFAULT FALSE,
    
    -- 時間戳
    posted_date DATE,
    scraped_at TIMESTAMP NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 確保唯一性
    UNIQUE(job_id, source_platform)
);

-- 職缺技能橋接表 (多對多關係)
CREATE TABLE dwh.bridge_job_skills (
    job_key BIGINT REFERENCES dwh.fact_jobs(job_key) ON DELETE CASCADE,
    skill_key INTEGER REFERENCES dwh.dim_skills(skill_key) ON DELETE CASCADE,
    skill_importance INTEGER DEFAULT 1,   -- 1-5，技能重要程度
    extracted_from VARCHAR(50),           -- 'description', 'requirements', 'title'
    PRIMARY KEY (job_key, skill_key)
);

-- ============================================================================
-- 效能優化索引
-- ============================================================================

-- Raw Staging 索引
CREATE INDEX idx_linkedin_raw_scraped_at ON raw_staging.linkedin_jobs_raw(scraped_at DESC);
CREATE INDEX idx_linkedin_raw_batch_id ON raw_staging.linkedin_jobs_raw(batch_id);
CREATE INDEX idx_linkedin_raw_source_job_id ON raw_staging.linkedin_jobs_raw(source_job_id);

CREATE INDEX idx_indeed_raw_scraped_at ON raw_staging.indeed_jobs_raw(scraped_at DESC);
CREATE INDEX idx_indeed_raw_batch_id ON raw_staging.indeed_jobs_raw(batch_id);

CREATE INDEX idx_glassdoor_raw_scraped_at ON raw_staging.glassdoor_jobs_raw(scraped_at DESC);
CREATE INDEX idx_glassdoor_raw_batch_id ON raw_staging.glassdoor_jobs_raw(batch_id);

-- Clean Staging 索引
CREATE INDEX idx_jobs_unified_source ON clean_staging.jobs_unified(source_platform, scraped_at DESC);
CREATE INDEX idx_jobs_unified_location ON clean_staging.jobs_unified(city, state);
CREATE INDEX idx_jobs_unified_company ON clean_staging.jobs_unified(company_name_clean);
CREATE INDEX idx_jobs_unified_category ON clean_staging.jobs_unified(job_category);
CREATE INDEX idx_jobs_unified_duplicate ON clean_staging.jobs_unified(is_duplicate, duplicate_group_id);

-- Business Staging 索引
CREATE INDEX idx_jobs_final_ready ON business_staging.jobs_final(ready_for_dwh, created_at);
CREATE INDEX idx_jobs_final_tech_role ON business_staging.jobs_final(is_tech_role);

-- Data Warehouse 索引
CREATE INDEX idx_fact_jobs_posted_date ON dwh.fact_jobs(posted_date_key);
CREATE INDEX idx_fact_jobs_company ON dwh.fact_jobs(company_key);
CREATE INDEX idx_fact_jobs_location ON dwh.fact_jobs(location_key);
CREATE INDEX idx_fact_jobs_role ON dwh.fact_jobs(role_key);
CREATE INDEX idx_fact_jobs_source ON dwh.fact_jobs(source_platform);
CREATE INDEX idx_fact_jobs_remote ON dwh.fact_jobs(is_remote);

-- 橋接表索引
CREATE INDEX idx_bridge_job_skills_job ON dwh.bridge_job_skills(job_key);
CREATE INDEX idx_bridge_job_skills_skill ON dwh.bridge_job_skills(skill_key);

-- ============================================================================
-- 註解說明
-- ============================================================================

COMMENT ON SCHEMA raw_staging IS '原始爬蟲資料暫存區';
COMMENT ON SCHEMA clean_staging IS '清洗標準化資料暫存區';
COMMENT ON SCHEMA business_staging IS '業務規則處理暫存區';
COMMENT ON SCHEMA dwh IS '資料倉儲 Star Schema';

COMMENT ON TABLE dwh.fact_jobs IS '職缺事實表 - 記錄所有職缺的核心資訊';
COMMENT ON TABLE dwh.dim_companies IS '公司維度表 - 包含公司基本資訊與行業分類';
COMMENT ON TABLE dwh.dim_locations IS '地點維度表 - 標準化的地理位置資訊';
COMMENT ON TABLE dwh.dim_skills IS '技能維度表 - 職位要求的技能清單';
COMMENT ON TABLE dwh.bridge_job_skills IS '職缺技能關聯表 - 多對多關係橋接';