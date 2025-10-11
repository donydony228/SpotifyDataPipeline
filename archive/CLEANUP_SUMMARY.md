# 專案清理摘要

**清理日期**: $(date)
**目的**: 從求職市場爬蟲轉換為音樂分析平台

## 已封存的內容

### 1. 爬蟲程式碼
- `src/scrapers/` → `archive/scrapers/`
- LinkedIn 爬蟲
- Indeed 爬蟲
- 模擬爬蟲

### 2. DAG 檔案
- `dags/scrapers/*.py` → `archive/old_dags/`
- 所有爬蟲相關 DAG

### 3. 部署腳本
- Railway 部署腳本
- Render 部署腳本
- Supabase 修復腳本

### 4. 測試腳本
- LinkedIn 爬蟲測試
- Indeed 爬蟲測試
- 模擬測試驗證

### 5. 文檔
- 舊的 architecture.md
- 舊的 README 備份

## 保留的內容

### ✅ 核心架構
- Airflow 環境 (`airflow_home/`)
- Python 虛擬環境 (`venv/`)
- 工具類別 (`src/utils/`)
- ETL 框架 (`src/etl/`)

### ✅ 設定檔
- `.env` (需更新 Spotify credentials)
- `requirements.txt` (需更新套件)
- `Makefile`
- `.gitignore`

### ✅ 資料庫連線
- MongoDB Atlas 設定
- Supabase PostgreSQL 設定

## 下一步

1. 更新 `requirements.txt` (移除爬蟲套件,新增 Spotify)
2. 更新 `.env` (新增 Spotify API credentials)
3. 建立新的 DAG (`dags/spotify/daily_music_tracker.py`)
4. 建立 Spotify API 客戶端 (`src/spotify/spotify_client.py`)
5. 設計新的資料結構 (MongoDB collections, PostgreSQL tables)

## 如何還原

如果需要還原任何檔案:
```bash
# 範例: 還原 LinkedIn 爬蟲
cp -r archive/scrapers/linkedin_scraper.py src/scrapers/
```

所有封存的檔案都在 `archive/` 目錄中,隨時可以查閱或還原。
