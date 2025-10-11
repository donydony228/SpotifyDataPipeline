#!/bin/bash
# cleanup_old_project.sh
# 清理舊的求職市場爬蟲專案,準備轉換為音樂分析平台

echo "🧹 開始清理舊專案檔案"
echo "========================================"
echo ""
echo "⚠️  這個腳本會:"
echo "  1. 刪除爬蟲相關程式碼"
echo "  2. 封存舊文檔與部署腳本"
echo "  3. 保留核心架構 (Airflow, 資料庫連線等)"
echo ""
read -p "確認要繼續嗎? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ 已取消清理"
    exit 1
fi

# ============================================================================
# Step 1: 建立 archive 目錄
# ============================================================================

echo ""
echo "📁 Step 1: 建立封存目錄..."

mkdir -p archive/{scrapers,deployment,docs,old_dags,old_sql}

echo "  ✅ 封存目錄已建立:"
echo "     - archive/scrapers/"
echo "     - archive/deployment/"
echo "     - archive/docs/"
echo "     - archive/old_dags/"
echo "     - archive/old_sql/"

# ============================================================================
# Step 2: 封存 (不刪除) 爬蟲程式碼
# ============================================================================

echo ""
echo "📦 Step 2: 封存爬蟲程式碼..."

# 封存 src/scrapers/
if [ -d "src/scrapers" ]; then
    echo "  📦 封存 src/scrapers/ ..."
    cp -r src/scrapers/ archive/scrapers/
    rm -rf src/scrapers/
    echo "  ✅ src/scrapers/ 已封存並刪除"
else
    echo "  ⚠️  src/scrapers/ 不存在"
fi

# 封存 DAG 中的爬蟲
echo "  📦 封存爬蟲 DAGs..."

if [ -d "dags/scrapers" ]; then
    # 先複製到 archive
    cp -r dags/scrapers/ archive/old_dags/
    
    # 刪除爬蟲相關的 DAG
    rm -f dags/scrapers/linkedin_scraper_dag.py
    rm -f dags/scrapers/indeed_scraper_dag.py
    rm -f dags/scrapers/*mock*.py
    rm -f dags/scrapers/linkedin_mock_scraper*.py
    
    echo "  ✅ 爬蟲 DAGs 已封存並刪除"
else
    echo "  ⚠️  dags/scrapers/ 不存在"
fi

# 封存測試 DAGs
if [ -f "dags/dual_db_scraper.py" ]; then
    mv dags/dual_db_scraper.py archive/old_dags/
    echo "  ✅ dual_db_scraper.py 已封存"
fi

if [ -f "dags/diagnostic_dag.py" ]; then
    mv dags/diagnostic_dag.py archive/old_dags/
    echo "  ✅ diagnostic_dag.py 已封存"
fi

if [ -f "dags/linkedin_mock_scraper_env.py" ]; then
    mv dags/linkedin_mock_scraper_env.py archive/old_dags/
    echo "  ✅ linkedin_mock_scraper_env.py 已封存"
fi

# ============================================================================
# Step 3: 刪除測試腳本
# ============================================================================

echo ""
echo "🗑️  Step 3: 刪除測試腳本..."

test_scripts=(
    "scripts/test_linkedin_scraper.py"
    "scripts/test_indeed_scraper.py"
    "scripts/validate_mock_test_results.py"
    "scripts/run_mock_tests.sh"
    "scripts/test_final_mock_dag.sh"
)

for script in "${test_scripts[@]}"; do
    if [ -f "$script" ]; then
        rm "$script"
        echo "  ✅ 已刪除: $script"
    fi
done

# ============================================================================
# Step 4: 封存部署腳本
# ============================================================================

echo ""
echo "📦 Step 4: 封存部署相關腳本..."

deployment_scripts=(
    "scripts/setup_railway*.sh"
    "scripts/deploy_to_railway.sh"
    "scripts/railway_start*.sh"
    "scripts/render_*.sh"
    "scripts/prepare_railway.py"
    "scripts/update_railway_env.py"
    "scripts/fix_railway_network.sh"
    "scripts/simple_health_server.py"
)

for pattern in "${deployment_scripts[@]}"; do
    for file in $pattern; do
        if [ -f "$file" ]; then
            mv "$file" archive/deployment/
            echo "  ✅ 已封存: $file"
        fi
    done
done

# 封存 Railway 環境變數檔案
if ls railway*.txt 1> /dev/null 2>&1; then
    mv railway*.txt archive/deployment/ 2>/dev/null
    echo "  ✅ 已封存: railway*.txt"
fi

# ============================================================================
# Step 5: 封存舊文檔
# ============================================================================

echo ""
echo "📦 Step 5: 封存舊文檔..."

# 封存舊的 README (如果存在備份)
if [ -f "README.md.backup" ]; then
    mv README.md.backup archive/docs/
    echo "  ✅ 已封存: README.md.backup"
fi

# 封存架構文檔
if [ -f "docs/architecture.md" ]; then
    mv docs/architecture.md archive/docs/
    echo "  ✅ 已封存: docs/architecture.md"
fi

# ============================================================================
# Step 6: 封存舊 SQL 檔案
# ============================================================================

echo ""
echo "📦 Step 6: 封存舊 SQL 檔案..."

if [ -d "sql/ddl" ]; then
    cp -r sql/ddl/ archive/old_sql/
    echo "  ✅ 已封存: sql/ddl/ (原檔案保留,稍後手動清理)"
fi

# ============================================================================
# Step 7: 清理特定的舊檔案
# ============================================================================

echo ""
echo "🗑️  Step 7: 清理其他舊檔案..."

old_files=(
    "scripts/init_mongodb_atlas.py"
    "scripts/fix_supabase_ipv6.py"
    "scripts/fix_supabase_complete.py"
    "scripts/force_ipv4_dns.py"
    "scripts/quick_env_check.py"
    "scripts/fix_airflow_environment.sh"
    "scripts/start_local_development.sh"
    ".env.backup"
    ".env.local.template"
    "airflow_start.sh.backup"
)

for file in "${old_files[@]}"; do
    if [ -f "$file" ]; then
        mv "$file" archive/ 2>/dev/null
        echo "  ✅ 已封存: $file"
    fi
done

# ============================================================================
# Step 8: 建立新的專案結構
# ============================================================================

echo ""
echo "📁 Step 8: 建立新的專案目錄結構..."

# 建立 Spotify 相關目錄
mkdir -p src/spotify
mkdir -p dags/spotify
mkdir -p notebooks
mkdir -p dashboard

# 建立 __init__.py
touch src/spotify/__init__.py
touch dags/spotify/__init__.py

echo "  ✅ 已建立:"
echo "     - src/spotify/"
echo "     - dags/spotify/"
echo "     - notebooks/"
echo "     - dashboard/"

# ============================================================================
# Step 9: 建立清理摘要
# ============================================================================

echo ""
echo "📊 Step 9: 生成清理摘要..."

cat > archive/CLEANUP_SUMMARY.md << 'EOF'
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
EOF

echo "  ✅ 清理摘要已生成: archive/CLEANUP_SUMMARY.md"

# ============================================================================
# Step 10: 顯示清理結果
# ============================================================================

echo ""
echo "========================================"
echo "🎉 清理完成!"
echo "========================================"
echo ""
echo "📊 清理統計:"
echo ""

# 統計封存的檔案數量
scraper_count=$(find archive/scrapers -type f 2>/dev/null | wc -l)
dag_count=$(find archive/old_dags -type f 2>/dev/null | wc -l)
deploy_count=$(find archive/deployment -type f 2>/dev/null | wc -l)
doc_count=$(find archive/docs -type f 2>/dev/null | wc -l)

echo "  🗂️  封存的爬蟲檔案: $scraper_count 個"
echo "  🗂️  封存的 DAG 檔案: $dag_count 個"
echo "  🗂️  封存的部署腳本: $deploy_count 個"
echo "  🗂️  封存的文檔: $doc_count 個"

echo ""
echo "✅ 保留的核心架構:"
echo "  - Airflow 環境"
echo "  - 資料庫連線設定"
echo "  - 工具類別"
echo "  - Makefile"
echo ""
echo "🆕 建立的新目錄:"
echo "  - src/spotify/"
echo "  - dags/spotify/"
echo "  - notebooks/"
echo "  - dashboard/"
echo ""
echo "📋 下一步建議:"
echo ""
echo "  1. 檢查封存的檔案: ls -la archive/"
echo "  2. 更新 requirements.txt"
echo "  3. 更新 .env 檔案 (新增 Spotify credentials)"
echo "  4. 開始建立新的 Spotify API 客戶端"
echo ""
echo "💡 所有封存的檔案都在 archive/ 目錄"
echo "   如需還原,隨時可以從 archive/ 複製回來"
echo ""
echo "🎵 準備好開始音樂分析平台了!"