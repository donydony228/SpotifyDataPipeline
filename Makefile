.PHONY: help start stop logs test lint format env-setup cloud-test cloud-status

help: ## 顯示幫助訊息
	@echo 'US Job Data Engineering Platform - 本地虛擬環境版本'
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ============================================================================
# 本地開發環境（虛擬環境）
# ============================================================================

start: ## 啟動 Airflow（本地模式）
	@echo "🚀 啟動 Airflow 本地開發環境..."
	@./airflow_start.sh

stop: ## 停止 Airflow
	@echo "🛑 停止 Airflow..."
	@./stop_airflow.sh

logs: ## 顯示 Airflow logs
	@echo "📜 Airflow Logs:"
	@tail -f airflow_home/logs/scheduler/latest/*.log 2>/dev/null || echo "No logs found"

restart: ## 重啟 Airflow
	@make stop
	@sleep 2
	@make start

# ============================================================================
# 環境設置
# ============================================================================

env-setup: ## 從範本創建 .env 文件
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "📝 已從範本創建 .env 文件"; \
		echo "⚠️  請編輯 .env 填入你的憑證"; \
	else \
		echo "✅ .env 文件已存在"; \
	fi

venv-setup: ## 設置 Python 虛擬環境
	@echo "🐍 創建 Python 虛擬環境..."
	@python3 -m venv venv
	@./venv/bin/pip install --upgrade pip
	@./venv/bin/pip install -r requirements.txt
	@echo "✅ 虛擬環境設置完成"
	@echo "💡 啟動虛擬環境: source venv/bin/activate"

generate-fernet: ## 生成新的 Fernet Key
	@echo "🔑 生成新的 Fernet Key..."
	@python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
	@echo ""
	@echo "💡 請將此 Key 添加到 .env 文件中："
	@echo "   AIRFLOW__CORE__FERNET_KEY=<上面的 Key>"

# ============================================================================
# 雲端連接測試
# ============================================================================

cloud-test-supabase: ## 測試 Supabase 連接
	@echo "🔗 測試 Supabase 連接..."
	@source venv/bin/activate && python test_supabase.py

cloud-test-mongodb: ## 測試 MongoDB Atlas 連接
	@echo "🔗 測試 MongoDB Atlas 連接..."
	@source venv/bin/activate && python test_mongodb.py

cloud-test: ## 測試所有雲端連接
	@echo "🧪 測試所有雲端連接..."
	@make cloud-test-supabase
	@echo ""
	@make cloud-test-mongodb

cloud-status: ## 顯示雲端資源狀態
	@echo "🌐 雲端資源狀態"
	@echo "========================="
	@echo ""
	@echo "📊 PostgreSQL (Supabase):"
	@source venv/bin/activate && python -c "import psycopg2, os; from dotenv import load_dotenv; load_dotenv(); conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL')); cur = conn.cursor(); cur.execute('SELECT schemaname, COUNT(*) FROM pg_tables WHERE schemaname IN (\"raw_staging\", \"clean_staging\", \"business_staging\", \"dwh\") GROUP BY schemaname ORDER BY schemaname'); print('\\n'.join(f'  ✅ {row[0]}: {row[1]} tables' for row in cur.fetchall())); conn.close()" 2>/dev/null || echo "  ❌ 連接失敗"
	@echo ""
	@echo "🍃 MongoDB (Atlas):"
	@source venv/bin/activate && python -c "from pymongo import MongoClient; from pymongo.server_api import ServerApi; import os; from dotenv import load_dotenv; load_dotenv(); client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1')); db = client.get_database(); collections = db.list_collection_names(); print(f'  ✅ Collections: {len(collections)}'); print(f'  📦 {collections}'); client.close()" 2>/dev/null || echo "  ❌ 連接失敗"

# ============================================================================
# 開發工具
# ============================================================================

test: ## 運行測試
	@echo "🧪 運行測試..."
	@source venv/bin/activate && pytest tests/ -v

lint: ## 代碼檢查
	@echo "🔍 運行 linting..."
	@source venv/bin/activate && flake8 dags/ --max-line-length=120

format: ## 格式化代碼
	@echo "🎨 格式化代碼..."
	@source venv/bin/activate && black dags/

dag-test: ## 測試 DAG 語法
	@echo "🔍 測試 DAG 語法..."
	@source venv/bin/activate && python -c "from airflow.models import DagBag; dagbag = DagBag(dag_folder='dags/'); print(f'✅ 找到 {len(dagbag.dags)} 個 DAGs'); print(f'❌ 錯誤: {len(dagbag.import_errors)}'); [print(f'  - {filename}: {error}') for filename, error in dagbag.import_errors.items()]"

# ============================================================================
# 清理
# ============================================================================

clean-logs: ## 清理 Airflow logs
	@echo "🧹 清理 Airflow logs..."
	@rm -rf airflow_home/logs/*
	@echo "✅ Logs 已清理"

clean-db: ## 清理本地 SQLite 資料庫
	@echo "🧹 清理本地資料庫..."
	@rm -f airflow.db
	@rm -f airflow_home/airflow.db
	@echo "✅ 本地資料庫已清理"

clean-pyc: ## 清理 Python 編譯文件
	@echo "🧹 清理 Python 編譯文件..."
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -delete
	@echo "✅ Python 編譯文件已清理"

clean-all: clean-logs clean-db clean-pyc ## 清理所有臨時文件
	@echo "✅ 所有臨時文件已清理"

# ============================================================================
# 快速指令
# ============================================================================

dev-start: ## 開發快速啟動
	@echo "⚡ 開發快速啟動"
	@echo "==============="
	@make env-setup
	@echo ""
	@make start
	@sleep 5
	@echo ""
	@echo "✅ Airflow 已啟動"
	@echo "🌐 訪問 http://localhost:8080"
	@echo "👤 用戶名: admin / 密碼: admin123"

dev-status: ## 顯示開發環境狀態
	@echo "📊 開發環境狀態"
	@echo "==============="
	@echo ""
	@echo "🐍 虛擬環境:"
	@if [ -d "venv" ]; then echo "  ✅ 已創建"; else echo "  ❌ 未創建 (運行 make venv-setup)"; fi
	@echo ""
	@echo "📝 環境變數:"
	@if [ -f ".env" ]; then echo "  ✅ .env 已存在"; else echo "  ❌ .env 不存在 (運行 make env-setup)"; fi
	@echo ""
	@echo "🌊 Airflow:"
	@if pgrep -f "airflow scheduler" > /dev/null; then echo "  ✅ 正在運行"; else echo "  ❌ 未運行 (運行 make start)"; fi
	@echo ""
	@make cloud-status

# ============================================================================
# 資訊指令
# ============================================================================

info: ## 顯示專案資訊
	@echo "📋 US Job Data Engineering Platform"
	@echo "===================================="
	@echo ""
	@echo "專案目錄: $(PWD)"
	@echo "Python: $(shell python3 --version)"
	@echo ""
	@echo "📁 重要文件:"
	@echo "  - DAGs: dags/"
	@echo "  - 環境變數: .env"
	@echo "  - Airflow Home: airflow_home/"
	@echo ""
	@echo "🌐 雲端服務:"
	@echo "  - PostgreSQL: Supabase"
	@echo "  - MongoDB: Atlas"
	@echo ""
	@echo "💡 常用指令:"
	@echo "  make dev-start   - 快速啟動開發環境"
	@echo "  make dev-status  - 檢查環境狀態"
	@echo "  make cloud-test  - 測試雲端連接"
	@echo "  make help        - 顯示所有指令"