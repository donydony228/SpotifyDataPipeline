.PHONY: help start stop logs restart env-setup venv-setup generate-fernet cloud-status dag-test clean-logs clean-db clean-pyc clean-all dev-start dev-status info

help: ## 顯示幫助訊息
	@echo 'Music Data Engineering Platform - 本地環境版本'
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ============================================================================
# 本地開發環境（純虛擬環境，無容器）
# ============================================================================

start: ## 啟動 Airflow（本地模式）
	@echo "🚀 啟動 Airflow 本地開發環境..."
	@./scripts/start_local_airflow.sh

stop: ## 停止 Airflow
	@echo "🛑 停止 Airflow..."
	@./scripts/stop_local_airflow.sh

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
		echo "⚠️  請編輯 .env 填入你的 Spotify 憑證"; \
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
# 環境變數檢查與修復
# ============================================================================

env-check: ## 檢查環境變數是否正確載入
	@echo "🔍 檢查環境變數..."
	@source venv/bin/activate && python -c "\
	import os; \
	from dotenv import load_dotenv; \
	load_dotenv(); \
	spotify_vars = ['SPOTIFY_CLIENT_ID', 'SPOTIFY_CLIENT_SECRET', 'SPOTIFY_REFRESH_TOKEN']; \
	print('🎵 Spotify 環境變數:'); \
	for var in spotify_vars: \
		value = os.getenv(var); \
		if value: \
			print(f'  ✅ {var}: {value[:10]}***'); \
		else: \
			print(f'  ❌ {var}: 未設定'); \
	"

env-fix: ## 修復環境變數問題
	@echo "🔧 修復環境變數載入問題..."
	@if [ ! -f .env ]; then echo "❌ .env 檔案不存在！"; exit 1; fi
	@echo "✅ .env 檔案存在"
	@echo "📝 複製 .env 到 Airflow home..."
	@mkdir -p airflow_home
	@cp .env airflow_home/.env
	@echo "✅ 環境變數已複製到 airflow_home/.env"

# ============================================================================
# 雲端狀態監控
# ============================================================================

cloud-status: ## 顯示雲端資源狀態
	@echo "🌐 雲端資源狀態"
	@echo "========================="
	@echo ""
	@source venv/bin/activate && python -c "\
	import os; \
	from dotenv import load_dotenv; \
	load_dotenv(); \
	print('📊 PostgreSQL (Supabase):'); \
	supabase_url = os.getenv('SUPABASE_DB_URL'); \
	if supabase_url: \
		print('  ✅ URL 已設定'); \
		try: \
			import psycopg2; \
			conn = psycopg2.connect(supabase_url, connect_timeout=10); \
			conn.close(); \
			print('  ✅ 連線測試成功'); \
		except Exception as e: \
			print(f'  ❌ 連線失敗: {e}'); \
	else: \
		print('  ❌ URL 未設定'); \
	print(''); \
	print('📊 MongoDB Atlas:'); \
	mongodb_url = os.getenv('MONGODB_ATLAS_URL'); \
	if mongodb_url: \
		print('  ✅ URL 已設定'); \
		try: \
			from pymongo import MongoClient; \
			client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5000); \
			client.admin.command('ping'); \
			client.close(); \
			print('  ✅ 連線測試成功'); \
		except Exception as e: \
			print(f'  ❌ 連線失敗: {e}'); \
	else: \
		print('  ❌ URL 未設定'); \
	"

# ============================================================================
# DAG 管理
# ============================================================================

dag-list: ## 列出所有 DAGs
	@source venv/bin/activate && cd airflow_home && airflow dags list

dag-test: ## 測試 DAG 語法
	@echo "🧪 測試 DAG 語法..."
	@source venv/bin/activate && cd airflow_home && python -c "\
	import sys; \
	sys.path.append('../dags'); \
	try: \
		from spotify.daily_music_tracker import dag; \
		print('✅ DAG 語法正確'); \
	except Exception as e: \
		print(f'❌ DAG 語法錯誤: {e}'); \
	"

# ============================================================================
# 清理指令
# ============================================================================

clean-logs: ## 清理 Airflow logs
	@rm -rf airflow_home/logs/*
	@echo "✅ Airflow logs 已清理"

clean-db: ## 清理本地資料庫
	@rm -f airflow_home/airflow.db
	@echo "✅ 本地 Airflow 資料庫已清理"

clean-pyc: ## 清理 Python 編譯文件
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
	@make venv-setup
	@make env-fix
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
	@if [ -f "airflow_home/.env" ]; then echo "  ✅ airflow_home/.env 已存在"; else echo "  ❌ airflow_home/.env 不存在 (運行 make env-fix)"; fi
	@echo ""
	@echo "🌊 Airflow:"
	@if pgrep -f "airflow standalone" > /dev/null; then echo "  ✅ 正在運行"; else echo "  ❌ 未運行 (運行 make start)"; fi
	@echo ""
	@make cloud-status

# ============================================================================
# 資訊指令
# ============================================================================

info: ## 顯示專案資訊
	@echo "📋 Music Data Engineering Platform"
	@echo "===================================="
	@echo ""
	@echo "專案目錄: $(PWD)"
	@echo "Python: $(shell python3 --version 2>/dev/null || echo 'Not found')"
	@echo ""
	@echo "📁 重要文件:"
	@echo "  - DAGs: dags/"
	@echo "  - 環境變數: .env"
	@echo "  - Airflow Home: airflow_home/"
	@echo "  - 虛擬環境: venv/"
	@echo ""
	@echo "🌐 雲端服務:"
	@echo "  - PostgreSQL: Supabase"
	@echo "  - MongoDB: Atlas"
	@echo "  - 音樂 API: Spotify"
	@echo ""
	@echo "💡 常用指令:"
	@echo "  make dev-start   - 快速啟動開發環境"
	@echo "  make dev-status  - 檢查環境狀態"
	@echo "  make env-check   - 檢查環境變數"
	@echo "  make cloud-status - 檢查雲端連接"
	@echo "  make dag-list    - 列出所有 DAGs"
	@echo "  make help        - 顯示所有指令"