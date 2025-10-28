.PHONY: help airflow-start airflow-stop airflow-restart airflow-status airflow-logs clean-airflow \
	     env-setup venv-setup db-test spotify-test dag-test \
	     clean-logs clean-pyc clean-all dev-start info

help: ## 顯示幫助訊息
	@echo 'Spotify Music Analytics Platform'
	@echo '================================'
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ============================================================================
# Airflow 管理 - 主要指令
# ============================================================================

airflow-start: ## 啟動 Airflow (webserver + scheduler)
	@echo "🚀 啟動 Airflow 服務..."
	@if pgrep -f "airflow" > /dev/null; then \
		echo "⚠️  Airflow 已在運行，先停止現有服務..."; \
		make airflow-stop; \
		sleep 3; \
	fi
	@echo "📁 設定 AIRFLOW_HOME=$(PWD)"
	@export AIRFLOW_HOME=$(PWD) && \
	source venv/bin/activate && \
	echo "🌐 啟動 Webserver (後台)..." && \
	airflow webserver --port 8080 --daemon && \
	sleep 5 && \
	echo "📅 啟動 Scheduler (後台)..." && \
	airflow scheduler --daemon && \
	sleep 3
	@echo ""
	@make airflow-status

airflow-stop: ## 停止所有 Airflow 服務
	@echo "🛑 停止 Airflow 服務..."
	@pkill -f "airflow webserver" 2>/dev/null || true
	@pkill -f "airflow scheduler" 2>/dev/null || true
	@pkill -f "gunicorn.*airflow" 2>/dev/null || true
	@sleep 2
	@if pgrep -f "airflow" > /dev/null; then \
		echo "⚠️  強制停止殘留進程..."; \
		pkill -9 -f "airflow" 2>/dev/null || true; \
	fi
	@echo "✅ Airflow 服務已停止"

airflow-restart: ## 重啟 Airflow 服務
	@make airflow-stop
	@sleep 2
	@make airflow-start

airflow-status: ## 檢查 Airflow 服務狀態
	@echo "📊 Airflow 服務狀態"
	@echo "==================="
	@if pgrep -f "airflow webserver" > /dev/null; then \
		echo "🌐 Webserver: ✅ 運行中"; \
		echo "   URL: http://localhost:8080"; \
		echo "   帳號: admin / 密碼: admin123"; \
	else \
		echo "🌐 Webserver: ❌ 未運行"; \
	fi
	@if pgrep -f "airflow scheduler" > /dev/null; then \
		echo "📅 Scheduler: ✅ 運行中"; \
	else \
		echo "📅 Scheduler: ❌ 未運行"; \
	fi
	@echo ""
	@if curl -s http://localhost:8080/health > /dev/null 2>&1; then \
		echo "🔗 連接測試: ✅ Webserver 可訪問"; \
	else \
		echo "🔗 連接測試: ❌ Webserver 不可訪問"; \
	fi

airflow-logs: ## 查看 Airflow 日誌
	@echo "📜 Airflow 日誌 (按 Ctrl+C 退出)"
	@echo "=========================="
	@if [ -f "logs/scheduler/latest/*.log" ]; then \
		tail -f logs/scheduler/latest/*.log; \
	else \
		echo "📋 Scheduler 日誌檔案不存在，顯示 DAG 處理日誌..."; \
		find logs -name "*.log" -type f -exec tail -20 {} \; 2>/dev/null || echo "沒有找到日誌檔案"; \
	fi

clean-airflow: ## 清理 Airflow 相關檔案 (重置環境)
	@echo "🧹 清理 Airflow 環境..."
	@make airflow-stop
	@rm -f airflow.db airflow.cfg
	@rm -rf logs/*
	@rm -f webserver_config.py
	@echo "✅ Airflow 環境已清理"

# ============================================================================
# 環境設置和測試
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
	@if [ ! -d "venv" ]; then \
		python3.11 -m venv venv; \
		echo "📦 安裝套件..."; \
		./venv/bin/pip install --upgrade pip; \
		./venv/bin/pip install -r requirements.txt; \
		echo "✅ 虛擬環境設置完成"; \
	else \
		echo "✅ 虛擬環境已存在"; \
	fi
	@echo "💡 啟動虛擬環境: source venv/bin/activate"

db-test: ## 測試資料庫連接
	@echo "🔍 測試資料庫連接..."
	@source venv/bin/activate && python -c "\
	from utils.database import test_mongodb_connection, test_postgresql_connection; \
	print('MongoDB:', '✅ 連接成功' if test_mongodb_connection() else '❌ 連接失敗'); \
	print('PostgreSQL:', '✅ 連接成功' if test_postgresql_connection() else '❌ 連接失敗')"

spotify-test: ## 測試 Spotify API 連接
	@echo "🎵 測試 Spotify API..."
	@source venv/bin/activate && python -c "\
	import os; \
	from dotenv import load_dotenv; \
	load_dotenv(); \
	required = ['SPOTIFY_CLIENT_ID', 'SPOTIFY_CLIENT_SECRET', 'SPOTIFY_REFRESH_TOKEN']; \
	missing = [var for var in required if not os.getenv(var)]; \
	if missing: \
		print(f'❌ 缺少環境變數: {missing}'); \
	else: \
		print('✅ Spotify 環境變數齊全')"

dag-test: ## 測試 DAG 載入
	@echo "🔍 測試 DAG 載入..."
	@source venv/bin/activate && \
	export AIRFLOW_HOME=$(PWD) && \
	python -c "\
	import sys; \
	sys.path.append('$(PWD)'); \
	try: \
		from dags.spotify.daily_etl_pipeline import dag as dag1; \
		print('✅ daily_etl_pipeline.py 載入成功'); \
	except Exception as e: \
		print(f'❌ daily_etl_pipeline.py 載入失敗: {e}'); \
	try: \
		from dags.spotify.curl_spotify_tracker import dag as dag2; \
		print('✅ curl_spotify_tracker.py 載入成功'); \
	except Exception as e: \
		print(f'❌ curl_spotify_tracker.py 載入失敗: {e}')"

# ============================================================================
# 清理指令
# ============================================================================

clean-logs: ## 清理日誌檔案
	@echo "🧹 清理日誌檔案..."
	@rm -rf logs/*
	@echo "✅ 日誌檔案已清理"

clean-pyc: ## 清理 Python 編譯檔案
	@echo "🧹 清理 Python 編譯檔案..."
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -delete
	@echo "✅ Python 編譯檔案已清理"

clean-all: clean-airflow clean-logs clean-pyc ## 清理所有臨時檔案
	@echo "✅ 所有臨時檔案已清理"

# ============================================================================
# 快速開發指令
# ============================================================================

dev-start: ## 完整開發環境啟動 (一鍵啟動)
	@echo "⚡ 開發環境一鍵啟動"
	@echo "=================="
	@make env-setup
	@make venv-setup
	@echo ""
	@echo "🔍 測試環境..."
	@make spotify-test
	@make db-test
	@echo ""
	@echo "🚀 啟動 Airflow..."
	@make airflow-start
	@echo ""
	@echo "✅ 開發環境啟動完成！"
	@echo "🌐 訪問 http://localhost:8080"
	@echo "📋 查看狀態: make airflow-status"
	@echo "📜 查看日誌: make airflow-logs"

dev-stop: ## 完整停止開發環境
	@echo "🛑 停止開發環境..."
	@make airflow-stop
	@echo "✅ 開發環境已停止"

# ============================================================================
# 資訊和幫助
# ============================================================================

info: ## 顯示專案和環境資訊
	@echo "📋 Spotify Music Analytics Platform"
	@echo "===================================="
	@echo ""
	@echo "📂 專案目錄: $(PWD)"
	@echo "🐍 Python 版本: $(shell python3 --version 2>/dev/null || echo 'Not found')"
	@echo "🌊 Airflow 版本: $(shell source venv/bin/activate && airflow version 2>/dev/null || echo 'Not installed')"
	@echo ""
	@echo "📁 重要目錄:"
	@echo "  • DAGs: dags/spotify/"
	@echo "  • Utils: utils/"
	@echo "  • 環境變數: .env"
	@echo "  • 虛擬環境: venv/"
	@echo "  • 日誌: logs/"
	@echo ""
	@echo "🎵 DAG 列表:"
	@echo "  • daily_etl_pipeline - 基礎 ETL 管道"
	@echo "  • enhanced_spotify_tracker - 完整音樂追蹤"
	@echo ""
	@echo "🌐 雲端服務:"
	@echo "  • PostgreSQL: Supabase"
	@echo "  • MongoDB: MongoDB Atlas"
	@echo "  • 音樂 API: Spotify Web API"
	@echo ""
	@echo "💡 常用指令:"
	@echo "  make dev-start      - 一鍵啟動開發環境"
	@echo "  make airflow-status - 檢查 Airflow 狀態"
	@echo "  make airflow-logs   - 查看即時日誌"
	@echo "  make db-test        - 測試資料庫連接"
	@echo "  make help           - 顯示所有指令"