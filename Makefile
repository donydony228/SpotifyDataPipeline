.PHONY: help start stop restart logs test lint format clean cloud-setup cloud-test

help: ## Show this help message
	@echo 'US Job Data Engineering Platform'
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ============================================================================
# 本地開發環境
# ============================================================================

start: ## Start all services with Docker
	@./start_docker.sh

stop: ## Stop all services
	@./stop_docker.sh

restart: ## Restart all services
	@docker compose restart

logs: ## Show logs for all services
	@./logs_docker.sh

logs-airflow: ## Show Airflow logs only
	@./logs_docker.sh airflow

status: ## Show service status
	@docker compose ps

# ============================================================================
# 雲端遷移
# ============================================================================

cloud-setup: ## Complete cloud migration setup
	@echo "🚀 Starting complete cloud migration..."
	@chmod +x scripts/complete_cloud_migration.sh
	@./scripts/complete_cloud_migration.sh

cloud-test-supabase: ## Test Supabase connection only
	@echo "🔗 Testing Supabase connection..."
	@python scripts/test_supabase_connection.py

cloud-test-mongodb: ## Test MongoDB Atlas connection only
	@echo "🔗 Testing MongoDB Atlas connection..."
	@python scripts/test_mongodb_atlas.py

cloud-test: ## Test all cloud connections
	@echo "🧪 Testing all cloud connections..."
	@python scripts/test_supabase_connection.py
	@python scripts/test_mongodb_atlas.py
	@python scripts/integration_test.py

cloud-deploy-schema: ## Deploy schema to Supabase
	@echo "🏗️  Deploying schema to Supabase..."
	@python scripts/deploy_to_supabase.py

cloud-migrate-data: ## Migrate test data to cloud
	@echo "📊 Migrating data to cloud..."
	@python scripts/migrate_test_data_to_supabase.py
	@python scripts/migrate_to_mongodb_atlas.py

cloud-verify: ## Verify cloud deployments
	@echo "✅ Verifying cloud deployments..."
	@python scripts/verify_supabase_deployment.py
	@python scripts/verify_mongodb_atlas.py

cloud-status: ## Show cloud resources status
	@echo "🌐 Cloud Resources Status"
	@echo "========================="
	@echo ""
	@echo "📊 PostgreSQL (Supabase):"
	@python -c "import psycopg2, os; from dotenv import load_dotenv; load_dotenv(); conn = psycopg2.connect(host=os.getenv('SUPABASE_DB_HOST'), port=os.getenv('SUPABASE_DB_PORT', 5432), database=os.getenv('SUPABASE_DB_NAME'), user=os.getenv('SUPABASE_DB_USER'), password=os.getenv('SUPABASE_DB_PASSWORD')); cur = conn.cursor(); cur.execute('SELECT COUNT(*) FROM dwh.fact_jobs'); print(f'  ✅ Jobs: {cur.fetchone()[0]}'); conn.close()" 2>/dev/null || echo "  ❌ Connection failed"
	@echo ""
	@echo "🍃 MongoDB (Atlas):"
	@python -c "from pymongo import MongoClient; from pymongo.server_api import ServerApi; import os; from dotenv import load_dotenv; load_dotenv(); client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1')); db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]; print(f'  ✅ Raw Jobs: {db[\"raw_jobs_data\"].count_documents({})}'); client.close()" 2>/dev/null || echo "  ❌ Connection failed"

# ============================================================================
# 開發工具
# ============================================================================

test: ## Run tests
	@echo "🧪 Running tests..."
	@pytest tests/ -v

lint: ## Run linting
	@echo "🔍 Running linting..."
	@flake8 src/ dags/ tests/

format: ## Format code with black
	@echo "🎨 Formatting code..."
	@black src/ dags/ tests/

clean: ## Stop services and clean up
	@echo "🧹 Cleaning up..."
	@docker compose down -v
	@docker system prune -f

build: ## Build or rebuild services
	@docker compose build

# ============================================================================
# 資料庫管理
# ============================================================================

shell-airflow: ## Open shell in Airflow container
	@docker compose exec airflow-webserver bash

shell-postgres: ## Connect to local PostgreSQL
	@docker compose exec postgres-dwh psql -U dwh_user -d job_data_warehouse

shell-mongodb: ## Connect to local MongoDB
	@docker compose exec mongodb mongosh -u admin -p admin123

# ============================================================================
# 本地資料檢查
# ============================================================================

check-local-data: ## Check local database data
	@echo "📊 Local Data Status"
	@echo "==================="
	@echo ""
	@echo "🐘 PostgreSQL:"
	@docker compose exec postgres-dwh psql -U dwh_user -d job_data_warehouse -c "SELECT 'raw_staging' as stage, COUNT(*) FROM raw_staging.linkedin_jobs_raw UNION ALL SELECT 'clean_staging', COUNT(*) FROM clean_staging.jobs_unified UNION ALL SELECT 'business_staging', COUNT(*) FROM business_staging.jobs_final UNION ALL SELECT 'dwh_fact_jobs', COUNT(*) FROM dwh.fact_jobs;" 2>/dev/null || echo "  ❌ Local PostgreSQL not accessible"
	@echo ""
	@echo "🍃 MongoDB:"
	@docker compose exec mongodb mongosh --quiet --eval "use job_market_data; print('  ✅ Raw Jobs: ' + db.raw_jobs_data.countDocuments())" 2>/dev/null || echo "  ❌ Local MongoDB not accessible"

# ============================================================================
# 快速設定
# ============================================================================

quick-start: ## Quick start for development
	@echo "⚡ Quick Start - US Job Data Engineering"
	@echo "========================================"
	@make start
	@sleep 30
	@make check-local-data
	@echo ""
	@echo "🌐 Access URLs:"
	@echo "  📊 Airflow UI:    http://localhost:8080 (admin/admin123)"
	@echo "  🗄️  MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
	@echo "  📈 Grafana:       http://localhost:3000 (admin/admin123)"

env-setup: ## Setup environment file from template
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "📝 Created .env file from template"; \
		echo "⚠️  Please edit .env with your cloud credentials"; \
	else \
		echo "✅ .env file already exists"; \
	fi

# ============================================================================
# 完整工作流程
# ============================================================================

full-setup: ## Complete setup from scratch
	@echo "🎯 Complete Setup - US Job Data Engineering Platform"
	@echo "=================================================="
	@make env-setup
	@make start
	@sleep 30
	@echo ""
	@echo "✅ Local environment ready!"
	@echo "🚀 Next steps:"
	@echo "  1. Edit .env with cloud credentials"
	@echo "  2. Run: make cloud-setup"
	@echo "  3. Start developing scrapers!"

dev-reset: ## Reset development environment
	@echo "🔄 Resetting development environment..."
	@make stop
	@make clean
	@make start
	@sleep 30
	@make check-local-data