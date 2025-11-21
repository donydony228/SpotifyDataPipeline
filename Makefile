.PHONY: help airflow-start airflow-stop airflow-restart airflow-status airflow-logs clean-airflow \
	     env-setup venv-setup db-test spotify-test dag-test \
	     clean-logs clean-pyc clean-all dev-start info

help: ## Show this help message
	@echo 'Spotify Music Analytics Platform'
	@echo '================================'
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ============================================================================
# Airflow Service Management
# ============================================================================

airflow-start: ## Start Airflow (webserver + scheduler)
	@echo "Starting Airflow service..."
	@if pgrep -f "airflow" > /dev/null; then \
		echo "Airflow is already running, stopping existing service..."; \
		make airflow-stop; \
		sleep 3; \
	fi
	@echo "Setting AIRFLOW_HOME=$(PWD)"
	@export AIRFLOW_HOME=$(PWD) && \
	source venv/bin/activate && \
	echo "Starting Webserver (background)..." && \
	airflow webserver --port 8080 --daemon && \
	sleep 5 && \
	echo "Starting Scheduler (background)..." && \
	airflow scheduler --daemon && \
	sleep 3
	@echo ""
	@make airflow-status

airflow-stop: ## Stop all Airflow services
	@echo "Stopping Airflow service..."
	@pkill -f "airflow webserver" 2>/dev/null || true
	@pkill -f "airflow scheduler" 2>/dev/null || true
	@pkill -f "gunicorn.*airflow" 2>/dev/null || true
	@sleep 2
	@if pgrep -f "airflow" > /dev/null; then \
		echo "Force stopping lingering processes..."; \
		pkill -9 -f "airflow" 2>/dev/null || true; \
	fi
	@echo "Airflow service has been stopped"

airflow-restart: ## Restart Airflow service
	@make airflow-stop
	@sleep 2
	@make airflow-start

airflow-status: ## Check Airflow service status
	@echo "Airflow service status"
	@echo "==================="
	@if pgrep -f "airflow webserver" > /dev/null; then \
		echo "🌐 Webserver: Running"; \
		echo "   URL: http://localhost:8080"; \
		echo "   Username: admin / Password: admin123"; \
	else \
		echo "🌐 Webserver: Not running"; \
	fi
	@if pgrep -f "airflow scheduler" > /dev/null; then \
		echo "📅 Scheduler: Running"; \
	else \
		echo "📅 Scheduler: Not running"; \
	fi
	@echo ""
	@if curl -s http://localhost:8080/health > /dev/null 2>&1; then \
		echo "Connection Test: Webserver is reachable"; \
	else \
		echo "Connection Test: Webserver is not reachable"; \
	fi

airflow-logs: ## View Airflow logs (tail -f)
	@echo "Airflow logs (Press Ctrl+C to exit)"
	@echo "=========================="
	@if [ -f "logs/scheduler/latest/*.log" ]; then \
		tail -f logs/scheduler/latest/*.log; \
	else \
		echo "Scheduler log file does not exist, showing DAG processing logs..."; \
		find logs -name "*.log" -type f -exec tail -20 {} \; 2>/dev/null || echo "No log files found"; \
	fi

clean-airflow: ## Clean Airflow environment (DB, logs, configs)
	@echo "Cleaning Airflow environment..."
	@make airflow-stop
	@rm -f airflow.db airflow.cfg
	@rm -rf logs/*
	@rm -f webserver_config.py
	@echo "Airflow environment has been cleaned"

# ============================================================================
# Environment Setup and Testing
# ============================================================================

env-setup: ## Create .env file from template
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env file from template"; \
		echo "Please edit .env to include your Spotify credentials"; \
	else \
		echo ".env file already exists"; \
	fi

venv-setup: ## Set up Python virtual environment
	@echo "Creating Python virtual environment..."
	@if [ ! -d "venv" ]; then \
		python3.11 -m venv venv; \
		echo "Installing packages..."; \
		./venv/bin/pip install --upgrade pip; \
		./venv/bin/pip install -r requirements.txt; \
		echo "Virtual environment setup complete"; \
	else \
		echo "Virtual environment already exists"; \
	fi
	@echo "Activate the virtual environment: source venv/bin/activate"

db-test: ## Test database connection
	@echo "Testing database connection..."
	@source venv/bin/activate && python -c "\
	from utils.database import test_mongodb_connection, test_postgresql_connection; \
	print('MongoDB:', 'Success' if test_mongodb_connection() else 'Failed'); \
	print('PostgreSQL:', 'Success' if test_postgresql_connection() else 'Failed')"

spotify-test: ## Test Spotify API connection
	@echo "🎵 Testing Spotify API..."
	@source venv/bin/activate && python -c "\
	import os; \
	from dotenv import load_dotenv; \
	load_dotenv(); \
	required = ['SPOTIFY_CLIENT_ID', 'SPOTIFY_CLIENT_SECRET', 'SPOTIFY_REFRESH_TOKEN']; \
	missing = [var for var in required if not os.getenv(var)]; \
	if missing: \
		print(f'Missing environment variables: {missing}'); \
	else: \
		print('All required Spotify environment variables are set')"

dag-test: ## Test DAG loading
	@echo "Testing DAG loading..."
	@source venv/bin/activate && \
	export AIRFLOW_HOME=$(PWD) && \
	python -c "\
	import sys; \
	sys.path.append('$(PWD)'); \
	try: \
		from dags.spotify.daily_etl_pipeline import dag as dag1; \
		print('daily_etl_pipeline.py loaded successfully'); \
	except Exception as e: \
		print(f'daily_etl_pipeline.py failed to load: {e}'); \
	try: \
		from dags.spotify.curl_spotify_tracker import dag as dag2; \
		print('curl_spotify_tracker.py loaded successfully'); \
	except Exception as e: \
		print(f'curl_spotify_tracker.py failed to load: {e}')"

# ============================================================================
# Cleaning Temporary Files
# ============================================================================

clean-logs: ## Clean log files
	@echo "Cleaning log files..."
	@rm -rf logs/*
	@echo "Log files have been cleaned"

clean-pyc: ## Clean Python bytecode files
	@echo "🧹 Cleaning Python bytecode files..."
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -delete
	@echo "Python bytecode files have been cleaned"

clean-all: clean-airflow clean-logs clean-pyc ## Clean all temporary files
	@echo "All temporary files have been cleaned"

# ============================================================================
# Development Environment Management
# ============================================================================

dev-start: ## Start development environment
	@echo "⚡ Starting development environment"
	@echo "=================="
	@make env-setup
	@make venv-setup
	@echo ""
	@echo "Testing environment..."
	@make spotify-test
	@make db-test
	@echo ""
	@echo "Starting Airflow..."
	@make airflow-start
	@echo ""
	@echo "Development environment started successfully!"
	@echo "Access http://localhost:8080"
	@echo "Check status: make airflow-status"
	@echo "Check logs: make airflow-logs"

dev-stop: ## Stop development environment
	@echo "Stopping development environment..."
	@make airflow-stop
	@echo "Development environment stopped"

# ============================================================================
# Information Display
# ============================================================================

info: ## Display project and environment information
	@echo "Spotify Music Analytics Platform"
	@echo "===================================="
	@echo ""
	@echo "Project Directory: $(PWD)"
	@echo "Python Version: $(shell python3 --version 2>/dev/null || echo 'Not found')"
	@echo "Airflow Version: $(shell source venv/bin/activate && airflow version 2>/dev/null || echo 'Not installed')"
	@echo ""
	@echo "Important Directories:"
	@echo "  • DAGs: dags/spotify/"
	@echo "  • Utils: utils/"
	@echo "  • Environment Variables: .env"
	@echo "  • Virtual Environment: venv/"
	@echo "  • Logs: logs/"
	@echo ""
	@echo "DAG List:"
	@echo "  • daily_etl_pipeline - Basic ETL Pipeline"
	@echo "  • enhanced_spotify_tracker - Comprehensive Music Tracking"
	@echo ""
	@echo "Cloud Services:"
	@echo "  • PostgreSQL: Supabase"
	@echo "  • MongoDB: MongoDB Atlas"
	@echo "  • Music API: Spotify Web API"
	@echo ""
	@echo "Common Commands:"
	@echo "  make dev-start      - Start development environment"
	@echo "  make airflow-status - Check Airflow status"
	@echo "  make airflow-logs   - View real-time logs"
	@echo "  make db-test        - Test database connection"
	@echo "  make help           - Show all commands"