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
# Airflow Service Management (Updated for SequentialExecutor)
# ============================================================================

airflow-start: ## Start Airflow (webserver + scheduler)
	@echo "Starting Airflow service with SequentialExecutor..."
	@if pgrep -f "airflow" > /dev/null; then \
		echo "Airflow is already running, stopping existing service..."; \
		make airflow-stop; \
		sleep 3; \
	fi
	@echo "Setting AIRFLOW_HOME=$(PWD)"
	@export AIRFLOW_HOME=$(PWD) && \
	export AIRFLOW__CORE__EXECUTOR=SequentialExecutor && \
	export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(PWD)/airflow.db && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags && \
	export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True && \
	export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO && \
	source venv/bin/activate && \
	echo "Starting Airflow standalone..." && \
	nohup airflow standalone > airflow.log 2>&1 & echo $$! > airflow.pid
	@sleep 8
	@echo ""
	@make airflow-status

airflow-stop: ## Stop all Airflow services
	@echo "Stopping Airflow service..."
	@pkill -f "airflow webserver" 2>/dev/null || true
	@pkill -f "airflow scheduler" 2>/dev/null || true
	@pkill -f "airflow standalone" 2>/dev/null || true
	@pkill -f "gunicorn.*airflow" 2>/dev/null || true
	@sleep 2
	@if pgrep -f "airflow" > /dev/null; then \
		echo "Force stopping lingering processes..."; \
		pkill -9 -f "airflow" 2>/dev/null || true; \
	fi
	@rm -f airflow.pid
	@echo "Airflow service has been stopped"

airflow-restart: ## Restart Airflow service
	@make airflow-stop
	@sleep 2
	@make airflow-start

airflow-status: ## Check Airflow service status
	@echo "Airflow service status"
	@echo "==================="
	@if pgrep -f "airflow" > /dev/null; then \
		echo "🌐 Airflow: Running"; \
		echo "   URL: http://localhost:8080"; \
		echo "   Username: admin / Password: admin123"; \
	else \
		echo "🌐 Airflow: Not running"; \
	fi
	@echo ""
	@if curl -s http://localhost:8080/health > /dev/null 2>&1; then \
		echo "Connection Test: Airflow is reachable"; \
	else \
		echo "Connection Test: Airflow is not reachable"; \
	fi
	@echo ""
	@echo "Current Configuration:"
	@export AIRFLOW_HOME=$(PWD) && airflow config get-value core executor 2>/dev/null | sed 's/^/  Executor: /' || echo "  Executor: Not available"
	@export AIRFLOW_HOME=$(PWD) && airflow config get-value database sql_alchemy_conn 2>/dev/null | sed 's/^/  Database: /' || echo "  Database: Not available"

airflow-logs: ## View Airflow logs (tail -f)
	@echo "Airflow logs (Press Ctrl+C to exit)"
	@echo "=========================="
	@if [ -f "airflow.log" ]; then \
		tail -f airflow.log; \
	elif [ -f "logs/scheduler/latest/*.log" ]; then \
		tail -f logs/scheduler/latest/*.log; \
	else \
		echo "No log files found. Try: make airflow-start"; \
	fi

clean-airflow: ## Clean Airflow environment (DB, logs, configs)
	@echo "Cleaning Airflow environment..."
	@make airflow-stop
	@rm -f airflow.db airflow.cfg airflow.log airflow.pid
	@rm -rf logs/*
	@rm -f webserver_config.py
	@rm -f standalone_admin_password.txt
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
	@export AIRFLOW_HOME=$(PWD) && \
	export AIRFLOW__CORE__EXECUTOR=SequentialExecutor && \
	export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(PWD)/airflow.db && \
	export AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags && \
	source venv/bin/activate && \
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
	@rm -f airflow.log
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
	@echo "Ensuring Airflow database is initialized..."
	@export AIRFLOW_HOME=$(PWD) && \
	export AIRFLOW__CORE__EXECUTOR=SequentialExecutor && \
	export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(PWD)/airflow.db && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags && \
	source venv/bin/activate && \
	if [ ! -f "airflow.db" ]; then \
		echo "Initializing Airflow database..."; \
		airflow db init; \
		echo "Creating admin user..."; \
		airflow users create \
			--username admin \
			--firstname Admin \
			--lastname User \
			--role Admin \
			--email admin@example.com \
			--password admin123 || true; \
	fi
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
	@echo "Username: admin / Password: admin123"
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
	@echo "Airflow Configuration:"
	@export AIRFLOW_HOME=$(PWD) && airflow config get-value core executor 2>/dev/null | sed 's/^/  Executor: /' || echo "  Executor: Not configured"
	@export AIRFLOW_HOME=$(PWD) && airflow config get-value database sql_alchemy_conn 2>/dev/null | sed 's/^/  Database: /' || echo "  Database: Not configured"
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
