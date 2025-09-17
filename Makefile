.PHONY: help start stop restart logs test lint format clean

help: ## Show this help message
	@echo 'US Job Data Engineering Platform'
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

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

shell-airflow: ## Open shell in Airflow container
	@docker compose exec airflow-webserver bash

shell-postgres: ## Connect to PostgreSQL
	@docker compose exec postgres-dwh psql -U dwh_user -d job_data_warehouse