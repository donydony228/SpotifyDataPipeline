#!/bin/bash

echo "🔧 修復 Railway 部署配置..."

# 1. 更新 Dockerfile
cat > Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

RUN mkdir -p /app/logs /app/data /app/airflow_home

ENV AIRFLOW_HOME=/app/airflow_home
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/dags

COPY scripts/railway_start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080

# 修復健康檢查
HEALTHCHECK --interval=60s --timeout=30s --start-period=300s --retries=5 \
  CMD curl -f http://localhost:8080/api/v1/health || exit 1

CMD ["/app/start.sh"]
EOF