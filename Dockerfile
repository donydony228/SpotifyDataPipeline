FROM python:3.9-slim

WORKDIR /app

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    postgresql-client \
    iputils-ping \
    dnsutils \
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

# 使用修正的啟動腳本
COPY scripts/railway_start_fixed.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080

# 延長健康檢查時間，給 Supabase 連線更多時間
HEALTHCHECK --interval=60s --timeout=30s --start-period=300s --retries=5 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["/app/start.sh"]
