FROM python:3.9-slim

WORKDIR /app

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 複製並安裝依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式檔案
COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

# 建立目錄
RUN mkdir -p /app/logs /app/data /app/airflow_home

# 環境變數
ENV AIRFLOW_HOME=/app/airflow_home
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/dags

# 啟動腳本
COPY scripts/railway_start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["/app/start.sh"]
