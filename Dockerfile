FROM python:3.9-slim

WORKDIR /app

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式檔案
COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

# 建立必要目錄
RUN mkdir -p /app/logs /app/data /app/airflow_home

# 設定環境變數
ENV AIRFLOW_HOME=/app/airflow_home
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# 複製啟動腳本
COPY scripts/render_start.sh /app/start.sh
RUN chmod +x /app/start.sh

# 暴露端口
EXPOSE 8080

# 健康檢查 - 檢查 Airflow Webserver
HEALTHCHECK --interval=30s --timeout=10s --start-period=180s --retries=5 \
  CMD curl -f http://localhost:8080/health || exit 1

# 啟動命令
CMD ["/app/start.sh"]
