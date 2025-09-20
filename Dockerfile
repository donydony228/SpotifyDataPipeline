FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ curl postgresql-client \
    ca-certificates socat && \
    rm -rf /var/lib/apt/lists/* && \
    update-ca-certificates

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt requests

COPY dags/ ./dags/
COPY src/ ./src/
COPY config/ ./config/
COPY sql/ ./sql/
COPY scripts/ ./scripts/

RUN mkdir -p /app/logs /app/data /app/airflow_home

ENV AIRFLOW_HOME=/app/airflow_home

COPY scripts/railway_start_fixed.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080

HEALTHCHECK --interval=60s --timeout=30s --start-period=180s --retries=5 \
  CMD curl -f http://localhost:8080/health || curl -f http://localhost:8080/ || exit 1

CMD ["/app/start.sh"]
