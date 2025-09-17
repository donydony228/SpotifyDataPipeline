#!/bin/bash

if [ "$1" = "" ]; then
    echo "📜 Showing all service logs..."
    docker compose logs -f
elif [ "$1" = "airflow" ]; then
    echo "📜 Showing Airflow logs..."
    docker compose logs -f airflow-webserver airflow-scheduler
elif [ "$1" = "db" ]; then
    echo "📜 Showing database logs..."
    docker compose logs -f postgres-airflow postgres-dwh
else
    echo "📜 Showing logs for: $1"
    docker compose logs -f $1
fi

echo ""
echo "💡 Usage examples:"
echo "  ./logs_docker.sh              - All logs"
echo "  ./logs_docker.sh airflow       - Airflow logs only"
echo "  ./logs_docker.sh db            - Database logs only"
echo "  ./logs_docker.sh minio         - MinIO logs only"