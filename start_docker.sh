#!/bin/bash

echo "🚀 Starting US Job Data Engineering Platform with Docker"

# 檢查 Docker 是否安裝
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    echo "📥 Download Docker Desktop: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# 檢查 Docker 是否運行
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    echo "🚀 Starting Docker Desktop..."
    open /Applications/Docker.app
    echo "⏳ Waiting for Docker to start (this may take a minute)..."
    
    # 等待 Docker 啟動
    for i in {1..30}; do
        if docker info &> /dev/null; then
            echo "✅ Docker is now running!"
            break
        fi
        echo "   Waiting... ($i/30)"
        sleep 2
    done
    
    if ! docker info &> /dev/null; then
        echo "❌ Docker failed to start. Please start Docker Desktop manually."
        exit 1
    fi
fi

# 檢查 Docker Compose 是否可用
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "✅ Docker and Docker Compose are ready!"

# 建立 .env 檔案 (如果不存在)
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your configurations"
fi

# 建立必要的目錄
echo "📁 Creating necessary directories..."
mkdir -p data/raw data/processed data/logs
mkdir -p sql/ddl sql/migrations

# 停止現有的 Airflow 進程 (避免衝突)
echo "🛑 Stopping existing Airflow processes..."
./stop_airflow.sh 2>/dev/null || true

# 啟動服務
echo "🐳 Starting Docker services..."

# 先啟動資料庫相關服務
echo "1️⃣  Starting databases..."
docker-compose up -d postgres-airflow postgres-dwh mongodb redis minio

# 等待資料庫啟動
echo "⏳ Waiting for databases to start..."
sleep 20

# 初始化 Airflow
echo "2️⃣  Initializing Airflow..."
docker-compose up airflow-init

# 啟動 Airflow 服務
echo "3️⃣  Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

# 啟動監控服務
echo "4️⃣  Starting monitoring services..."
docker-compose up -d grafana

# 等待服務啟動
echo "⏳ Waiting for services to be ready..."
sleep 30

# 檢查服務狀態
echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "✅ US Job Data Engineering Platform started successfully!"
echo ""
echo "🌐 Access URLs:"
echo "  📊 Airflow UI:    http://localhost:8080 (admin/admin123)"
echo "  🗄️  MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "  📈 Grafana:       http://localhost:3000 (admin/admin123)"
echo ""
echo "🔗 Database Connections:"
echo "  🐘 Airflow DB:    localhost:5432"
echo "  🏛️  Data Warehouse: localhost:5433"
echo "  🍃 MongoDB:       localhost:27017"
echo "  ⚡ Redis:         localhost:6379"
echo ""
echo "📜 Useful commands:"
echo "  ./stop_docker.sh     - Stop all services"
echo "  ./logs_docker.sh     - View logs"
echo "  docker-compose ps    - Check service status"