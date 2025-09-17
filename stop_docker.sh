#!/bin/bash

echo "🛑 Stopping US Job Data Engineering Platform"

# 停止所有服務
docker-compose down

echo "✅ All services stopped"

# 顯示選項
echo ""
echo "📋 Additional options:"
echo "  docker-compose down -v    - Stop and remove volumes (⚠️  will delete all data)"
echo "  docker system prune       - Clean up Docker resources"