#!/bin/bash

echo "🚀 Railway 自動部署腳本"
echo "======================="

# 檢查必要工具
if ! command -v railway &> /dev/null; then
    echo "❌ Railway CLI 未安裝"
    echo "📥 安裝指令: npm install -g @railway/cli"
    exit 1
fi

if ! command -v git &> /dev/null; then
    echo "❌ Git 未安裝"
    exit 1
fi

# 檢查環境
if [ ! -f ".env" ]; then
    echo "❌ .env 檔案不存在"
    echo "💡 請先完成雲端遷移設定"
    exit 1
fi

echo "✅ 環境檢查通過"

# 準備部署
echo "📦 準備 Railway 部署..."
python scripts/prepare_railway_deployment.py

if [ $? -ne 0 ]; then
    echo "❌ 部署準備失敗"
    exit 1
fi

# 提交程式碼
echo "📝 提交程式碼到 Git..."
git add .
git commit -m "Railway deployment preparation" || echo "沒有新的變更要提交"
git push origin main

# Railway 登入檢查
echo "🔑 檢查 Railway 登入狀態..."
if ! railway whoami &> /dev/null; then
    echo "🔐 需要登入 Railway..."
    railway login
fi

# 建立或連接專案
echo "🏗️  建立 Railway 專案..."
if [ ! -f "railway.json" ]; then
    echo "❌ railway.json 配置檔不存在"
    exit 1
fi

# 部署
echo "🚀 開始部署到 Railway..."
railway up

echo ""
echo "🎉 部署完成！"
echo ""
echo "📋 下一步："
echo "  1. 等待 2-3 分鐘讓服務完全啟動"
echo "  2. 檢查 Railway 儀表板的日誌"
echo "  3. 存取 Airflow UI 並測試功能"
echo ""
echo "🌐 使用 'railway open' 開啟專案 URL"
echo "👤 Airflow 登入: admin / admin123"