#!/bin/bash
# 🚀 Dashboard 啟動腳本 (新版)

echo "🎵 啟動音樂分析 Dashboard"
echo "========================="

# 檢查 Python 環境
if ! command -v python3 &> /dev/null; then
    echo "❌ 找不到 Python3，請先安裝 Python"
    exit 1
fi

# 檢查虛擬環境
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ 使用虛擬環境: $VIRTUAL_ENV"
else
    echo "⚠️  建議使用虛擬環境"
fi

# 檢查套件安裝
echo "🔍 檢查套件安裝..."
if ! python3 -c "import streamlit" &> /dev/null; then
    echo "❌ Streamlit 未安裝"
    echo "正在安裝必要套件..."
    pip install -r dashboard_requirements.txt
fi

# 檢查必要檔案
if [ ! -f "app.py" ]; then
    echo "❌ 找不到 app.py 檔案"
    echo "請確保在正確的目錄中執行此腳本"
    exit 1
fi

# 建立環境變數檔案 (如果不存在)
if [ ! -f ".env.dashboard" ]; then
    echo "⚙️ 建立環境變數檔案..."
    cp .env.dashboard.example .env.dashboard
    echo "✅ 請編輯 .env.dashboard 設定你的資料庫連線"
fi

echo ""
echo "🌐 Dashboard 啟動中..."
echo "   本地網址: http://localhost:8501"
echo "   網路存取: http://$(hostname -I | awk '{print $1}' 2>/dev/null || echo 'localhost'):8501"
echo ""
echo "🛑 按 Ctrl+C 停止服務"
echo ""

# 啟動 Streamlit
# python -m streamlit run app.py
streamlit run app.py \
  --server.address 0.0.0.0 \
  --server.port 8501 \
  --theme.primaryColor "#1f77b4" \
  --theme.backgroundColor "#ffffff" \
  --theme.secondaryBackgroundColor "#f0f2f6" \
  --theme.textColor "#262730"