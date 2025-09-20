# 🚀 Render 部署檢查清單

## ✅ 準備階段

### 本地準備
- [ ] 程式碼已推送到 GitHub
- [ ] Dockerfile 已更新為 Render 版本
- [ ] 啟動腳本已建立 (scripts/render_start.sh)
- [ ] 環境變數檔案已準備 (render_environment_variables.txt)

### 雲端資料庫確認
- [ ] Supabase PostgreSQL 正常運行
- [ ] MongoDB Atlas 正常運行
- [ ] 本地可以正常連接雲端資料庫

## 🌐 Render 部署步驟

### 1. 建立 Render 服務
1. 前往 https://render.com
2. 點擊 "New +" → "Web Service"
3. 連接你的 GitHub repository
4. 選擇 repository: `us-job-market-data-engineering`

### 2. 基本設定
- **Name**: `us-job-data-platform` (或你喜歡的名稱)
- **Environment**: `Docker`
- **Region**: `Oregon (US West)` 或 `Ohio (US East)`
- **Branch**: `main`

### 3. 進階設定
- **Dockerfile Path**: `./Dockerfile` (預設值)
- **Auto-Deploy**: `Yes`

### 4. 環境變數設定
在 "Environment" 頁面加入以下變數：

```
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__CORE__FERNET_KEY=render-fernet-key-32-chars-long!!
AIRFLOW__WEBSERVER__SECRET_KEY=render-secret-key
ENVIRONMENT=production
DEPLOYMENT_PLATFORM=render

# 從 render_environment_variables.txt 複製 Supabase 和 MongoDB 設定
SUPABASE_DB_URL=postgresql://postgres:[password]@db.xxx.supabase.co:5432/postgres
MONGODB_ATLAS_URL=mongodb+srv://...
MONGODB_ATLAS_DB_NAME=job_market_data
```

### 5. 部署
- 點擊 "Create Web Service"
- 等待建置和部署完成 (約 5-10 分鐘)

## 🔍 部署後驗證

### 檢查部署狀態
- [ ] 部署成功 (綠色狀態)
- [ ] 服務正在運行
- [ ] 沒有錯誤日誌

### 測試 Airflow
- [ ] 能夠存取 Airflow UI (`https://你的應用名.onrender.com`)
- [ ] 能夠登入 (admin / admin123)
- [ ] 可以看到 DAGs 列表
- [ ] hello_world_dag 存在並可執行

### 測試資料庫連線
- [ ] 檢查日誌中的資料庫連線狀態
- [ ] 如果使用 Supabase，確認連線成功
- [ ] 如果降級到 SQLite，確認正常運作

## 🚨 故障排除

### 常見問題
1. **部署失敗**
   - 檢查 Dockerfile 語法
   - 確認所有檔案都已推送到 GitHub

2. **服務無法啟動**
   - 檢查 Render 日誌
   - 確認環境變數設定正確

3. **無法存取 Airflow UI**
   - 確認服務正在運行
   - 檢查健康檢查狀態

4. **資料庫連線失敗**
   - 確認 Supabase URL 正確
   - 檢查網路連線日誌
   - 確認會自動降級到 SQLite

### 預期結果
✅ **成功標準**：
- Render 部署成功
- Airflow UI 可正常存取
- 管理員帳號可正常登入
- DAGs 可正常顯示和執行
- 資料庫連線正常 (Supabase 或 SQLite)

## 📞 下一步
部署成功後：
1. 測試現有 DAGs
2. 開發第一個爬蟲
3. 設定監控和告警
4. 逐步完善 ETL Pipeline
