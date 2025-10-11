import os
from dotenv import load_dotenv
import socket

load_dotenv()

def generate_railway_env():
    """生成修正的 Railway 環境變數"""
    
    # 基礎環境變數
    env_vars = {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO",
        "AIRFLOW__CORE__FERNET_KEY": "railway-fernet-key-32-chars-long!",
        "AIRFLOW__WEBSERVER__SECRET_KEY": "railway-secret-key",
        "ENVIRONMENT": "production",
        "DEPLOYMENT_PLATFORM": "railway"
    }
    
    # Supabase 連線修正
    supabase_url = os.getenv('SUPABASE_DB_URL')
    mongodb_url = os.getenv('MONGODB_ATLAS_URL')
    mongodb_db = os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')
    
    if supabase_url:
        # 嘗試修正 Supabase URL
        try:
            from urllib.parse import urlparse
            parsed = urlparse(supabase_url)
            
            # 方法1: 加上 SSL 參數
            ssl_url = f"{supabase_url}?sslmode=require&connect_timeout=10"
            env_vars["SUPABASE_DB_URL"] = ssl_url
            
            # 方法2: 分別設定參數（Railway 偏好）
            env_vars["SUPABASE_DB_HOST"] = parsed.hostname
            env_vars["SUPABASE_DB_PORT"] = str(parsed.port or 5432)
            env_vars["SUPABASE_DB_NAME"] = parsed.path.lstrip('/')
            env_vars["SUPABASE_DB_USER"] = parsed.username
            env_vars["SUPABASE_DB_PASSWORD"] = parsed.password
            
            print("✅ Supabase 連線參數已修正")
            
        except Exception as e:
            print(f"⚠️ Supabase URL 解析失敗: {e}")
    
    if mongodb_url:
        env_vars["MONGODB_ATLAS_URL"] = mongodb_url
        env_vars["MONGODB_ATLAS_DB_NAME"] = mongodb_db
        print("✅ MongoDB 參數已設定")
    
    return env_vars

def main():
    print("🔧 生成修正的 Railway 環境變數...")
    
    env_vars = generate_railway_env()
    
    # 保存 Railway 指令
    with open('railway_env_commands_fixed.txt', 'w') as f:
        f.write("# 修正的 Railway 環境變數設定指令\n")
        f.write("# 在 Railway 控制台的 Variables 頁面設定這些變數\n\n")
        
        for key, value in env_vars.items():
            if value:
                f.write(f'{key}={value}\n')
    
    print("📋 Railway 環境變數指令:")
    print("=" * 50)
    
    for key, value in env_vars.items():
        if value:
            # 隱藏敏感資訊
            if any(word in key for word in ['PASSWORD', 'SECRET', 'KEY', 'URL']):
                display_value = f"{value[:20]}***" if len(value) > 20 else "***"
            else:
                display_value = value
            print(f'{key}={display_value}')
    
    print(f"\n✅ 完整指令已保存到 railway_env_commands_fixed.txt")
    print("\n🚀 修正步驟:")
    print("1. 複製新的啟動腳本到專案")
    print("2. 更新 Railway 環境變數")
    print("3. 重新部署")

if __name__ == "__main__":
    main()
