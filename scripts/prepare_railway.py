import os
from dotenv import load_dotenv

load_dotenv()

print("🚀 Railway 部署準備")
print("=" * 30)

# 環境變數列表
env_vars = {
    "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO",
    "AIRFLOW__CORE__FERNET_KEY": "railway-fernet-key-32-chars-long!",
    "AIRFLOW__WEBSERVER__SECRET_KEY": "railway-secret-key",
    "SUPABASE_DB_URL": os.getenv('SUPABASE_DB_URL'),
    "MONGODB_ATLAS_URL": os.getenv('MONGODB_ATLAS_URL'),
    "MONGODB_ATLAS_DB_NAME": os.getenv('MONGODB_ATLAS_DB_NAME'),
    "ENVIRONMENT": "production",
    "DEPLOYMENT_PLATFORM": "railway"
}

print("📋 Railway 環境變數設定:")
print("-" * 30)

# 保存到檔案供複製使用
with open('railway_env_vars.txt', 'w') as f:
    f.write("# Railway 環境變數設定\n\n")
    for key, value in env_vars.items():
        if value:
            if any(word in key for word in ['URL', 'SECRET', 'KEY']):
                display = f"{value[:20]}***"
            else:
                display = value
            print(f"{key}={display}")
            f.write(f"{key}={value}\n")

print(f"\n✅ 完整環境變數已保存到 railway_env_vars.txt")
print("\n🌐 下一步:")
print("1. 前往 https://railway.app")
print("2. 建立新專案 -> Deploy from GitHub")
print("3. 選擇你的 repository")
print("4. 在 Variables 頁面貼上 railway_env_vars.txt 的內容")
print("5. 等待部署完成")
