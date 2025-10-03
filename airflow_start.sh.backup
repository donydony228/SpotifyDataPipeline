#!/bin/bash
echo "🌊 Starting Airflow Standalone..."

if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "❌ Please activate virtual environment first: source start_dev.sh"
    exit 1
fi

if [[ "$AIRFLOW_HOME" == "" ]]; then
    export AIRFLOW_HOME=$(pwd)/airflow_home
fi

# 重新載入 .env
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# 使用 SQLite for Airflow metadata
echo "📁 Using SQLite for Airflow metadata..."
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# 確認環境變數
if [[ -n "$SUPABASE_DB_URL" ]]; then
    echo "✓ SUPABASE_DB_URL is set: ${SUPABASE_DB_URL:0:60}..."
else
    echo "⚠️ SUPABASE_DB_URL not found"
fi

echo "🚀 Starting Airflow at http://localhost:8080"
airflow standalone