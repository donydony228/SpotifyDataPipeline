#!/bin/bash
# scripts/start_local_airflow.sh
# Local Airflow startup script (without containers)

set -e

echo "Starting Music Data Engineering Platform - Local Mode"
echo "==============================================="

# Check virtual environment
if [ ! -d "venv" ]; then
    echo "Virtual environment not found! Please run: make venv-setup"
    exit 1
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check required packages
echo "Checking package installation..."
python -c "import airflow" 2>/dev/null || {
    echo "Airflow not found in virtual environment. Installing required packages..."
    pip install -r requirements.txt
}

# Set Airflow Home
export AIRFLOW_HOME=$(pwd)/airflow_home
mkdir -p $AIRFLOW_HOME
echo "AIRFLOW_HOME: $AIRFLOW_HOME"

# Load environment variables
echo "Loading environment variables..."
if [ -f .env ]; then
    # Use python-dotenv to load environment variables correctly
    export $(python -c "
from dotenv import load_dotenv
import os
load_dotenv()
for key, value in os.environ.items():
    if any(key.startswith(prefix) for prefix in ['SPOTIFY_', 'SUPABASE_', 'MONGODB_', 'AIRFLOW__']):
        print(f'{key}={value}')
")
    echo "Environment variables loaded from .env"

    # Copy .env to Airflow home as a backup
    cp .env $AIRFLOW_HOME/.env
else
    echo ".env file not found! Please create one with the necessary environment variables."
    echo "Usage: make env-setup"
    exit 1
fi

# Check required environment variables
echo "Checking environment variables..."
python -c "
import os
spotify_vars = ['SPOTIFY_CLIENT_ID', 'SPOTIFY_CLIENT_SECRET']
missing = []
for var in spotify_vars:
    if not os.getenv(var):
        missing.append(var)
if missing:
    print(f'Missing environment variables: {missing}')
    exit(1)
else:
    print('Spotify environment variables are set correctly.')
"

# Set Airflow configuration
echo "Setting Airflow configuration..."
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# Check if Airflow is already running
if pgrep -f "airflow standalone" > /dev/null; then
    echo "Airflow is already running"
    echo "Access: http://localhost:8080"
    echo "Username: admin / Password: admin"
    exit 0
fi

# Initialize database (if needed)
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init
fi

echo "Starting Airflow standalone..."
echo "This will start both the webserver and scheduler"

# Create logs directory
mkdir -p $AIRFLOW_HOME/logs

# Start Airflow standalone (background)
nohup airflow standalone > $AIRFLOW_HOME/logs/standalone.log 2>&1 &
AIRFLOW_PID=$!

echo "Waiting for Airflow to start..."
echo "Process ID: $AIRFLOW_PID"

# Wait for startup to complete
for i in {1..30}; do
    echo -n "."
    sleep 2
    
    # Check if Airflow process is still running
    if ! kill -0 $AIRFLOW_PID 2>/dev/null; then
        echo ""
        echo "Airflow process exited unexpectedly"
        echo "Error log:"
        tail -20 $AIRFLOW_HOME/logs/standalone.log
        exit 1
    fi

    # Check if web server is up
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo ""
        echo "Airflow started successfully!"
        break
    fi
done

# Final status check
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo ""
    echo "Music Data Engineering Platform is up and running!"
    echo "=============================="
    echo "Web UI: http://localhost:8080"
    echo ""
else
    echo ""
    echo "Airflow startup is taking longer than expected, please wait..."
    echo "Check logs: tail -f $AIRFLOW_HOME/logs/standalone.log"
    echo "Check status manually: make dev-status"
fi

echo ""
echo "Startup script execution completed"