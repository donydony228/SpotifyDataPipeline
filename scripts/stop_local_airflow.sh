#!/bin/bash
# scripts/stop_local_airflow.sh
# Local Airflow stop script

echo "Starting to stop local Airflow services..."

# Find and stop all Airflow related processes
AIRFLOW_PIDS=$(pgrep -f "airflow")

if [ -z "$AIRFLOW_PIDS" ]; then
    echo "No running Airflow processes found"
else
    echo "Found Airflow processes: $AIRFLOW_PIDS"

    # Gracefully stop processes
    echo "Stopping Airflow processes..."
    for pid in $AIRFLOW_PIDS; do
        echo "  Stopping process $pid..."
        kill $pid 2>/dev/null || true
    done

    # Wait for processes to stop
    sleep 3

    # Check if any processes are still running
    REMAINING_PIDS=$(pgrep -f "airflow" || true)
    if [ -n "$REMAINING_PIDS" ]; then
        echo "Forcefully terminating remaining processes: $REMAINING_PIDS"
        for pid in $REMAINING_PIDS; do
            kill -9 $pid 2>/dev/null || true
        done
    fi

    echo "Airflow processes have been stopped"
fi

# Check if port is still in use
if lsof -i :8080 > /dev/null 2>&1; then
    echo "Port 8080 is still in use"
    PORT_PID=$(lsof -ti :8080)
    if [ -n "$PORT_PID" ]; then
        echo "Terminating process $PORT_PID occupying port 8080"
        kill -9 $PORT_PID 2>/dev/null || true
    fi
fi

# Final status check
sleep 1
if pgrep -f "airflow" > /dev/null; then
    echo "Some Airflow processes may still be running"
    echo "Remaining processes:"
    pgrep -f "airflow" -l || true
else
    echo "All Airflow processes have been stopped"
fi

if ! lsof -i :8080 > /dev/null 2>&1; then
    echo "Port 8080 has been released"
else
    echo "Port 8080 is still in use"
fi

echo "Stopping completed"