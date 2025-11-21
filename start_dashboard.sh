#!/bin/bash
# Dashboard Activation Script for Music Analysis Application

echo "Starting Music Analysis Dashboard"
echo "========================="

# Check Python environment
if ! command -v python3 &> /dev/null; then
    echo "Cannot find Python3, please install Python"
    exit 1
fi

# Check virtual environment
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "Using virtual environment: $VIRTUAL_ENV"
else
    echo "It is recommended to use a virtual environment"
fi

# Check package installation
echo "Checking package installation..."
if ! python3 -c "import streamlit" &> /dev/null; then
    echo "Streamlit is not installed"
    echo "Installing required packages..."
    pip install -r dashboard_requirements.txt
fi

# Check necessary files
if [ ! -f "app.py" ]; then
    echo "Cannot find app.py file"
    echo "Please ensure you are running this script in the correct directory"
    exit 1
fi

# Create environment variable file (if not exists)
if [ ! -f ".env.dashboard" ]; then
    echo "Creating environment variable file..."
    cp .env.dashboard.example .env.dashboard
    echo "Please edit .env.dashboard to configure your database connection"
fi

echo ""
echo "Starting Music Analysis Dashboard..."
echo "Local URL: http://localhost:8501"
echo "Network Access: http://$(hostname -I | awk '{print $1}' 2>/dev/null || echo 'localhost'):8501"
echo ""
echo "Press Ctrl+C to stop the service"
echo ""

# Start Streamlit
# python -m streamlit run app.py
streamlit run app.py \
  --server.address 0.0.0.0 \
  --server.port 8501 \
  --theme.primaryColor "#1f77b4" \
  --theme.backgroundColor "#ffffff" \
  --theme.secondaryBackgroundColor "#f0f2f6" \
  --theme.textColor "#262730"