#!/bin/bash
# Script to set Airflow environment variables from .env file

if [ -f ".env" ]; then
    echo "Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)

    echo "Environment variables set:"
    echo "   SUPABASE_DB_URL: ${SUPABASE_DB_URL:0:30}***"
    echo "   MONGODB_ATLAS_URL: ${MONGODB_ATLAS_URL:0:30}***"
    echo "   MONGODB_ATLAS_DB_NAME: $MONGODB_ATLAS_DB_NAME"
else
    echo ".env file not found! Please create one with the necessary environment variables."
    exit 1
fi
