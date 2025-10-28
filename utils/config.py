"""
Configuration utilities for Spotify Music Analytics ETL
Handles environment variables and configuration settings
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Spotify API Configuration
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8080/callback')

# Database Configuration
MONGODB_ATLAS_URL = os.getenv('MONGODB_ATLAS_URL')
MONGODB_ATLAS_DB_NAME = os.getenv('MONGODB_ATLAS_DB_NAME', 'music_data')
SUPABASE_DB_URL = os.getenv('SUPABASE_DB_URL')

# Airflow Configuration
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/Users/desmond/airflow')

def validate_config():
    """Validate that required configuration is present"""
    required_vars = {
        'MONGODB_ATLAS_URL': MONGODB_ATLAS_URL,
        'SUPABASE_DB_URL': SUPABASE_DB_URL,
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return True

def get_database_config():
    """Get database configuration"""
    return {
        'mongodb_url': MONGODB_ATLAS_URL,
        'mongodb_db': MONGODB_ATLAS_DB_NAME,
        'postgres_url': SUPABASE_DB_URL
    }

def get_spotify_config():
    """Get Spotify API configuration"""
    return {
        'client_id': SPOTIFY_CLIENT_ID,
        'client_secret': SPOTIFY_CLIENT_SECRET,
        'redirect_uri': SPOTIFY_REDIRECT_URI
    }

def load_config():
    """Load and return all configuration"""
    validate_config()
    
    return {
        'database': get_database_config(),
        'spotify': get_spotify_config(),
        'airflow_home': AIRFLOW_HOME
    }
