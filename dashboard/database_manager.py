#!/usr/bin/env python3
import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from dotenv import load_dotenv
import warnings
import numpy as np

load_dotenv()

class SupabaseManager:
    """
    Supabase PostgreSQL Manager for handling database connections and queries.
    """
    
    def __init__(self):
        """Initialize database connection parameters."""
        self.connection_string = os.getenv('SUPABASE_DB_URL')
        
        if not self.connection_string:
            self.db_config = {
                'host': os.getenv('SUPABASE_HOST', 'localhost'),
                'port': os.getenv('SUPABASE_PORT', '5432'),
                'database': os.getenv('SUPABASE_DATABASE', 'postgres'),
                'user': os.getenv('SUPABASE_USER', 'postgres'),
                'password': os.getenv('SUPABASE_PASSWORD', '')
            }
        
        self.connection = None
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Set up the logger."""
        logger = logging.getLogger('SupabaseManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def connect(self):
        """Establish database connection."""
        try:
            if self.connection_string:
                self.connection = psycopg2.connect(
                    self.connection_string,
                    cursor_factory=RealDictCursor
                )
            else:
                self.connection = psycopg2.connect(
                    **self.db_config,
                    cursor_factory=RealDictCursor
                )
            return True
            
        except Exception as e:
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def test_connection(self):
        """Test database connection."""
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                
                if result and result['test'] == 1:
                    return True
                else:
                    return False
                    
        except Exception as e:
            self.logger.error(f"ERROR: Database connection test failed: {e}")
            return False
        finally:
            self.disconnect()

    def execute_query(self, query, params=None):
        if not self.connect():
            return pd.DataFrame()
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(results, columns=columns)
    
            return df
            
        except Exception as e:
            self.logger.error(f"ERROR: Database query execution failed: {e}")
            return pd.DataFrame()
        finally:
            self.disconnect()