"""
Database connection utilities for Spotify Music Analytics ETL
Handles connections to MongoDB Atlas (raw data) and PostgreSQL/Supabase (analytics)
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoDBConnection:
    """MongoDB Atlas connection handler for raw music data storage"""
    
    def __init__(self):
        self.url = os.getenv('MONGODB_ATLAS_URL')
        self.db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'music_data')
        self.client = None
        self.db = None
        
        if not self.url:
            raise ValueError("MONGODB_ATLAS_URL environment variable is required")
    
    def connect(self):
        """Establish connection to MongoDB Atlas"""
        try:
            self.client = MongoClient(
                self.url, 
                server_api=ServerApi('1'),
                serverSelectionTimeoutMS=5000
            )
            self.db = self.client[self.db_name]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB Atlas - Database: {self.db_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {str(e)}")
            return False
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def insert_listening_history(self, tracks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert listening history data into MongoDB"""
        if not self.db:
            raise ConnectionError("MongoDB not connected")
        
        collection = self.db['daily_listening_history']
        
        # Add metadata to each track
        for track in tracks:
            track['stored_at'] = datetime.utcnow()
            track['data_source'] = 'spotify_api'
        
        try:
            result = collection.insert_many(tracks)
            logger.info(f"✅ Inserted {len(result.inserted_ids)} tracks to listening_history")
            
            return {
                'success': True,
                'inserted_count': len(result.inserted_ids),
                'collection': 'daily_listening_history'
            }
        except Exception as e:
            logger.error(f"❌ Failed to insert tracks: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def insert_audio_features(self, features: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert audio features data into MongoDB"""
        if not self.db:
            raise ConnectionError("MongoDB not connected")
        
        collection = self.db['audio_features']
        
        # Add metadata
        for feature in features:
            feature['stored_at'] = datetime.utcnow()
            feature['data_source'] = 'spotify_api'
        
        try:
            result = collection.insert_many(features)
            logger.info(f"✅ Inserted {len(result.inserted_ids)} audio features")
            
            return {
                'success': True,
                'inserted_count': len(result.inserted_ids),
                'collection': 'audio_features'
            }
        except Exception as e:
            logger.error(f"❌ Failed to insert audio features: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def insert_artist_profiles(self, artists: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert artist profile data into MongoDB"""
        if not self.db:
            raise ConnectionError("MongoDB not connected")
        
        collection = self.db['artist_profiles']
        
        # Add metadata
        for artist in artists:
            artist['stored_at'] = datetime.utcnow()
            artist['data_source'] = 'spotify_api'
        
        try:
            # Use upsert to avoid duplicates based on artist_id
            bulk_operations = []
            for artist in artists:
                bulk_operations.append({
                    'replaceOne': {
                        'filter': {'id': artist['id']},
                        'replacement': artist,
                        'upsert': True
                    }
                })
            
            result = collection.bulk_write(bulk_operations)
            logger.info(f"✅ Upserted {result.upserted_count + result.modified_count} artist profiles")
            
            return {
                'success': True,
                'upserted_count': result.upserted_count,
                'modified_count': result.modified_count,
                'collection': 'artist_profiles'
            }
        except Exception as e:
            logger.error(f"❌ Failed to insert artist profiles: {str(e)}")
            return {'success': False, 'error': str(e)}


class PostgreSQLConnection:
    """PostgreSQL/Supabase connection handler for analytics data"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_DB_URL')
        self.connection = None
        
        if not self.url:
            raise ValueError("SUPABASE_DB_URL environment variable is required")
    
    def connect(self):
        """Establish connection to PostgreSQL/Supabase"""
        try:
            self.connection = psycopg2.connect(
                self.url,
                cursor_factory=RealDictCursor,
                connect_timeout=10
            )
            self.connection.autocommit = False
            logger.info("✅ Connected to PostgreSQL/Supabase")
            return True
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {str(e)}")
            return False
    
    def close(self):
        """Close PostgreSQL connection"""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results"""
        if not self.connection:
            raise ConnectionError("PostgreSQL not connected")
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"❌ Query execution failed: {str(e)}")
            self.connection.rollback()
            raise
    
    def execute_insert(self, table: str, data: List[Dict[str, Any]], schema: str = 'raw_staging') -> Dict[str, Any]:
        """Insert data into PostgreSQL table"""
        if not self.connection:
            raise ConnectionError("PostgreSQL not connected")
        
        if not data:
            return {'success': True, 'inserted_count': 0}
        
        try:
            with self.connection.cursor() as cursor:
                # Generate INSERT statement
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"""
                    INSERT INTO {schema}.{table} ({', '.join(columns)})
                    VALUES ({placeholders})
                """
                
                # Prepare data for insertion
                values = [tuple(row[col] for col in columns) for row in data]
                
                cursor.executemany(query, values)
                self.connection.commit()
                
                inserted_count = cursor.rowcount
                logger.info(f"✅ Inserted {inserted_count} rows into {schema}.{table}")
                
                return {
                    'success': True,
                    'inserted_count': inserted_count,
                    'table': f"{schema}.{table}"
                }
                
        except Exception as e:
            logger.error(f"❌ Insert failed for {schema}.{table}: {str(e)}")
            self.connection.rollback()
            return {'success': False, 'error': str(e)}
    
    def upsert_daily_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Upsert daily listening statistics"""
        if not self.connection:
            raise ConnectionError("PostgreSQL not connected")
        
        try:
            with self.connection.cursor() as cursor:
                query = """
                    INSERT INTO analytics.daily_listening_stats 
                    (date, total_tracks, unique_artists, total_duration_ms, top_genre, avg_energy, avg_valence)
                    VALUES (%(date)s, %(total_tracks)s, %(unique_artists)s, %(total_duration_ms)s, 
                           %(top_genre)s, %(avg_energy)s, %(avg_valence)s)
                    ON CONFLICT (date) 
                    DO UPDATE SET
                        total_tracks = EXCLUDED.total_tracks,
                        unique_artists = EXCLUDED.unique_artists,
                        total_duration_ms = EXCLUDED.total_duration_ms,
                        top_genre = EXCLUDED.top_genre,
                        avg_energy = EXCLUDED.avg_energy,
                        avg_valence = EXCLUDED.avg_valence,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(query, stats_data)
                self.connection.commit()
                
                logger.info(f"✅ Upserted daily stats for {stats_data['date']}")
                
                return {
                    'success': True,
                    'date': stats_data['date'],
                    'table': 'analytics.daily_listening_stats'
                }
                
        except Exception as e:
            logger.error(f"❌ Daily stats upsert failed: {str(e)}")
            self.connection.rollback()
            return {'success': False, 'error': str(e)}
    
    def get_recent_listening_stats(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recent listening statistics"""
        query = """
            SELECT date, total_tracks, unique_artists, total_duration_ms, 
                   top_genre, avg_energy, avg_valence
            FROM analytics.daily_listening_stats
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date DESC
        """
        
        return self.execute_query(query, (days,))


# Utility functions for DAGs
def test_mongodb_connection() -> bool:
    """Test MongoDB connection"""
    try:
        mongo = MongoDBConnection()
        result = mongo.connect()
        mongo.close()
        return result
    except Exception as e:
        logger.error(f"MongoDB connection test failed: {str(e)}")
        return False


def test_postgresql_connection() -> bool:
    """Test PostgreSQL connection"""
    try:
        postgres = PostgreSQLConnection()
        result = postgres.connect()
        postgres.close()
        return result
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {str(e)}")
        return False


def get_database_status() -> Dict[str, Any]:
    """Get status of both databases"""
    return {
        'mongodb_status': test_mongodb_connection(),
        'postgresql_status': test_postgresql_connection(),
        'timestamp': datetime.utcnow().isoformat()
    }


# Context manager for database connections
class DatabaseManager:
    """Context manager for handling both database connections"""
    
    def __init__(self):
        self.mongo = None
        self.postgres = None
    
    def __enter__(self):
        # Initialize connections
        self.mongo = MongoDBConnection()
        self.postgres = PostgreSQLConnection()
        
        # Connect to databases
        mongo_connected = self.mongo.connect()
        postgres_connected = self.postgres.connect()
        
        if not mongo_connected:
            logger.warning("MongoDB connection failed, continuing with PostgreSQL only")
        
        if not postgres_connected:
            logger.warning("PostgreSQL connection failed, continuing with MongoDB only")
        
        if not mongo_connected and not postgres_connected:
            raise ConnectionError("Both database connections failed")
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close connections
        if self.mongo:
            self.mongo.close()
        if self.postgres:
            self.postgres.close()