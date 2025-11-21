# dags/spotify/curl_spotify_tracker.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import base64
import os
import time
import json
import subprocess
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# ============================================================================
# Load Environment Variables
# ============================================================================

def force_load_env_vars():
    """Force load environment variables"""
    env_files = ['/Users/desmond/airflow/.env', '.env', 'airflow_home/.env', '../.env']
    
    for env_file in env_files:
        if os.path.exists(env_file):
            print(f"Loading environment variables: {env_file}")
            
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('"').strip("'")

            print("Environment variables loaded successfully")
            return True

    raise FileNotFoundError("Cannot find .env file to load environment variables")

def get_spotify_credentials():
    """Get Spotify API credentials from environment variables"""
    force_load_env_vars()
    
    client_id = os.environ.get('SPOTIFY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')
    refresh_token = os.environ.get('SPOTIFY_REFRESH_TOKEN')
    
    if not all([client_id, client_secret, refresh_token]):
        missing = []
        if not client_id: missing.append('SPOTIFY_CLIENT_ID')
        if not client_secret: missing.append('SPOTIFY_CLIENT_SECRET')
        if not refresh_token: missing.append('SPOTIFY_REFRESH_TOKEN')
        raise ValueError(f"Missing credentials: {missing}")
    
    return client_id, client_secret, refresh_token

# ============================================================================
# MongoDB Connection
# ============================================================================

def get_mongodb_connection():
    """Get MongoDB connection"""
    try:
        # Ensure environment variables are loaded
        force_load_env_vars()
        
        mongodb_url = os.environ.get('MONGODB_ATLAS_URL')
        db_name = os.environ.get('MONGODB_ATLAS_DB_NAME', 'music_data')

        print(f"MongoDB URL: {mongodb_url[:50]}..." if mongodb_url else "MongoDB URL not set")

        if not mongodb_url:
            raise ValueError("Missing MONGODB_ATLAS_URL")
        
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[db_name]
        return db
        
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        raise

def exists_in_mongodb(collection_name: str, spotify_id: str) -> bool:
    """Check if ID exists in MongoDB"""
    try:
        db = get_mongodb_connection()
        collection = db[collection_name]
        
        # Based on collection, determine the ID field
        if collection_name == 'track_details':
            id_field = 'track_id'
        elif collection_name == 'artist_profiles':
            id_field = 'artist_id'
        elif collection_name == 'album_catalog':
            id_field = 'album_id'
        else:
            id_field = 'id'
        
        result = collection.find_one({id_field: spotify_id})
        return result is not None
    except Exception as e:
        print(f"MongoDB check error: {e}")
        return False

def store_to_mongodb(collection_name: str, data: list) -> dict:
    """Batch store data to MongoDB - use upsert to avoid duplicates"""
    if not data:
        return {"status": "no_data", "count": 0}
    
    try:
        db = get_mongodb_connection()
        collection = db[collection_name]
        
        inserted_count = 0
        updated_count = 0

        # Special handling for daily_listening_history
        if collection_name == 'daily_listening_history':
            for item in data:
                # Use upsert to avoid duplicates
                result = collection.replace_one(
                    {
                        "track_id": item["track_id"], 
                        "played_at": item["played_at"]
                    },
                    item,
                    upsert=True
                )
                
                if result.upserted_id:
                    inserted_count += 1
                elif result.modified_count > 0:
                    updated_count += 1
        else:
            # Other collections use the original logic
            result = collection.insert_many(data, ordered=False)
            inserted_count = len(result.inserted_ids)

        print(f"{collection_name}: Added {inserted_count} documents, Updated {updated_count} documents")

        return {
            "status": "success",
            "collection": collection_name,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "total_attempted": len(data)
        }
        
    except Exception as e:
        print(f"MongoDB error: {e}")
        return {
            "status": "failed",
            "collection": collection_name,
            "error": str(e),
            "total_attempted": len(data)
        }

# ============================================================================
# Based on curl Spotify Client
# ============================================================================

class CurlSpotifyClient:
    """Curl-based Spotify API client"""

    def __init__(self):
        self.client_id, self.client_secret, self.refresh_token = get_spotify_credentials()
        self.access_token = None

        print(f"Curl Spotify Client initialized:")
        print(f"CLIENT_ID: {self.client_id[:10]}***")
        print(f"CLIENT_SECRET: {self.client_secret[:10]}***")
        print(f"REFRESH_TOKEN: {self.refresh_token[:20]}***")
    
    def get_access_token(self):
        """Get Access Token using curl"""
        print("Get Access Token using curl...")
        
        # Prepare Basic Auth header
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_base64 = base64.b64encode(auth_str.encode()).decode()

        # Build curl command
        cmd = [
            'curl', '-s', '-X', 'POST',
            'https://accounts.spotify.com/api/token',
            '-H', f'Authorization: Basic {auth_base64}',
            '-H', 'Content-Type: application/x-www-form-urlencoded',
            '-d', f'grant_type=refresh_token&refresh_token={self.refresh_token}',
            '--max-time', '50'
        ]

        print("Conduct curl command...")

        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=35
            )
            
            if result.returncode == 0:
                response_data = json.loads(result.stdout)
                
                if 'access_token' in response_data:
                    self.access_token = response_data['access_token']
                    expires_in = response_data.get('expires_in', 3600)

                    print(f"Access Token obtained successfully!")
                    print(f"Token expiration time: {expires_in} seconds")
                    return True
                else:
                    error_msg = response_data.get('error', 'Unknown error')
                    print(f"Token acquisition failed: {error_msg}")
                    return False
            else:
                print(f"curl execution failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("curl token request timed out")
            return False
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            return False
        except Exception as e:
            print(f"Token acquisition failed: {e}")
            return False
    
    def make_api_call(self, endpoint: str, params: dict = None):
        """Call Spotify API using curl"""
        if not self.access_token:
            if not self.get_access_token():
                raise Exception("Access Token")
        
        url = f"https://api.spotify.com/v1/{endpoint}"
        
        # Build curl command
        cmd = [
            'curl', '-s', '-X', 'GET',
            url,
            '-H', f'Authorization: Bearer {self.access_token}',
            '--max-time', '30'
        ]
        
        # Add query parameters if any
        if params:
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            cmd[4] = f"{url}?{param_string}"
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=25
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            else:
                print(f"API call failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print("API call timed out")
            return None
        except json.JSONDecodeError as e:
            print(f"Failed to parse API response: {e}")
            return None
        except Exception as e:
            print(f"API call failed: {e}")
            return None
    
    def get_recently_played(self, limit=50):
        """Get recently played tracks"""
        print(f"Getting recently played tracks (limit: {limit})...")
        response = self.make_api_call('me/player/recently-played', {'limit': limit})
        
        if response and 'items' in response:
            print(f"Successfully retrieved {len(response['items'])} tracks")
            return response['items']
        else:
            print("Failed to retrieve playback history")
            return []
    
    def get_several_tracks(self, track_ids: list):
        """Batch get track details (up to 50)"""
        if len(track_ids) > 50:
            print(f"Exceeding track limit, only processing first 50")
            track_ids = track_ids[:50]
        
        ids_string = ','.join(track_ids)
        print(f"Batch getting details for {len(track_ids)} tracks...")
        
        response = self.make_api_call('tracks', {'ids': ids_string})
        
        if response and 'tracks' in response:
            tracks = response['tracks']
            print(f"Successfully retrieved {len(tracks)} track details")
            return tracks
        else:
            print("Failed to retrieve track details")
            return []
    
    def get_several_artists(self, artist_ids: list):
        """Batch get artist details (up to 50)"""
        if len(artist_ids) > 50:
            print(f"Exceeding artist limit, only processing first 50")
            artist_ids = artist_ids[:50]
        
        ids_string = ','.join(artist_ids)
        print(f"Batch getting details for {len(artist_ids)} artists...")
        
        response = self.make_api_call('artists', {'ids': ids_string})
        
        if response and 'artists' in response:
            artists = response['artists']
            print(f"Successfully retrieved {len(artists)} artist details")
            return artists
        else:
            print("Failed to retrieve artist details")
            return []
    
    def get_several_albums(self, album_ids: list):
        """Batch get album details (up to 20)"""
        if len(album_ids) > 20:
            print(f"Exceeding album limit, only processing first 20")
            album_ids = album_ids[:20]
        
        ids_string = ','.join(album_ids)
        print(f"Batch getting details for {len(album_ids)} albums...")
        
        response = self.make_api_call('albums', {'ids': ids_string})
        
        if response and 'albums' in response:
            albums = response['albums']
            print(f"Successfully retrieved {len(albums)} album details")
            return albums
        else:
            print("Failed to retrieve album details")
            return []

# ============================================================================
# DAG Definition
# ============================================================================

default_args = {
    'owner': 'enhanced-spotify',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=15)
}

dag = DAG(
    'enhanced_spotify_tracker',
    default_args=default_args,
    description='Spotify Music Tracker using curl with enhanced data collection',
    schedule='0 */2 * * *',  # Every 2 hours
    max_active_runs=1,
    catchup=False,
    tags=['spotify', 'enhanced', 'batch-api']
)

# ============================================================================
# Task Functions
# ============================================================================

def check_curl_availability(**context):
    """Check if curl is available"""
    print("Checking curl availability...")
    
    try:
        result = subprocess.run(['curl', '--version'], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            version_info = result.stdout.split('\n')[0]
            print(f"curl: {version_info}")

            # Test basic network connectivity
            test_result = subprocess.run(
                ['curl', '-I', 'https://accounts.spotify.com', '--max-time', '10'],
                capture_output=True, text=True, timeout=15
            )
            
            if test_result.returncode == 0:
                print("curl network connectivity test successful")
                return {"status": "success", "curl_version": version_info}
            else:
                print(f"curl network test failed: {test_result.stderr}")
                raise Exception(f"curl network test failed: {test_result.stderr}")
        else:
            print(f"curl not available: {result.stderr}")
            raise Exception(f"curl not available: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print("curl check timeout")
        raise Exception("curl check timeout")
    except Exception as e:
        print(f"curl check failed: {e}")
        raise

def fetch_spotify_data(**context):
    """Fetch Spotify data - including listening history and detailed information collection"""
    print("Starting to fetch Spotify data...")
    batch_id = f"spotify_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = time.time()
    
    try:
        # Initialize client
        client = CurlSpotifyClient()

        # 1. Fetch listening history
        print("\n" + "="*50)
        print("Stage 1: Fetching listening history")
        print("="*50)
        
        listening_data = client.get_recently_played(limit=50)
        
        if not listening_data:
            print("No listening history found")
            return {
                "status": "no_data",
                "batch_id": batch_id,
                "message": "No listening data found"
            }
        
        # 2. Analyze IDs for detailed information collection
        print("\n" + "="*50)
        print("Stage 2: Analyzing IDs for detailed information collection")
        print("="*50)
        
        new_track_ids = []
        new_artist_ids = []
        new_album_ids = []
        
        for item in listening_data:
            track = item['track']
            track_id = track['id']
            album_id = track['album']['id']

            # Check if track needs to be updated
            if not exists_in_mongodb('track_details', track_id):
                new_track_ids.append(track_id)
                print(f"New track: {track['name']}")

            # Check if album needs to be updated
            if not exists_in_mongodb('album_catalog', album_id):
                new_album_ids.append(album_id)
                print(f"New album: {track['album']['name']}")

            # Check all artists
            for artist in track['artists']:
                artist_id = artist['id']
                if not exists_in_mongodb('artist_profiles', artist_id):
                    new_artist_ids.append(artist_id)
                    print(f"New artist: {artist['name']}")

        # Deduplicate IDs
        new_track_ids = list(set(new_track_ids))
        new_artist_ids = list(set(new_artist_ids))
        new_album_ids = list(set(new_album_ids))

        print(f"\nSummary:")
        print(f"Tracks to collect: {len(new_track_ids)}")
        print(f"Artists to collect: {len(new_artist_ids)}")
        print(f"Albums to collect: {len(new_album_ids)}")

        # 3. Batch call API to collect detailed information
        print("\n" + "="*50)
        print("Stage 3: Batch collecting detailed information")
        print("="*50)
        
        collection_results = {
            'track_details': [],
            'artist_profiles': [],
            'album_catalog': []
        }

        # Batch fetch track details
        if new_track_ids:
            print(f"\nProcessing {len(new_track_ids)} tracks...")
            tracks_data = client.get_several_tracks(new_track_ids)
            
            for track in tracks_data:
                if track:  # Make sure track is not none
                    track_detail = {
                        'track_id': track['id'],
                        'name': track['name'],
                        'duration_ms': track['duration_ms'],
                        'explicit': track['explicit'],
                        'popularity': track['popularity'],
                        'preview_url': track.get('preview_url'),
                        'track_number': track.get('track_number'),
                        'artists': [{'id': a['id'], 'name': a['name']} for a in track['artists']],
                        'album': {
                            'id': track['album']['id'],
                            'name': track['album']['name']
                        },
                        'available_markets': track.get('available_markets', []),
                        'external_ids': track.get('external_ids', {}),
                        'external_urls': track.get('external_urls', {}),
                        'metadata': {
                            'first_seen': datetime.utcnow(),
                            'last_updated': datetime.utcnow(),
                            'play_count': 1
                        },
                        'raw_api_response': track
                    }
                    collection_results['track_details'].append(track_detail)

        # Batch fetch artist profiles
        if new_artist_ids:
            print(f"\nProcessing {len(new_artist_ids)} artists...")
            artists_data = client.get_several_artists(new_artist_ids)
            
            for artist in artists_data:
                if artist:  # Make sure artist is not none
                    artist_profile = {
                        'artist_id': artist['id'],
                        'name': artist['name'],
                        'genres': artist.get('genres', []),
                        'popularity': artist.get('popularity', 0),
                        'followers': artist.get('followers', {}).get('total', 0),
                        'images': artist.get('images', []),
                        'external_urls': artist.get('external_urls', {}),
                        'metadata': {
                            'first_seen': datetime.utcnow(),
                            'last_updated': datetime.utcnow(),
                            'total_tracks_played': 1
                        },
                        'raw_api_response': artist
                    }
                    collection_results['artist_profiles'].append(artist_profile)

        # Batch fetch album information
        if new_album_ids:
            print(f"\nProcessing {len(new_album_ids)} albums...")
            albums_data = client.get_several_albums(new_album_ids)
            
            for album in albums_data:
                if album:  # Make sure album is not none
                    album_info = {
                        'album_id': album['id'],
                        'name': album['name'],
                        'album_type': album.get('album_type'),
                        'release_date': album.get('release_date'),
                        'release_date_precision': album.get('release_date_precision'),
                        'total_tracks': album.get('total_tracks', 0),
                        'artists': [{'id': a['id'], 'name': a['name']} for a in album['artists']],
                        'images': album.get('images', []),
                        'genres': album.get('genres', []),
                        'label': album.get('label'),
                        'popularity': album.get('popularity', 0),
                        'external_urls': album.get('external_urls', {}),
                        'metadata': {
                            'first_seen': datetime.utcnow(),
                            'last_updated': datetime.utcnow(),
                            'total_plays': 1
                        },
                        'raw_api_response': album
                    }
                    collection_results['album_catalog'].append(album_info)

        # 4. Process listening records
        print("\n" + "="*50)
        print("Stage 4: Processing listening records")
        print("="*50)
        
        processed_listening = []
        for item in listening_data:
            track = item['track']
            played_at = datetime.fromisoformat(item['played_at'].replace('Z', '+00:00'))
            
            listening_record = {
                'track_id': track['id'],
                'played_at': played_at,
                'track_info': {
                    'name': track['name'],
                    'artists': [{'id': a['id'], 'name': a['name']} for a in track['artists']],
                    'album': {
                        'id': track['album']['id'],
                        'name': track['album']['name']
                    },
                    'duration_ms': track['duration_ms'],
                    'explicit': track['explicit'],
                    'popularity': track['popularity']
                },
                'batch_info': {
                    'batch_id': batch_id,
                    'collected_at': datetime.utcnow(),
                    'api_version': 'v1'
                },
                'raw_api_response': item
            }
            processed_listening.append(listening_record)
        
        collection_time = time.time() - start_time
        
        # Prepare final result
        result = {
            "status": "success",
            "method": "curl_enhanced",
            "batch_id": batch_id,
            "collection_time": round(collection_time, 2),
            "listening_data": processed_listening,
            "detailed_collections": collection_results,
            "summary": {
                "total_listening_records": len(processed_listening),
                "new_tracks_collected": len(collection_results['track_details']),
                "new_artists_collected": len(collection_results['artist_profiles']),
                "new_albums_collected": len(collection_results['album_catalog'])
            }
        }

        print(f"\nFinished! Took {collection_time:.2f} seconds")
        print(f"Listening Records: {len(processed_listening)}")
        print(f"New Tracks: {len(collection_results['track_details'])}")
        print(f"New Artists: {len(collection_results['artist_profiles'])}")
        print(f"New Albums: {len(collection_results['album_catalog'])}")

        # Push result to XCom
        context['task_instance'].xcom_push(key='spotify_data', value=result)
        return result
        
    except Exception as e:
        error_result = {
            "status": "failed",
            "method": "curl_enhanced",
            "batch_id": batch_id,
            "error": str(e),
            "collection_time": time.time() - start_time
        }

        print(f"Failed: {e}")
        context['task_instance'].xcom_push(key='spotify_data', value=error_result)
        raise

def store_enhanced_data(**context):
    """Store enhanced data to MongoDB"""
    print("Storing enhanced data to MongoDB...")
    
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_enhanced_spotify_data',
        key='spotify_data'
    )
    
    if not spotify_data or spotify_data.get('status') != 'success':
        print("No valid data to store")
        return {"status": "no_data"}
    
    storage_results = {}
    
    try:
        # 1. Store listening history
        listening_data = spotify_data.get('listening_data', [])
        if listening_data:
            print(f"Storing {len(listening_data)} listening records...")
            result = store_to_mongodb('daily_listening_history', listening_data)
            storage_results['listening_history'] = result

        # 2. Store track details
        track_details = spotify_data.get('detailed_collections', {}).get('track_details', [])
        if track_details:
            print(f"Storing {len(track_details)} track details...")
            result = store_to_mongodb('track_details', track_details)
            storage_results['track_details'] = result

        # 3. Store artist profiles
        artist_profiles = spotify_data.get('detailed_collections', {}).get('artist_profiles', [])
        if artist_profiles:
            print(f"Storing {len(artist_profiles)} artist profiles...")
            result = store_to_mongodb('artist_profiles', artist_profiles)
            storage_results['artist_profiles'] = result

        # 4. Store album catalog
        album_catalog = spotify_data.get('detailed_collections', {}).get('album_catalog', [])
        if album_catalog:
            print(f"Storing {len(album_catalog)} album catalog...")
            result = store_to_mongodb('album_catalog', album_catalog)
            storage_results['album_catalog'] = result

        # 5. Store batch execution log
        batch_log = {
            'batch_id': spotify_data['batch_id'],
            'execution_date': datetime.utcnow(),
            'status': 'success',
            'summary': spotify_data['summary'],
            'storage_results': storage_results,
            'execution_time': spotify_data.get('collection_time', 0),
            'method': 'curl_enhanced'
        }

        print(f"Storing batch execution log...")
        batch_result = store_to_mongodb('batch_execution_log', [batch_log])
        storage_results['batch_log'] = batch_result

        # Calculate total storage statistics
        total_inserted = 0
        total_attempted = 0
        
        for collection, result in storage_results.items():
            if result.get('status') == 'success':
                total_inserted += result.get('inserted_count', 0)
                total_attempted += result.get('total_attempted', 0)
        
        final_result = {
            'status': 'success',
            'batch_id': spotify_data['batch_id'],
            'collections_updated': len(storage_results),
            'total_inserted': total_inserted,
            'total_attempted': total_attempted,
            'storage_details': storage_results
        }

        print(f"\nData storage complete!")
        print(f"Total stored: {total_inserted}/{total_attempted}")
        print(f"Collections updated: {len(storage_results)}")

        context['task_instance'].xcom_push(key='storage_results', value=final_result)
        return final_result
        
    except Exception as e:
        error_result = {
            'status': 'failed',
            'batch_id': spotify_data.get('batch_id', 'unknown'),
            'error': str(e),
            'partial_results': storage_results
        }

        print(f"Data storage failed: {e}")
        context['task_instance'].xcom_push(key='storage_results', value=error_result)
        return error_result

def log_enhanced_summary(**context):
    """Log enhanced summary report"""
    execution_date = context['ds']
    
    spotify_data = context['task_instance'].xcom_pull(
        task_ids='fetch_enhanced_spotify_data',
        key='spotify_data'
    ) or {}
    
    storage_results = context['task_instance'].xcom_pull(
        task_ids='store_enhanced_data',
        key='storage_results'
    ) or {}
    
    print("\n" + "=" * 80)
    print("Enhanced Spotify Music Tracking Execution Report")
    print("=" * 80)
    print(f"Execution Date: {execution_date}")
    print(f"Batch ID: {spotify_data.get('batch_id', 'unknown')}")
    print(f"Method: {spotify_data.get('method', 'unknown')}")
    print(f"⏱Collection Time: {spotify_data.get('collection_time', 0)} seconds")
    print("")
    
    # Spotify API 
    summary = spotify_data.get('summary', {})
    print("Spotify API Collection:")
    print(f"Listening Records: {summary.get('total_listening_records', 0)}")
    print(f"New Tracks: {summary.get('new_tracks_collected', 0)}")
    print(f"New Artists: {summary.get('new_artists_collected', 0)}")
    print(f"New Albums: {summary.get('new_albums_collected', 0)}")
    print("")
    
    # MongoDB Storage
    print("MongoDB Storage:")
    storage_details = storage_results.get('storage_details', {})
    
    for collection, result in storage_details.items():
        status_icon = "Success" if result.get('status') == 'success' else "Failed"
        inserted = result.get('inserted_count', 0)
        attempted = result.get('total_attempted', 0)
        print(f"   {status_icon} {collection}: {inserted}/{attempted}")

    print(f"Total: {storage_results.get('total_inserted', 0)}/{storage_results.get('total_attempted', 0)}")
    print("")
    
    # 成功率計算
    if spotify_data.get('status') == 'success' and storage_results.get('status') == 'success':
        success_rate = 100
        status_emoji = ""
        status_msg = "Full Success"
    elif spotify_data.get('status') == 'success':
        success_rate = 75
        status_emoji = ""
        status_msg = "Data collection successful, storage failed partially"
    else:
        success_rate = 0
        status_emoji = ""
        status_msg = "Execution Failed"

    print("Execution Status:")
    print(f"{status_emoji} Status: {status_msg}")
    print(f" Success Rate: {success_rate}%")
    print("")
    
    # Data Quality Insights
    if summary.get('total_listening_records', 0) > 0:
        print("Data Quality:")
        new_items = (summary.get('new_tracks_collected', 0) +
                    summary.get('new_artists_collected', 0) +
                    summary.get('new_albums_collected', 0))
        
        if new_items > 0:
            print(f"New Discoveries: {new_items} items")
            print("The system is continuously enriching the music database")
        else:
            print("All content already exists, the database remains up-to-date")

        print(f"Data coverage continues to improve")

    return "Enhanced Music Tracking Execution Completed Successfully"

# ============================================================================
# Task Definitions
# ============================================================================

check_curl_task = PythonOperator(
    task_id='check_curl_availability',
    python_callable=check_curl_availability,
    dag=dag
)

fetch_enhanced_data_task = PythonOperator(
    task_id='fetch_enhanced_spotify_data',
    python_callable=fetch_spotify_data,
    dag=dag
)

store_enhanced_data_task = PythonOperator(
    task_id='store_enhanced_data',
    python_callable=store_enhanced_data,
    dag=dag
)

summary_task = PythonOperator(
    task_id='log_enhanced_summary',
    python_callable=log_enhanced_summary,
    dag=dag
)

# ============================================================================
# Task Dependencies
# ============================================================================

check_curl_task >> fetch_enhanced_data_task >> store_enhanced_data_task >> summary_task