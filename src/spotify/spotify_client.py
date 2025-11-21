# src/spotify/spotify_client.py
# Spotify API Client

import requests
import base64
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import logging
from dotenv import load_dotenv

class SpotifyAuthError(Exception):
    """Spotify authentication error"""
    pass

class SpotifyAPIError(Exception):
    """Spotify API request error"""
    pass

class SpotifyClient:
    """
    Spotify API Client
    
    Features:
    - OAuth2 Access Token Management
    - Rate Limiting Handling
    - Robust API Request with Retries
    - Key Endpoint Methods (Recently Played, Track Details, Audio Features, Artist Info, Album Info, User Profile, Search)
    - API Usage Statistics
    """
    
    def __init__(self, client_id: str = None, client_secret: str = None, refresh_token: str = None):
        """
        Initialize Spotify API Client
        
        Args:
            client_id: Spotify Client ID (optional, read from environment variables)
            client_secret: Spotify Client Secret (optional, read from environment variables)
            refresh_token: Spotify Refresh Token (optional, read from environment variables)
        """
        # Load environment variables
        load_dotenv()
        
        self.client_id = client_id or os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('SPOTIFY_CLIENT_SECRET')
        self.refresh_token = refresh_token or os.getenv('SPOTIFY_REFRESH_TOKEN')

        # Validate required parameters
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            missing = []
            if not self.client_id: missing.append('SPOTIFY_CLIENT_ID')
            if not self.client_secret: missing.append('SPOTIFY_CLIENT_SECRET')
            if not self.refresh_token: missing.append('SPOTIFY_REFRESH_TOKEN')

            raise SpotifyAuthError(f"Missing required Spotify API credentials: {missing}")

        # Token Management
        self.access_token = None
        self.token_expires_at = None
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms interval

        # Set up logging
        self.logger = logging.getLogger(__name__)

        # API statistics
        self.api_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'rate_limit_hits': 0,
            'token_refreshes': 0
        }
    
    def _ensure_rate_limit(self):
        """Ensure API request interval to respect rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _is_token_valid(self) -> bool:
        """Check if access token is valid"""
        if not self.access_token:
            return False
        
        if not self.token_expires_at:
            return False

        # Refresh token 5 minutes early
        return datetime.now() < (self.token_expires_at - timedelta(minutes=5))
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get a valid Access Token
        
        Args:
            force_refresh: Force refresh token

        Returns:
            A valid access token

        Raises:
            SpotifyAuthError: Authentication failed
        """
        if not force_refresh and self._is_token_valid():
            return self.access_token

        self.logger.info("Refreshing Spotify Access Token...")

        # Encode credentials
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_base64 = base64.b64encode(auth_str.encode()).decode()

        # Request new access token
        try:
            response = requests.post(
                "https://accounts.spotify.com/api/token",
                headers={
                    'Authorization': f'Basic {auth_base64}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                data={
                    'grant_type': 'refresh_token',
                    'refresh_token': self.refresh_token
                },
                timeout=30
            )
            
            if response.status_code == 200:
                tokens = response.json()
                self.access_token = tokens['access_token']

                # Calculate token expiration time
                expires_in = tokens.get('expires_in', 3600)  # Default 1 hour
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                # Update statistics
                self.api_stats['token_refreshes'] += 1

                self.logger.info(f"Access Token refreshed successfully (expires in: {expires_in} seconds)")
                return self.access_token
            
            else:
                error_msg = f"Token refresh failed: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise SpotifyAuthError(error_msg)
                
        except requests.RequestException as e:
            error_msg = f"Token refresh request failed: {str(e)}"
            self.logger.error(error_msg)
            raise SpotifyAuthError(error_msg)
    
    def _make_api_request(self, url: str, params: Dict = None, retries: int = 3) -> Dict:
        """
        Execute Spotify API request (internal method)

        Args:
            url: API endpoint URL
            params: Request parameters
            retries: Number of retries

        Returns:
            API response JSON data

        Raises:
            SpotifyAPIError: API request failed
        Returns:
            API response JSON data

        Raises:
            SpotifyAPIError: API request failed
        """
        params = params or {}
        
        for attempt in range(retries + 1):
            try:
                # Ensure a valid access token
                access_token = self.get_access_token()
                
                # Rate limiting
                self._ensure_rate_limit()

                # Execute request
                headers = {"Authorization": f"Bearer {access_token}"}
                
                self.api_stats['total_requests'] += 1
                
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    self.api_stats['successful_requests'] += 1
                    return response.json()
                
                elif response.status_code == 401:
                    # Token invalid, try refreshing
                    self.logger.warning(f"Token invalid (attempt {attempt + 1}/{retries + 1}), refreshing...")
                    self.get_access_token(force_refresh=True)
                    continue
                
                elif response.status_code == 429:
                    # Rate limit hit
                    self.api_stats['rate_limit_hits'] += 1
                    retry_after = int(response.headers.get('Retry-After', 1))
                    self.logger.warning(f"Encountered Rate Limit, waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                else:
                    error_msg = f"API request failed: {response.status_code} - {response.text}"
                    if attempt == retries:
                        self.api_stats['failed_requests'] += 1
                        raise SpotifyAPIError(error_msg)
                    else:
                        self.logger.warning(f"{error_msg} (attempt {attempt + 1}/{retries + 1})")
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                        
            except requests.RequestException as e:
                error_msg = f"Request exception: {str(e)}"
                if attempt == retries:
                    self.api_stats['failed_requests'] += 1
                    raise SpotifyAPIError(error_msg)
                else:
                    self.logger.warning(f"{error_msg} (attempt {attempt + 1}/{retries + 1})")
                    time.sleep(2 ** attempt)

        # Should not reach here
        raise SpotifyAPIError("API request retries exhausted")

    def get_recently_played(self, limit: int = 50, after: int = None, before: int = None) -> List[Dict]:
        """
        Get recently played tracks

        Args:
            limit: Number of records to retrieve (max 50)
            after: Unix timestamp to retrieve playback records after this time
            before: Unix timestamp to retrieve playback records before this time

        Returns:
            List of playback records
            
        Raises:
            SpotifyAPIError: API request failed
        """
        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50")

        self.logger.info(f"Fetching recently played {limit} tracks...")
        
        url = "https://api.spotify.com/v1/me/player/recently-played"
        params = {"limit": limit}
        
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        
        try:
            data = self._make_api_request(url, params)
            items = data.get('items', [])

            self.logger.info(f"Successfully fetched {len(items)} tracks")
            # Log time range
            if items:
                times = [item['played_at'] for item in items]
                self.logger.info(f"Time range: {min(times)} to {max(times)}")

            return items
            
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to retrieve playback records: {e}")
            raise
    
    def get_track_details(self, track_id: str) -> Dict:
        """
        Get details of a single track
        
        Args:
            track_id: Spotify Track ID
            
        Returns:
            Track details
        """
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to retrieve track details (ID: {track_id}): {e}")
            raise
    
    def get_audio_features(self, track_ids: List[str]) -> List[Dict]:
        """
        Get audio features for multiple tracks
        
        Args:
            track_ids: Spotify Track ID list (max 100)

        Returns:
            List of audio features
        """
        if len(track_ids) > 100:
            raise ValueError("Audio features can be requested for up to 100 tracks at once")
        
        url = "https://api.spotify.com/v1/audio-features"
        params = {"ids": ",".join(track_ids)}
        
        try:
            data = self._make_api_request(url, params)
            return data.get('audio_features', [])
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to retrieve audio features: {e}")
            raise
    
    def get_artist_info(self, artist_id: str) -> Dict:
        """
        Get artist information
        
        Args:
            artist_id: Spotify Artist ID
            
        Returns:
            Artist information
        """
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to retrieve artist information (ID: {artist_id}): {e}")
            raise
    
    def get_album_info(self, album_id: str) -> Dict:
        """
        Get album information
        
        Args:
            album_id: Spotify Album ID
            
        Returns:
            Album information
        """
        url = f"https://api.spotify.com/v1/albums/{album_id}"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to retrieve album information (ID: {album_id}): {e}")
            raise
    
    def get_user_profile(self) -> Dict:
        """
        Get current user information
        
        Returns:
            User information
        """
        url = "https://api.spotify.com/v1/me"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to retrieve user information: {e}")
            raise
    
    def search(self, query: str, search_type: str = "track", limit: int = 20) -> Dict:
        """
        Search Spotify content
        
        Args:
            query: Search query
            search_type: Search type (track, artist, album, playlist)
            limit: Result limit
            
        Returns:
            Search results
        """
        url = "https://api.spotify.com/v1/search"
        params = {
            "q": query,
            "type": search_type,
            "limit": limit
        }
        
        try:
            return self._make_api_request(url, params)
        except SpotifyAPIError as e:
            self.logger.error(f"Failed to search (query: {query}): {e}")
            raise
    
    def get_api_stats(self) -> Dict:
        """
        Get API usage statistics
        
        Returns:
            API usage statistics
        """
        stats = self.api_stats.copy()
        stats['success_rate'] = (
            stats['successful_requests'] / stats['total_requests'] 
            if stats['total_requests'] > 0 else 0
        )
        stats['token_valid'] = self._is_token_valid()
        stats['token_expires_at'] = self.token_expires_at.isoformat() if self.token_expires_at else None
        
        return stats
    
    def __str__(self) -> str:
        """String representation"""
        return f"SpotifyClient(stats={self.get_api_stats()})"


# ============================================================================
# Tool (Data Processing Helpers)
# ============================================================================

def format_listening_record(api_item: Dict, batch_id: str) -> Dict:
    """
    Format Spotify API response for MongoDB storage
    
    Args:
        api_item: Single API response item
        batch_id: Batch ID

    Returns:
        Formatted listening record
    """
    track = api_item['track']
    played_at = api_item['played_at']

    # Convert time format
    played_time = datetime.fromisoformat(played_at.replace('Z', '+00:00'))
    
    return {
        'track_id': track['id'],
        'played_at': played_time,
        'track_info': {
            'name': track['name'],
            'artists': [
                {
                    'id': artist['id'],
                    'name': artist['name']
                } for artist in track['artists']
            ],
            'album': {
                'id': track['album']['id'],
                'name': track['album']['name'],
                'release_date': track['album'].get('release_date', ''),
                'images': track['album'].get('images', [])
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
        'raw_api_response': api_item
    }


def validate_track_record(record: Dict) -> tuple[bool, List[str]]:
    """
    Validate the data quality of a single listening record

    Args:
        record: Listening record

    Returns:
        Tuple of (is_valid, issues)
    """
    issues = []
    
    # Check required fields
    required_fields = ['track_id', 'played_at', 'track_info', 'batch_info']
    for field in required_fields:
        if not record.get(field):
            issues.append(f"Lack of field: {field}")

    # track_info check
    if record.get('track_info'):
        track_info = record['track_info']
        required_track_fields = ['name', 'artists', 'album']
        for field in required_track_fields:
            if not track_info.get(field):
                issues.append(f"Lack of track_info.{field}")

        # Check if artists is a non-empty list
        if track_info.get('artists') and not isinstance(track_info['artists'], list):
            issues.append("track_info.artists should be a list")
        elif track_info.get('artists') and len(track_info['artists']) == 0:
            issues.append("track_info.artists should not be an empty list")

    # Data type checks
    if record.get('track_info', {}).get('duration_ms'):
        if not isinstance(record['track_info']['duration_ms'], int):
            issues.append("duration_ms should be an integer")

    # played_at should be a datetime object
    if record.get('played_at') and not isinstance(record['played_at'], datetime):
        issues.append("played_at should be a datetime object")

    return len(issues) == 0, issues


def calculate_quality_score(record: Dict, issues: List[str]) -> float:
    """
    Calculate data quality score

    Args:
        record: Listening record
        issues: Quality issues list

    Returns:
        Quality score (0.0 - 1.0)
    """
    total_checks = 10  # Total number of checks
    failed_checks = len(issues)

    # Bonus points
    bonus_points = 0

    # If there is complete album information
    if record.get('track_info', {}).get('album', {}).get('release_date'):
        bonus_points += 1

    # If there are album images
    if record.get('track_info', {}).get('album', {}).get('images'):
        bonus_points += 1
    
    # If popularity is available
    if record.get('track_info', {}).get('popularity', 0) > 0:
        bonus_points += 1
    
    passed_checks = total_checks - failed_checks + bonus_points
    score = min(passed_checks / total_checks, 1.0)
    
    return round(score, 3)


class SpotifyDataProcessor:
    """
    Spotify Data Processor
    Integrates data fetching, validation, and formatting into a complete workflow
    """
    
    def __init__(self, client: SpotifyClient):
        self.client = client
        self.logger = logging.getLogger(__name__)
    
    def fetch_and_process_recent_tracks(self, limit: int = 50, batch_id: str = None) -> Dict:
        """
        Fetch and process recently played tracks
        
        Args:
            limit: Number of records to fetch
            batch_id: Batch ID (optional, auto-generated)

        Returns:
            Processing result
        """
        if not batch_id:
            batch_id = f"spotify_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"Start (Batch: {batch_id})")
        
        try:
            # 1. Fetch raw data
            raw_items = self.client.get_recently_played(limit=limit)
            
            if not raw_items:
                return {
                    'status': 'no_data',
                    'message': 'No recent tracks found',
                    'batch_id': batch_id
                }
            
            # 2. Format data
            formatted_records = []
            for item in raw_items:
                formatted_record = format_listening_record(item, batch_id)
                formatted_records.append(formatted_record)

            # 3. Validate data quality
            validation_results = self._validate_records(formatted_records)

            # 4. Compile results
            result = {
                'status': 'success',
                'batch_id': batch_id,
                'total_tracks': len(raw_items),
                'valid_tracks': validation_results['valid_tracks'],
                'validation_results': validation_results,
                'processed_data': formatted_records,
                'api_stats': self.client.get_api_stats(),
                'processed_at': datetime.utcnow().isoformat()
            }

            self.logger.info(f"Processing complete: {len(formatted_records)} records, {validation_results['valid_count']} valid")
            return result
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg,
                'batch_id': batch_id
            }
    
    def _validate_records(self, records: List[Dict]) -> Dict:
        """Validate record list"""
        validation_results = {
            'total_count': len(records),
            'valid_count': 0,
            'invalid_count': 0,
            'quality_issues': [],
            'summary_stats': {}
        }
        
        valid_records = []
        quality_scores = []
        
        for i, record in enumerate(records):
            is_valid, issues = validate_track_record(record)
            quality_score = calculate_quality_score(record, issues)
            quality_scores.append(quality_score)
            
            if is_valid:
                validation_results['valid_count'] += 1
                record['data_quality'] = {
                    'score': quality_score,
                    'validated_at': datetime.utcnow()
                }
                valid_records.append(record)
            else:
                validation_results['invalid_count'] += 1
                validation_results['quality_issues'].append({
                    'record_index': i,
                    'track_id': record.get('track_id', 'unknown'),
                    'track_name': record.get('track_info', {}).get('name', 'unknown'),
                    'issues': issues,
                    'quality_score': quality_score
                })
        
        # Compile summary statistics
        validation_results['valid_tracks'] = valid_records
        validation_results['summary_stats'] = {
            'avg_quality_score': sum(quality_scores) / len(quality_scores) if quality_scores else 0,
            'min_quality_score': min(quality_scores) if quality_scores else 0,
            'max_quality_score': max(quality_scores) if quality_scores else 0,
            'unique_artists': len(set(
                artist['name'] 
                for record in valid_records 
                for artist in record.get('track_info', {}).get('artists', [])
            )),
            'unique_albums': len(set(
                record.get('track_info', {}).get('album', {}).get('name', '')
                for record in valid_records
            )),
            'avg_popularity': sum(
                record.get('track_info', {}).get('popularity', 0)
                for record in valid_records
            ) / len(valid_records) if valid_records else 0
        }
        
        return validation_results


# ============================================================================
# Demo Usage
# ============================================================================

def demo_usage():
    """Show how to use SpotifyClient"""
    print("🎵 Spotify Client Demo")
    print("=" * 40)
    
    try:
        # Initialize client
        print("1. Initialize client...")
        client = SpotifyClient()
        print("Client initialized successfully")

        # Fetch recently played tracks
        print("\n2. Fetch recently played tracks...")
        tracks = client.get_recently_played(limit=5)
        print(f"Fetched {len(tracks)} tracks")

        if tracks:
            example = tracks[0]
            track_name = example['track']['name']
            artist_name = example['track']['artists'][0]['name']
            print(f"Example track: {track_name} - {artist_name}")

        # Use data processor
        print("\n3. Use data processor...")
        processor = SpotifyDataProcessor(client)
        result = processor.fetch_and_process_recent_tracks(limit=5)

        print(f"Processing result: {result['status']}")
        if result['status'] == 'success':
            print(f"Valid tracks: {result['validation_results']['valid_count']}")

        # Show API statistics
        print("\n4. API Statistics:")
        stats = client.get_api_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")

        print("\nDemo completed!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Execute demo
    demo_usage()