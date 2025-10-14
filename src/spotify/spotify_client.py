# src/spotify/spotify_client.py
# Spotify API 客戶端 - 可重用的模組化設計

import requests
import base64
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import logging
from dotenv import load_dotenv

class SpotifyAuthError(Exception):
    """Spotify 認證錯誤"""
    pass

class SpotifyAPIError(Exception):
    """Spotify API 請求錯誤"""
    pass

class SpotifyClient:
    """
    Spotify API 客戶端
    
    功能:
    - 自動管理 Access Token
    - Rate Limiting 處理
    - 錯誤重試機制
    - 完整的 API 回應記錄
    """
    
    def __init__(self, client_id: str = None, client_secret: str = None, refresh_token: str = None):
        """
        初始化 Spotify 客戶端
        
        Args:
            client_id: Spotify 客戶端 ID (可選，從環境變數讀取)
            client_secret: Spotify 客戶端密鑰 (可選，從環境變數讀取)
            refresh_token: Spotify 刷新令牌 (可選，從環境變數讀取)
        """
        # 載入環境變數
        load_dotenv()
        
        self.client_id = client_id or os.getenv('SPOTIFY_CLIENT_ID', '2d2343762689494080664bd26ccc898f')
        self.client_secret = client_secret or os.getenv('SPOTIFY_CLIENT_SECRET', '987c2fe892154b788df9790a04d84f6c')
        self.refresh_token = refresh_token or os.getenv('SPOTIFY_REFRESH_TOKEN', 'AQBk4fvESczjR30qGozNEDAe82YOJr_zJmR4Ga5_LTfTBH1xoFl1wAT4hIA9DieRyl1Vxg-Vlh9Vi2dqGf0h0iAPjIVkyPw6MjuvIAl6-02Qlh-6Bf55zKDGdhj8r7vp_F8')
        
        # 驗證必要參數
        if not all([self.client_id, self.client_secret, self.refresh_token]):
            missing = []
            if not self.client_id: missing.append('SPOTIFY_CLIENT_ID')
            if not self.client_secret: missing.append('SPOTIFY_CLIENT_SECRET')
            if not self.refresh_token: missing.append('SPOTIFY_REFRESH_TOKEN')
            
            raise SpotifyAuthError(f"缺少必要的 Spotify API 憑證: {missing}")
        
        # Token 管理
        self.access_token = None
        self.token_expires_at = None
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms 間隔
        
        # 設定 logging
        self.logger = logging.getLogger(__name__)
        
        # API 統計
        self.api_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'rate_limit_hits': 0,
            'token_refreshes': 0
        }
    
    def _ensure_rate_limit(self):
        """確保 API 請求間隔"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _is_token_valid(self) -> bool:
        """檢查 access token 是否有效"""
        if not self.access_token:
            return False
        
        if not self.token_expires_at:
            return False
        
        # 提前 5 分鐘更新 token
        return datetime.now() < (self.token_expires_at - timedelta(minutes=5))
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        獲取有效的 Access Token
        
        Args:
            force_refresh: 強制刷新 token
            
        Returns:
            有效的 access token
            
        Raises:
            SpotifyAuthError: 認證失敗
        """
        if not force_refresh and self._is_token_valid():
            return self.access_token
        
        self.logger.info("正在刷新 Spotify Access Token...")
        
        # 編碼 credentials
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_base64 = base64.b64encode(auth_str.encode()).decode()
        
        # 請求新的 access token
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
                
                # 計算 token 過期時間
                expires_in = tokens.get('expires_in', 3600)  # 預設 1 小時
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
                
                # 更新統計
                self.api_stats['token_refreshes'] += 1
                
                self.logger.info(f"Access Token 刷新成功 (有效期: {expires_in} 秒)")
                return self.access_token
            
            else:
                error_msg = f"Token 刷新失敗: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise SpotifyAuthError(error_msg)
                
        except requests.RequestException as e:
            error_msg = f"Token 刷新請求失敗: {str(e)}"
            self.logger.error(error_msg)
            raise SpotifyAuthError(error_msg)
    
    def _make_api_request(self, url: str, params: Dict = None, retries: int = 3) -> Dict:
        """
        執行 Spotify API 請求 (內部方法)
        
        Args:
            url: API 端點 URL
            params: 請求參數
            retries: 重試次數
            
        Returns:
            API 回應 JSON 資料
            
        Raises:
            SpotifyAPIError: API 請求失敗
        """
        params = params or {}
        
        for attempt in range(retries + 1):
            try:
                # 確保有效的 access token
                access_token = self.get_access_token()
                
                # Rate limiting
                self._ensure_rate_limit()
                
                # 執行請求
                headers = {"Authorization": f"Bearer {access_token}"}
                
                self.api_stats['total_requests'] += 1
                
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    self.api_stats['successful_requests'] += 1
                    return response.json()
                
                elif response.status_code == 401:
                    # Token 無效，嘗試刷新
                    self.logger.warning(f"Token 無效 (嘗試 {attempt + 1}/{retries + 1})，正在刷新...")
                    self.get_access_token(force_refresh=True)
                    continue
                
                elif response.status_code == 429:
                    # Rate limit hit
                    self.api_stats['rate_limit_hits'] += 1
                    retry_after = int(response.headers.get('Retry-After', 1))
                    self.logger.warning(f"遇到 Rate Limit，等待 {retry_after} 秒...")
                    time.sleep(retry_after)
                    continue
                
                else:
                    error_msg = f"API 請求失敗: {response.status_code} - {response.text}"
                    if attempt == retries:
                        self.api_stats['failed_requests'] += 1
                        raise SpotifyAPIError(error_msg)
                    else:
                        self.logger.warning(f"{error_msg} (嘗試 {attempt + 1}/{retries + 1})")
                        time.sleep(2 ** attempt)  # 指數退避
                        continue
                        
            except requests.RequestException as e:
                error_msg = f"請求異常: {str(e)}"
                if attempt == retries:
                    self.api_stats['failed_requests'] += 1
                    raise SpotifyAPIError(error_msg)
                else:
                    self.logger.warning(f"{error_msg} (嘗試 {attempt + 1}/{retries + 1})")
                    time.sleep(2 ** attempt)
        
        # 應該不會到達這裡
        raise SpotifyAPIError("API 請求重試次數用盡")
    
    def get_recently_played(self, limit: int = 50, after: int = None, before: int = None) -> List[Dict]:
        """
        獲取最近播放的歌曲
        
        Args:
            limit: 要獲取的歌曲數量 (1-50)
            after: Unix timestamp，獲取此時間後的播放記錄
            before: Unix timestamp，獲取此時間前的播放記錄
            
        Returns:
            播放記錄列表
            
        Raises:
            SpotifyAPIError: API 請求失敗
        """
        if not 1 <= limit <= 50:
            raise ValueError("limit 必須在 1-50 之間")
        
        self.logger.info(f"正在獲取最近播放的 {limit} 首歌曲...")
        
        url = "https://api.spotify.com/v1/me/player/recently-played"
        params = {"limit": limit}
        
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        
        try:
            data = self._make_api_request(url, params)
            items = data.get('items', [])
            
            self.logger.info(f"成功獲取 {len(items)} 首歌曲")
            
            # 紀錄時間範圍
            if items:
                times = [item['played_at'] for item in items]
                self.logger.info(f"時間範圍: {min(times)} 到 {max(times)}")
            
            return items
            
        except SpotifyAPIError as e:
            self.logger.error(f"獲取播放記錄失敗: {e}")
            raise
    
    def get_track_details(self, track_id: str) -> Dict:
        """
        獲取單首歌曲的詳細資訊
        
        Args:
            track_id: Spotify Track ID
            
        Returns:
            歌曲詳細資訊
        """
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"獲取歌曲詳情失敗 (ID: {track_id}): {e}")
            raise
    
    def get_audio_features(self, track_ids: List[str]) -> List[Dict]:
        """
        獲取多首歌曲的音訊特徵
        
        Args:
            track_ids: Spotify Track ID 列表 (最多 100 個)
            
        Returns:
            音訊特徵列表
        """
        if len(track_ids) > 100:
            raise ValueError("一次最多只能查詢 100 首歌曲的音訊特徵")
        
        url = "https://api.spotify.com/v1/audio-features"
        params = {"ids": ",".join(track_ids)}
        
        try:
            data = self._make_api_request(url, params)
            return data.get('audio_features', [])
        except SpotifyAPIError as e:
            self.logger.error(f"獲取音訊特徵失敗: {e}")
            raise
    
    def get_artist_info(self, artist_id: str) -> Dict:
        """
        獲取藝術家資訊
        
        Args:
            artist_id: Spotify Artist ID
            
        Returns:
            藝術家資訊
        """
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"獲取藝術家資訊失敗 (ID: {artist_id}): {e}")
            raise
    
    def get_album_info(self, album_id: str) -> Dict:
        """
        獲取專輯資訊
        
        Args:
            album_id: Spotify Album ID
            
        Returns:
            專輯資訊
        """
        url = f"https://api.spotify.com/v1/albums/{album_id}"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"獲取專輯資訊失敗 (ID: {album_id}): {e}")
            raise
    
    def get_user_profile(self) -> Dict:
        """
        獲取當前用戶資訊
        
        Returns:
            用戶資訊
        """
        url = "https://api.spotify.com/v1/me"
        
        try:
            return self._make_api_request(url)
        except SpotifyAPIError as e:
            self.logger.error(f"獲取用戶資訊失敗: {e}")
            raise
    
    def search(self, query: str, search_type: str = "track", limit: int = 20) -> Dict:
        """
        搜尋 Spotify 內容
        
        Args:
            query: 搜尋查詢
            search_type: 搜尋類型 (track, artist, album, playlist)
            limit: 結果數量限制
            
        Returns:
            搜尋結果
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
            self.logger.error(f"搜尋失敗 (查詢: {query}): {e}")
            raise
    
    def get_api_stats(self) -> Dict:
        """
        獲取 API 使用統計
        
        Returns:
            API 統計資訊
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
        """字串表示"""
        return f"SpotifyClient(stats={self.get_api_stats()})"


# ============================================================================
# 工具函數 (Data Processing Helpers)
# ============================================================================

def format_listening_record(api_item: Dict, batch_id: str) -> Dict:
    """
    將 Spotify API 回應格式化為 MongoDB 儲存格式
    
    Args:
        api_item: Spotify Recently Played API 的單筆回應
        batch_id: 批次識別碼
        
    Returns:
        格式化的聽歌記錄
    """
    track = api_item['track']
    played_at = api_item['played_at']
    
    # 轉換時間格式
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
    驗證單筆聽歌記錄的資料品質
    
    Args:
        record: 聽歌記錄
        
    Returns:
        (是否有效, 問題列表)
    """
    issues = []
    
    # 必要欄位檢查
    required_fields = ['track_id', 'played_at', 'track_info', 'batch_info']
    for field in required_fields:
        if not record.get(field):
            issues.append(f"缺少必要欄位: {field}")
    
    # track_info 詳細檢查
    if record.get('track_info'):
        track_info = record['track_info']
        required_track_fields = ['name', 'artists', 'album']
        for field in required_track_fields:
            if not track_info.get(field):
                issues.append(f"缺少 track_info.{field}")
        
        # 檢查 artists 是否為非空列表
        if track_info.get('artists') and not isinstance(track_info['artists'], list):
            issues.append("track_info.artists 應為列表")
        elif track_info.get('artists') and len(track_info['artists']) == 0:
            issues.append("track_info.artists 不應為空列表")
    
    # 資料型態檢查
    if record.get('track_info', {}).get('duration_ms'):
        if not isinstance(record['track_info']['duration_ms'], int):
            issues.append("duration_ms 應為整數")
    
    # played_at 應為 datetime 物件
    if record.get('played_at') and not isinstance(record['played_at'], datetime):
        issues.append("played_at 應為 datetime 物件")
    
    return len(issues) == 0, issues


def calculate_quality_score(record: Dict, issues: List[str]) -> float:
    """
    計算資料品質分數
    
    Args:
        record: 聽歌記錄
        issues: 品質問題列表
        
    Returns:
        品質分數 (0.0 - 1.0)
    """
    total_checks = 10  # 總檢查項目數
    failed_checks = len(issues)
    
    # 額外加分項目
    bonus_points = 0
    
    # 如果有完整的專輯資訊
    if record.get('track_info', {}).get('album', {}).get('release_date'):
        bonus_points += 1
    
    # 如果有專輯封面
    if record.get('track_info', {}).get('album', {}).get('images'):
        bonus_points += 1
    
    # 如果有人氣度分數
    if record.get('track_info', {}).get('popularity', 0) > 0:
        bonus_points += 1
    
    passed_checks = total_checks - failed_checks + bonus_points
    score = min(passed_checks / total_checks, 1.0)  # 最高 1.0
    
    return round(score, 3)


class SpotifyDataProcessor:
    """
    Spotify 資料處理器
    整合資料獲取、驗證、格式化的完整流程
    """
    
    def __init__(self, client: SpotifyClient):
        self.client = client
        self.logger = logging.getLogger(__name__)
    
    def fetch_and_process_recent_tracks(self, limit: int = 50, batch_id: str = None) -> Dict:
        """
        獲取並處理最近播放的歌曲
        
        Args:
            limit: 獲取數量
            batch_id: 批次 ID (可選，自動生成)
            
        Returns:
            處理結果
        """
        if not batch_id:
            batch_id = f"spotify_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"開始獲取並處理最近播放記錄 (批次: {batch_id})")
        
        try:
            # 1. 獲取原始資料
            raw_items = self.client.get_recently_played(limit=limit)
            
            if not raw_items:
                return {
                    'status': 'no_data',
                    'message': 'No recent tracks found',
                    'batch_id': batch_id
                }
            
            # 2. 格式化資料
            formatted_records = []
            for item in raw_items:
                formatted_record = format_listening_record(item, batch_id)
                formatted_records.append(formatted_record)
            
            # 3. 驗證資料品質
            validation_results = self._validate_records(formatted_records)
            
            # 4. 編譯結果
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
            
            self.logger.info(f"處理完成: {len(formatted_records)} 筆記錄, {validation_results['valid_count']} 筆有效")
            
            return result
            
        except Exception as e:
            error_msg = f"處理失敗: {str(e)}"
            self.logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg,
                'batch_id': batch_id
            }
    
    def _validate_records(self, records: List[Dict]) -> Dict:
        """驗證記錄列表"""
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
        
        # 計算統計
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
# 使用範例和測試
# ============================================================================

def demo_usage():
    """展示如何使用 SpotifyClient"""
    print("🎵 Spotify Client 使用範例")
    print("=" * 40)
    
    try:
        # 初始化客戶端
        print("1. 初始化客戶端...")
        client = SpotifyClient()
        print("✅ 客戶端初始化成功")
        
        # 獲取最近播放
        print("\n2. 獲取最近播放...")
        tracks = client.get_recently_played(limit=5)
        print(f"✅ 獲取 {len(tracks)} 首歌曲")
        
        if tracks:
            example = tracks[0]
            track_name = example['track']['name']
            artist_name = example['track']['artists'][0]['name']
            print(f"🎵 範例歌曲: {track_name} - {artist_name}")
        
        # 使用資料處理器
        print("\n3. 使用資料處理器...")
        processor = SpotifyDataProcessor(client)
        result = processor.fetch_and_process_recent_tracks(limit=5)
        
        print(f"✅ 處理結果: {result['status']}")
        if result['status'] == 'success':
            print(f"📊 有效記錄: {result['validation_results']['valid_count']}")
        
        # 顯示 API 統計
        print("\n4. API 統計:")
        stats = client.get_api_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        print("\n🎉 範例執行完成！")
        
    except Exception as e:
        print(f"❌ 錯誤: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 設定基本的 logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 執行範例
    demo_usage()