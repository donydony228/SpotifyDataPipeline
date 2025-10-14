# src/spotify/__init__.py
# Spotify 模組初始化

from .spotify_client import SpotifyClient, SpotifyAuthError, SpotifyAPIError

__all__ = [
    'SpotifyClient',
    'SpotifyAuthError', 
    'SpotifyAPIError'
]

__version__ = '1.0.0'