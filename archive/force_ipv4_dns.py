import socket
import os
from urllib.parse import urlparse

def get_ipv4_address(hostname):
    """Get IPv4 address for hostname"""
    try:
        # Force IPv4 only
        result = socket.getaddrinfo(hostname, None, socket.AF_INET)
        if result:
            return result[0][4][0]
    except:
        pass
    return None

def fix_supabase_url():
    """Convert Supabase URL to use IPv4 address"""
    original_url = os.getenv('SUPABASE_DB_URL')
    if not original_url:
        return None
    
    parsed = urlparse(original_url)
    hostname = parsed.hostname
    
    # Get IPv4 address
    ipv4 = get_ipv4_address(hostname)
    if ipv4:
        # Replace hostname with IPv4
        fixed_url = original_url.replace(hostname, ipv4)
        return fixed_url
    
    return original_url

if __name__ == "__main__":
    fixed = fix_supabase_url()
    if fixed:
        print(fixed)
