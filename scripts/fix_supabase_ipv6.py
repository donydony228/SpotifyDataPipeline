#!/usr/bin/env python3
import os
import socket
import requests
from urllib.parse import urlparse, urlunparse

def get_ipv4_for_supabase():
    host = "db.mzxadnjwgexlvhgleuwm.supabase.co"
    
    # 方法1: Google DNS API
    try:
        print("🔍 使用 Google DNS 查詢 IPv4...")
        url = f"https://dns.google/resolve?name={host}&type=A"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if 'Answer' in data:
            for answer in data['Answer']:
                if answer['type'] == 1:  # A record (IPv4)
                    print(f"✅ 找到 IPv4: {answer['data']}")
                    return answer['data']
    except Exception as e:
        print(f"❌ Google DNS 失敗: {e}")
    
    # 方法2: Cloudflare DNS API
    try:
        print("🔍 使用 Cloudflare DNS 查詢 IPv4...")
        url = f"https://cloudflare-dns.com/dns-query?name={host}&type=A"
        headers = {'Accept': 'application/dns-json'}
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        
        if 'Answer' in data:
            for answer in data['Answer']:
                if answer['type'] == 1:
                    print(f"✅ 找到 IPv4: {answer['data']}")
                    return answer['data']
    except Exception as e:
        print(f"❌ Cloudflare DNS 失敗: {e}")
    
    # 方法3: 嘗試已知 IP
    known_ips = ["52.209.78.15", "18.132.53.90", "3.123.75.248"]
    for ip in known_ips:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((ip, 5432))
            sock.close()
            if result == 0:
                print(f"✅ 測試 IP {ip} 可連接")
                return ip
        except:
            continue
    
    return None

def create_fixed_url():
    original_url = os.getenv('SUPABASE_DB_URL')
    if not original_url:
        print("❌ SUPABASE_DB_URL 未設定")
        return None
    
    parsed = urlparse(original_url)
    ipv4 = get_ipv4_for_supabase()
    
    if not ipv4:
        print("❌ 無法獲取 IPv4，使用原始 URL")
        return original_url + "?sslmode=require"
    
    # 建立新 URL
    new_netloc = f"{parsed.username}:{parsed.password}@{ipv4}:{parsed.port or 5432}"
    fixed_url = urlunparse((
        parsed.scheme, new_netloc, parsed.path, 
        parsed.params, "sslmode=require&connect_timeout=30", parsed.fragment
    ))
    
    print(f"🔧 修復 URL: postgresql://***@{ipv4}:5432/***")
    return fixed_url

if __name__ == "__main__":
    print("🔧 修復 Supabase IPv6 連線...")
    fixed_url = create_fixed_url()
    if fixed_url:
        with open('/tmp/supabase_fixed_url.txt', 'w') as f:
            f.write(fixed_url)
        print("✅ 修復 URL 已保存")
    else:
        print("❌ URL 修復失敗")
