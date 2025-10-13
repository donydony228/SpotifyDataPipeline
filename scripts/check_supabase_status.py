#!/usr/bin/env python3
# scripts/check_supabase_status.py
# 檢查 Supabase 專案狀態和連線

import requests
import time
from urllib.parse import urlparse

def check_supabase_project_status():
    """檢查 Supabase 專案狀態"""
    print("🔍 檢查 Supabase 專案狀態")
    print("=" * 50)
    
    # 從你的 URL 提取專案 ID
    project_url = "postgresql://postgres:[YOUR-PASSWORD]@db.mghoakoczztyfocvljrn.supabase.co:5432/postgres"
    parsed = urlparse(project_url)
    hostname = parsed.hostname
    
    # 提取專案 ID (去掉 db. 前綴)
    if hostname and hostname.startswith('db.'):
        project_id = hostname.replace('db.', '').replace('.supabase.co', '')
        print(f"📋 專案 ID: {project_id}")
    else:
        print("❌ 無法提取專案 ID")
        return False
    
    # 檢查專案 API 端點
    api_url = f"https://{project_id}.supabase.co"
    print(f"🌐 測試 API 端點: {api_url}")
    
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            print("✅ API 端點可存取")
        else:
            print(f"⚠️  API 端點回應: {response.status_code}")
    except Exception as e:
        print(f"❌ API 端點無法存取: {e}")
    
    # 檢查專案狀態頁面
    status_url = f"https://status.supabase.com/"
    print(f"\n🔍 檢查 Supabase 狀態...")
    
    try:
        response = requests.get(status_url, timeout=10)
        if response.status_code == 200:
            print("✅ Supabase 服務正常")
        else:
            print(f"⚠️  Supabase 狀態異常")
    except Exception as e:
        print(f"❌ 無法存取狀態頁面: {e}")
    
    print("\n💡 建議行動:")
    print("1. 登入 Supabase Dashboard (https://supabase.com/dashboard)")
    print("2. 檢查你的專案是否:")
    print("   - 正在運行 (not paused)")
    print("   - 沒有被暫停 (due to inactivity)")
    print("   - 資料庫服務正常")
    print("3. 如果專案被暫停，重新啟動它")
    print("4. 重新複製最新的連線字串")
    
    return True

def test_alternative_connections():
    """測試其他連線方式"""
    print("\n🔧 測試替代連線方案")
    print("=" * 50)
    
    # 測試連線池端點
    pooler_url = "postgresql://postgres:[YOUR-PASSWORD]@aws-0-us-west-1.pooler.supabase.com:6543/postgres"
    print(f"📊 連線池端點: aws-0-us-west-1.pooler.supabase.com")
    
    # 測試 IPv4 連線
    print("\n🌐 如果直連 IP 可行，可以嘗試:")
    print("1. 使用 ping 找到 Supabase IP")
    print("2. 直接用 IP 取代主機名稱")
    
    # 建議使用 Pooler
    print("\n💡 建議使用 Connection Pooler:")
    print("在 Supabase Dashboard > Settings > Database")
    print("複製 'Connection Pooling' 下的連線字串")
    print("這通常更穩定，格式像：")
    print("postgresql://postgres.xxx:password@aws-0-region.pooler.supabase.com:6543/postgres")

if __name__ == "__main__":
    check_supabase_project_status()
    test_alternative_connections()