#!/usr/bin/env python3
"""
Railway 網路診斷腳本 - 找出 Supabase 連線問題
"""

import socket
import ssl
import subprocess
import sys
import os

def run_command(cmd):
    """執行系統命令並回傳結果"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return "命令超時", "timeout"
    except Exception as e:
        return "", str(e)

def test_dns_resolution():
    """測試 DNS 解析"""
    print("🔍 DNS 解析測試")
    print("-" * 30)
    
    host = "db.mzxadnjwgexlvhgleuwm.supabase.co"
    
    # 測試 Python socket 解析
    try:
        ip_info = socket.getaddrinfo(host, 5432)
        print(f"✅ Python getaddrinfo 成功:")
        for info in ip_info[:3]:  # 只顯示前3個
            family = "IPv4" if info[0] == socket.AF_INET else "IPv6"
            ip = info[4][0]
            print(f"   {family}: {ip}")
    except Exception as e:
        print(f"❌ Python 解析失敗: {e}")
    
    # 測試系統 nslookup
    stdout, stderr = run_command(f"nslookup {host}")
    if stdout:
        print(f"✅ nslookup 結果:")
        print(f"   {stdout[:200]}...")
    else:
        print(f"❌ nslookup 失敗: {stderr}")
    
    # 測試 dig (如果有的話)
    stdout, stderr = run_command(f"dig {host}")
    if "ANSWER SECTION" in stdout:
        print(f"✅ dig 找到答案")
    else:
        print(f"⚠️ dig 不可用或無答案")

def test_network_connectivity():
    """測試網路連通性"""
    print("\n🌐 網路連通性測試")
    print("-" * 30)
    
    host = "db.mzxadnjwgexlvhgleuwm.supabase.co"
    port = 5432
    
    # 測試 TCP 連線
    for family, family_name in [(socket.AF_INET, "IPv4"), (socket.AF_INET6, "IPv6")]:
        try:
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.settimeout(10)
            
            # 嘗試解析並連線
            try:
                addr_info = socket.getaddrinfo(host, port, family)
                if addr_info:
                    ip = addr_info[0][4][0]
                    print(f"🔍 嘗試 {family_name} 連線到 {ip}:{port}")
                    sock.connect((ip, port))
                    print(f"✅ {family_name} 連線成功!")
                    sock.close()
                else:
                    print(f"❌ 無 {family_name} 地址")
            except socket.gaierror:
                print(f"❌ {family_name} DNS 解析失敗")
            except socket.timeout:
                print(f"❌ {family_name} 連線超時")
            except Exception as e:
                print(f"❌ {family_name} 連線失敗: {e}")
        except Exception as e:
            print(f"❌ {family_name} socket 建立失敗: {e}")

def test_ssl_connection():
    """測試 SSL 連線"""
    print("\n🔒 SSL 連線測試")
    print("-" * 30)
    
    host = "db.mzxadnjwgexlvhgleuwm.supabase.co"
    port = 5432
    
    try:
        # 建立 SSL 上下文
        context = ssl.create_default_context()
        
        # 嘗試 SSL 連線
        with socket.create_connection((host, port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=host) as ssock:
                print(f"✅ SSL 連線成功!")
                print(f"   協議: {ssock.version()}")
                print(f"   加密: {ssock.cipher()}")
                
    except Exception as e:
        print(f"❌ SSL 連線失敗: {e}")

def test_postgresql_connection():
    """測試 PostgreSQL 連線"""
    print("\n🐘 PostgreSQL 連線測試")
    print("-" * 30)
    
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if not supabase_url:
        print("❌ SUPABASE_DB_URL 環境變數未設定")
        return
    
    try:
        import psycopg2
        
        # 測試基本連線
        print("🔍 測試基本連線...")
        conn = psycopg2.connect(supabase_url)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"✅ PostgreSQL 連線成功!")
        print(f"   版本: {version[:80]}...")
        conn.close()
        
    except ImportError:
        print("❌ psycopg2 未安裝")
    except Exception as e:
        print(f"❌ PostgreSQL 連線失敗: {e}")

def test_environment_info():
    """顯示環境資訊"""
    print("\n💻 環境資訊")
    print("-" * 30)
    
    # Python 版本
    print(f"Python: {sys.version}")
    
    # 作業系統
    stdout, _ = run_command("uname -a")
    if stdout:
        print(f"OS: {stdout}")
    
    # 網路介面
    stdout, _ = run_command("ip addr show")
    if stdout:
        print("網路介面:")
        for line in stdout.split('\n')[:10]:  # 只顯示前10行
            if 'inet' in line:
                print(f"   {line.strip()}")
    
    # DNS 設定
    try:
        with open('/etc/resolv.conf', 'r') as f:
            dns_config = f.read()
            print("DNS 設定:")
            for line in dns_config.split('\n')[:5]:  # 只顯示前5行
                if line.strip():
                    print(f"   {line}")
    except:
        print("無法讀取 DNS 設定")

def main():
    print("🔧 Railway Supabase 連線診斷")
    print("=" * 50)
    print(f"目標: db.mzxadnjwgexlvhgleuwm.supabase.co:5432")
    print("=" * 50)
    
    test_environment_info()
    test_dns_resolution()
    test_network_connectivity() 
    test_ssl_connection()
    test_postgresql_connection()
    
    print("\n" + "=" * 50)
    print("🔍 診斷完成")
    print("=" * 50)
    print("\n💡 根據以上結果:")
    print("1. 如果 IPv4 成功但 IPv6 失敗 → 強制使用 IPv4")
    print("2. 如果 DNS 解析失敗 → 使用 IP 地址直接連線")
    print("3. 如果 SSL 失敗 → 檢查證書或降級 SSL")
    print("4. 如果全部失敗 → Railway 網路政策問題")

if __name__ == "__main__":
    main()