# scripts/fix_supabase_ipv6.py
# 修复 Supabase IPv6 连接问题

import os
import socket
import requests
from urllib.parse import urlparse, urlunparse

def get_ipv4_for_supabase():
    """强制获取 Supabase 的 IPv4 地址"""
    host = "db.mzxadnjwgexlvhgleuwm.supabase.co"
    
    try:
        # 方法1: 强制 IPv4 DNS 查询
        result = socket.getaddrinfo(host, 5432, socket.AF_INET)
        if result:
            ipv4 = result[0][4][0]
            print(f"✅ 找到 IPv4 地址: {ipv4}")
            return ipv4
    except Exception as e:
        print(f"❌ IPv4 查询失败: {e}")
    
    # 方法2: 使用公共 DNS API
    try:
        response = requests.get(f"https://dns.google/resolve?name={host}&type=A", timeout=10)
        data = response.json()
        if 'Answer' in data:
            for answer in data['Answer']:
                if answer['type'] == 1:  # A record
                    ipv4 = answer['data']
                    print(f"✅ DNS API 找到 IPv4: {ipv4}")
                    return ipv4
    except Exception as e:
        print(f"❌ DNS API 查询失败: {e}")
    
    return None

def create_fixed_supabase_url():
    """创建修复的 Supabase URL"""
    original_url = os.getenv('SUPABASE_DB_URL')
    if not original_url:
        print("❌ SUPABASE_DB_URL 未设定")
        return None
    
    print(f"🔍 原始 URL: {original_url[:50]}...")
    
    parsed = urlparse(original_url)
    ipv4 = get_ipv4_for_supabase()
    
    if not ipv4:
        print("❌ 无法获取 IPv4，保持原始 URL")
        return original_url
    
    # 替换主机名为 IPv4
    new_netloc = f"{parsed.username}:{parsed.password}@{ipv4}:{parsed.port or 5432}"
    fixed_url = urlunparse((
        parsed.scheme, new_netloc, parsed.path, 
        parsed.params, "sslmode=require", parsed.fragment
    ))
    
    print(f"🔧 修复后 URL: postgresql://***@{ipv4}:5432/***")
    return fixed_url

def update_env_file():
    """更新 .env 文件中的 Supabase URL"""
    fixed_url = create_fixed_supabase_url()
    if not fixed_url:
        return False
    
    try:
        # 读取现有 .env
        with open('.env', 'r') as f:
            lines = f.readlines()
        
        # 更新 SUPABASE_DB_URL
        updated_lines = []
        url_updated = False
        
        for line in lines:
            if line.startswith('SUPABASE_DB_URL='):
                updated_lines.append(f'SUPABASE_DB_URL={fixed_url}\n')
                url_updated = True
                print("✅ 更新了 .env 中的 SUPABASE_DB_URL")
            else:
                updated_lines.append(line)
        
        if not url_updated:
            updated_lines.append(f'SUPABASE_DB_URL={fixed_url}\n')
            print("✅ 添加了 SUPABASE_DB_URL 到 .env")
        
        # 写回 .env
        with open('.env', 'w') as f:
            f.writelines(updated_lines)
        
        return True
        
    except Exception as e:
        print(f"❌ 更新 .env 失败: {e}")
        return False

def test_fixed_connection():
    """测试修复后的连接"""
    try:
        import psycopg2
        from dotenv import load_dotenv
        
        # 重新加载环境变量
        load_dotenv(override=True)
        
        fixed_url = os.getenv('SUPABASE_DB_URL')
        print(f"🧪 测试修复后的连接...")
        
        conn = psycopg2.connect(fixed_url, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        conn.close()
        
        print("🎉 Supabase 连接修复成功！")
        return True
        
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False

if __name__ == "__main__":
    print("🔧 修复 Supabase IPv6 连接问题")
    print("=" * 40)
    
    if update_env_file():
        print("\n🧪 测试修复结果...")
        if test_fixed_connection():
            print("\n✅ 修复成功！现在可以重新运行测试")
            print("建议执行: make restart && ./scripts/test_final_mock_dag.sh")
        else:
            print("\n⚠️  修复未完全成功，但 DAG 会优雅处理连接失败")
    else:
        print("\n❌ 修复失败")