#!/usr/bin/env python3
# scripts/check_supabase_status.py
# Check Supabase project status and provide troubleshooting steps

import requests
import time
from urllib.parse import urlparse

def check_supabase_project_status():
    """Check the status of the Supabase project"""
    print("Checking Supabase project status")
    print("=" * 50)

    # Extract project ID from your URL
    project_url = "postgresql://postgres:[YOUR-PASSWORD]@db.mghoakoczztyfocvljrn.supabase.co:5432/postgres"
    parsed = urlparse(project_url)
    hostname = parsed.hostname

    # Extract project ID (remove db. prefix and .supabase.co suffix)
    if hostname and hostname.startswith('db.'):
        project_id = hostname.replace('db.', '').replace('.supabase.co', '')
        print(f"Project ID: {project_id}")
    else:
        print("Unable to extract project ID")
        return False

    # Check project API endpoint
    api_url = f"https://{project_id}.supabase.co"
    print(f"Testing API endpoint: {api_url}")

    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            print("API endpoint is accessible")
        else:
            print(f"API endpoint response: {response.status_code}")
    except Exception as e:
        print(f"API endpoint is not accessible: {e}")

    # Check project status page
    status_url = f"https://status.supabase.com/"
    print(f"\nChecking Supabase status...")

    try:
        response = requests.get(status_url, timeout=10)
        if response.status_code == 200:
            print("Supabase service is operational")
        else:
            print(f"Supabase status is abnormal: {response.status_code}")
    except Exception as e:
        print(f"Unable to access status page: {e}")

    return True

if __name__ == "__main__":
    check_supabase_project_status()