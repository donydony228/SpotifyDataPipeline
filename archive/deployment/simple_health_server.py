#!/usr/bin/env python3
"""
簡單的健康檢查服務器
在 Airflow 啟動前提供基本的健康檢查端點
"""

import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ['/health', '/api/v1/health']:
            # 簡單的健康檢查響應
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            health_data = {
                "status": "ok",
                "message": "Service is starting up",
                "timestamp": time.time()
            }
            
            self.wfile.write(json.dumps(health_data).encode())
        else:
            # 重定向到實際的 Airflow 服務
            self.send_response(302)
            self.send_header('Location', 'http://localhost:8080' + self.path)
            self.end_headers()
    
    def log_message(self, format, *args):
        # 簡化日誌輸出
        print(f"Health check: {format % args}")

def start_health_server():
    """啟動健康檢查服務器"""
    server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
    print("🏥 Health check server started on port 8080")
    server.serve_forever()

if __name__ == "__main__":
    start_health_server()
