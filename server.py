"""
Lightweight Web Server for EPWRF Data Explorer
Can run on any device with Python installed (laptops, smartphones via Termux, etc.)

Usage:
    python server.py [port]
    
Default port: 8000
Access: http://localhost:8000 or http://<your-ip>:8000

Features:
- Serves static files (HTML, CSS, JS, JSON)
- Minimal resource usage
- CORS enabled for local development
- Works on Windows, macOS, Linux, Android (Termux)
"""

import http.server
import socketserver
import os
import sys
import socket

# Configuration
DEFAULT_PORT = 8000
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")


class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with CORS support"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)
    
    def end_headers(self):
        # Add CORS headers
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
    
    def log_message(self, format, *args):
        # Custom logging
        print(f"[{self.log_date_time_string()}] {args[0]}")


def get_local_ip():
    """Get local IP address for network access"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    
    # Check if web directory exists
    if not os.path.exists(WEB_DIR):
        print(f"Error: Web directory not found: {WEB_DIR}")
        print("Creating directory structure...")
        os.makedirs(WEB_DIR, exist_ok=True)
    
    # Check if index.html exists
    index_path = os.path.join(WEB_DIR, "index.html")
    if not os.path.exists(index_path):
        print(f"Warning: index.html not found at {index_path}")
    
    local_ip = get_local_ip()
    
    print("=" * 60)
    print("EPWRF Data Explorer - Lightweight Server")
    print("=" * 60)
    print()
    print(f"Serving from: {WEB_DIR}")
    print()
    print("Access URLs:")
    print(f"  Local:   http://localhost:{port}")
    print(f"  Network: http://{local_ip}:{port}")
    print()
    print("Press Ctrl+C to stop the server")
    print("=" * 60)
    print()
    
    with socketserver.TCPServer(("", port), CORSRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\nServer stopped.")


if __name__ == "__main__":
    main()
