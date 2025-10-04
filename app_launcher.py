"""
Standalone Launcher for Unified Trade Processing Pipeline
Bundles Python + Streamlit + all dependencies into a single .exe
"""

import sys
import os
import subprocess
import webbrowser
import time
import socket
from pathlib import Path

def find_free_port():
    """Find a free port to run Streamlit on"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port

def get_resource_path(relative_path):
    """Get absolute path to resource - works for dev and PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def main():
    print("=" * 60)
    print("  Unified Trade Processing Pipeline")
    print("  Version 5.0 - Standalone Edition")
    print("=" * 60)
    print()
    print("Starting application...")

    # Find free port
    port = find_free_port()
    print(f"Using port: {port}")

    # Get path to the Streamlit app
    app_path = get_resource_path("unified-streamlit-app.py")

    if not os.path.exists(app_path):
        print(f"ERROR: Application file not found at {app_path}")
        input("Press Enter to exit...")
        sys.exit(1)

    # Prepare Streamlit command
    streamlit_cmd = [
        sys.executable,
        "-m", "streamlit",
        "run",
        app_path,
        f"--server.port={port}",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
        "--server.fileWatcherType=none"
    ]

    print(f"Launching Streamlit server on http://localhost:{port}")
    print("Please wait while the application loads...")
    print()

    # Start Streamlit server
    process = subprocess.Popen(
        streamlit_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
    )

    # Wait for server to start
    time.sleep(3)

    # Open browser
    url = f"http://localhost:{port}"
    print(f"Opening browser at {url}")
    webbrowser.open(url)

    print()
    print("=" * 60)
    print("  Application is running!")
    print("=" * 60)
    print()
    print(f"  Access the app at: {url}")
    print()
    print("  IMPORTANT:")
    print("  - Keep this window open while using the application")
    print("  - Close this window to stop the application")
    print("  - Press Ctrl+C to stop the server")
    print()
    print("=" * 60)

    try:
        # Keep the process running
        process.wait()
    except KeyboardInterrupt:
        print("\n\nShutting down application...")
        process.terminate()
        process.wait()
        print("Application closed.")

if __name__ == "__main__":
    main()
