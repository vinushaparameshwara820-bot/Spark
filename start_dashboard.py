#!/usr/bin/env python3
"""
Launcher script for the Weather Analysis Dashboard
"""

import subprocess
import webbrowser
import time
import sys
import os

def main():
    print("🌤️ Weather Analysis Dashboard Launcher")
    print("=" * 40)
    
    try:
        # Start the Flask application
        print("Starting the web dashboard...")
        process = subprocess.Popen([sys.executable, "app.py"], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE,
                                 text=True)
        
        # Wait a moment for the server to start
        time.sleep(3)
        
        # Check if the process is still running
        if process.poll() is None:
            print("✅ Dashboard server started successfully!")
            print("🌐 Opening http://localhost:5000 in your browser...")
            
            # Open the browser
            webbrowser.open("http://localhost:5000")
            
            print("\n📝 The dashboard is now running!")
            print("   - View it in your browser at: http://localhost:5000")
            print("   - Press Ctrl+C to stop the server")
            print("\n⚠️  Do not close this window while using the dashboard!")
            
            # Wait for the process to complete (or be interrupted)
            try:
                stdout, stderr = process.communicate()
                if stderr:
                    print(f"Error: {stderr}")
            except KeyboardInterrupt:
                print("\n🛑 Stopping the dashboard server...")
                process.terminate()
                process.wait()
                print("✅ Server stopped.")
                
        else:
            # Process ended early, show error
            stdout, stderr = process.communicate()
            print("❌ Failed to start the dashboard server.")
            if stderr:
                print(f"Error: {stderr}")
                
    except FileNotFoundError:
        print("❌ Error: Could not find app.py. Make sure you're in the correct directory.")
    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    main()